use std::{env, net::SocketAddr, str::FromStr, sync::Arc};

use async_trait::async_trait;
use axum::{
    extract::{rejection::JsonRejection, Extension, FromRequest, Json, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use bytes::BytesMut;
use futures_util::TryFutureExt;
use log::error;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;

const PUBLIC_OPERATIONS: [&str; 4] = [
    "FriendsRequestAction",
    "FriendsAction",
    "FriendsRequestCountAction",
    "LabelDescribe",
];

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("privacy error")]
    Privacy,
    #[error("reqwest error: `{0}`")]
    Reqwest(#[from] reqwest::Error),
    #[error("serde json error: `{0}`")]
    SerdeJson(#[from] serde_json::Error),
    #[error("stdio error: `{0}`")]
    StdIo(#[from] std::io::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self {
            AppError::Privacy => StatusCode::FORBIDDEN,
            AppError::Reqwest(..) => StatusCode::BAD_GATEWAY,
            AppError::StdIo(..) | AppError::SerdeJson(..) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = Json(json!({
            "error": self.to_string(),
        }));

        (status, body).into_response()
    }
}

struct HasuraReverseProxy {
    host: String,
    port: u16,
    admin_secret: String,
    client: reqwest::Client,
}

impl HasuraReverseProxy {
    fn new() -> HasuraReverseProxy {
        let host =
            env::var("HASURA_ENGINE_HOST").expect("HASURA_ENGINE_HOST env var should be specified");

        let port: u16 = env::var("HASURA_ENGINE_PORT")
            .expect("HASURA_ENGINE_PORT env var should be specified")
            .parse()
            .expect("HASURA_ENGINE_PORT env var should be number");

        let admin_secret = env::var("HASURA_ENGINE_ADMIN_SECRET")
            .expect("HASURA_ENGINE_ADMIN_SECRET env var should be specified");

        HasuraReverseProxy {
            host,
            port,
            admin_secret,
            client: reqwest::Client::new(),
        }
    }

    async fn post_graphql(
        &self,
        headers: HeaderMap<HeaderValue>,
        graphql: Value,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.client
            .post(format!("http://{}:{}/v1/graphql", self.host, self.port))
            .headers(headers)
            .header("x-hasura-admin-secret", &self.admin_secret)
            .json(&graphql)
            .send()
            .await
    }
}

async fn health() {}

#[derive(Debug)]
struct GraphQLCache {
    req: GraphQLRequest,
    post_headers: HeaderMap<HeaderValue>,
}

#[derive(Debug, Deserialize, Serialize)]
struct GraphQLRequest {
    query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    variables: Option<Value>,
    #[serde(rename = "operationName")]
    operation_name: Option<String>,
}

impl GraphQLRequest {
    fn try_as_json(&self) -> Result<Value, AppError> {
        serde_json::to_value(self).map_err(Into::into)
    }

    fn is_public(&self) -> bool {
        self.operation_name
            .as_deref()
            .map(|ref operation_name| PUBLIC_OPERATIONS.contains(operation_name))
            .unwrap_or(false)
    }
}

#[async_trait]
impl<S> FromRequest<S> for GraphQLCache
where
    S: Send + Sync,
{
    type Rejection = JsonRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let post_headers = req
            .headers()
            .iter()
            .filter(|(key, _)| ["address", "authorization"].contains(&key.as_str()))
            .flat_map(|(name, value)| {
                Ok::<_, anyhow::Error>((
                    HeaderName::from_str(name.as_str())?,
                    HeaderValue::from_str(value.to_str()?)?,
                ))
            })
            .collect::<HeaderMap<HeaderValue>>();

        let mut bytes = BytesMut::with_capacity(2048);
        Json::from_request(req, state)
            .await
            .map(|Json(req): Json<GraphQLRequest>| {
                bytes.extend_from_slice(req.query.as_bytes());
                let variables_bytes = serde_json::to_vec(&req.variables).unwrap();
                bytes.extend(variables_bytes);
                GraphQLCache { req, post_headers }
            })
    }
}

async fn post_graphql(
    graphql: GraphQLCache,
    proxy: Arc<HasuraReverseProxy>,
) -> Result<Json<Value>, AppError> {
    proxy
        .post_graphql(graphql.post_headers, graphql.req.try_as_json()?)
        .and_then(|response| response.json::<Value>())
        .await
        .map_err(Into::into)
        .map(Json)
}

async fn public_post_graphql(
    Extension(proxy): Extension<Arc<HasuraReverseProxy>>,
    graphql: GraphQLCache,
) -> Result<Json<Value>, AppError> {
    if !graphql.req.is_public() {
        return Err(AppError::Privacy);
    }

    post_graphql(graphql, proxy).await
}

async fn private_post_graphql(
    Extension(proxy): Extension<Arc<HasuraReverseProxy>>,
    graphql: GraphQLCache,
) -> Result<Json<Value>, AppError> {
    post_graphql(graphql, proxy).await
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    env_logger::init();

    let hasura_engine_reverse_proxy = Arc::new(HasuraReverseProxy::new());

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/graphql", post(private_post_graphql))
        .route("/public/v1/graphql", post(public_post_graphql))
        .layer(Extension(hasura_engine_reverse_proxy));

    let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 80))).await?;

    Ok(axum::serve(listener, app).await?)
}
