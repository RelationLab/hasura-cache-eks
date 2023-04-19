use std::{env, net::SocketAddr, sync::Arc};

use async_channel::{bounded, Sender};
use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderValue};
use axum::{
    body::HttpBody,
    extract::{rejection::JsonRejection, Extension, FromRequest, Json},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    BoxError, Router,
};
use bytes::BytesMut;
use futures_util::TryFutureExt;
use hmac_sha256::Hash;
use r2d2::ManageConnection;
use redis::{
    cluster::{ClusterClient, ClusterConnection},
    Commands, RedisError,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const CACHED_OPERATIONS: [&str; 1] = ["AddressesWithLabels"];
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
    #[error("redis error: `{0}`")]
    Redis(#[from] RedisError),
    #[error("reqwest error: `{0}`")]
    Reqwest(#[from] reqwest::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::Privacy => (StatusCode::FORBIDDEN, self.to_string()),
            AppError::Redis(ref error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("code: {:?}, category: {}", error.code(), error.category()),
            ),
            AppError::Reqwest(ref error) => (StatusCode::BAD_GATEWAY, error.to_string()),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}

struct HasuraReverseProxy {
    host: String,
    port: u16,
    admin_secret: String,
    client: Client,
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
            client: Client::new(),
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
    hash: String,
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
    fn as_json(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }

    fn cached(&self) -> bool {
        self.operation_name
            .as_deref()
            .map(|ref operation_name| CACHED_OPERATIONS.contains(operation_name))
            .unwrap_or(false)
    }

    fn is_public(&self) -> bool {
        self.operation_name
            .as_deref()
            .map(|ref operation_name| PUBLIC_OPERATIONS.contains(operation_name))
            .unwrap_or(false)
    }
}

#[async_trait]
impl<S, B> FromRequest<S, B> for GraphQLCache
where
    B: 'static + Send + HttpBody,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = JsonRejection;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let post_headers = req
            .headers()
            .iter()
            .filter(|(key, _)| ["address", "authorization"].contains(&key.as_str()))
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect::<HeaderMap<HeaderValue>>();

        let mut bytes = BytesMut::with_capacity(2048);
        Json::from_request(req, state)
            .await
            .map(|Json(req): Json<GraphQLRequest>| {
                bytes.extend_from_slice(req.query.as_bytes());
                let variables_bytes = serde_json::to_vec(&req.variables).unwrap();
                bytes.extend(variables_bytes);
                let hash = faster_hex::hex_string(&Hash::hash(&bytes)[..]);
                GraphQLCache {
                    hash,
                    req,
                    post_headers,
                }
            })
    }
}

async fn post_graphql(
    graphql: GraphQLCache,
    redis_cluster: ClusterClient,
    proxy: Arc<HasuraReverseProxy>,
    tx: Sender<(ClusterConnection, String, String)>,
) -> Result<Json<Value>, AppError> {
    if graphql.req.cached() {
        let mut redis_conn = redis_cluster.connect()?;

        let cache: Option<String> = redis_conn.get(&graphql.hash)?;

        if let Some(cache) = cache {
            Ok(Json(serde_json::from_str(&cache).unwrap()))
        } else {
            let response = proxy
                .post_graphql(graphql.post_headers, graphql.req.as_json())
                .await?;
            let json_response = response.json::<Value>().await?;
            if json_response.pointer("/errors").is_none() {
                let cached = serde_json::to_string(&json_response).unwrap();
                tx.send((redis_conn, graphql.hash, cached)).await.unwrap();
            }
            Ok(Json(json_response))
        }
    } else {
        proxy
            .post_graphql(graphql.post_headers, graphql.req.as_json())
            .and_then(|response| response.json::<Value>())
            .await
            .map_err(Into::into)
            .map(Json)
    }
}

async fn public_post_graphql(
    Extension(tx): Extension<Sender<(ClusterConnection, String, String)>>,
    Extension(redis_cluster): Extension<ClusterClient>,
    Extension(proxy): Extension<Arc<HasuraReverseProxy>>,
    graphql: GraphQLCache,
) -> Result<Json<Value>, AppError> {
    if !graphql.req.is_public() {
        return Err(AppError::Privacy);
    }

    post_graphql(graphql, redis_cluster, proxy, tx).await
}

async fn private_post_graphql(
    Extension(tx): Extension<Sender<(ClusterConnection, String, String)>>,
    Extension(redis_cluster): Extension<ClusterClient>,
    Extension(proxy): Extension<Arc<HasuraReverseProxy>>,
    graphql: GraphQLCache,
) -> Result<Json<Value>, AppError> {
    post_graphql(graphql, redis_cluster, proxy, tx).await
}

#[tokio::main]
async fn main() {
    let cache_live = if let Ok("production") = env::var("APP_ENV").as_ref().map(|v| v.as_str()) {
        172800
    } else {
        300
    };

    let redis_host = env::var("REDIS_HOST").expect("REDIS_HOST env var should be specified");
    let redis_cluster = ClusterClient::new(vec![format!("redis://{}/", redis_host)])
        .expect("Can't open redis cluster client");

    let hasura_engine_reverse_proxy = Arc::new(HasuraReverseProxy::new());

    let (tx, rx) = bounded::<(ClusterConnection, String, String)>(1024);

    for _ in 0..256 {
        let rx = rx.clone();
        tokio::spawn(async move {
            while let Ok((mut conn, hash, cache)) = rx.recv().await {
                conn.set_ex::<_, _, ()>(hash, cache, cache_live).unwrap();
            }
        });
    }

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/graphql", post(private_post_graphql))
        .route("/public/v1/graphql", post(public_post_graphql))
        .layer(Extension(tx))
        .layer(Extension(redis_cluster))
        .layer(Extension(hasura_engine_reverse_proxy));

    let addr = SocketAddr::from(([0, 0, 0, 0], 80));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
