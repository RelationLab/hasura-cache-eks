use std::{env, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use axum::{
    body::HttpBody,
    extract::{rejection::JsonRejection, Extension, FromRequest, Json, RequestParts},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    BoxError, Router,
};
use bytes::BytesMut;
use hmac_sha256::Hash;
use r2d2::ManageConnection;
use redis::{cluster::ClusterClient, Commands, RedisError};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const CACHE_BLACK_LIST: [&str; 1] = ["LabelDescribe"];

enum AppError {
    RedisError(RedisError),
    ReqwestError(reqwest::Error),
    ResponseNotJson,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::RedisError(ref error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("code: {:?}, category: {}", error.code(), error.category()),
            ),
            AppError::ReqwestError(ref error) => (StatusCode::BAD_GATEWAY, error.to_string()),
            AppError::ResponseNotJson => (
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("hasura engine's response is not json"),
            ),
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
}

async fn health() {}

#[derive(Debug)]
struct GraphQLCache {
    hash: String,
    req: GraphQLRequest,
}

#[derive(Debug, Deserialize, Serialize)]
struct GraphQLRequest {
    query: String,
    variables: Value,
    #[serde(rename = "operationName")]
    operation_name: Option<String>,
}

impl GraphQLRequest {
    fn as_json(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}

#[async_trait]
impl<B> FromRequest<B> for GraphQLCache
where
    B: Send + HttpBody,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    type Rejection = JsonRejection;

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        let mut bytes = BytesMut::with_capacity(1024);

        Json::from_request(req)
            .await
            .map(|Json(req): Json<GraphQLRequest>| {
                bytes.extend_from_slice(req.query.as_bytes());
                let variables_bytes = serde_json::to_vec(&req.variables).unwrap();
                bytes.extend(variables_bytes);
                let hash = faster_hex::hex_string(&Hash::hash(&bytes)[..]);
                GraphQLCache { hash, req }
            })
    }
}

async fn post_graphql(
    graphql: GraphQLCache,
    Extension(redis_cluster): Extension<ClusterClient>,
    Extension(proxy): Extension<Arc<HasuraReverseProxy>>,
) -> Result<Json<Value>, AppError> {
    let mut redis_conn = redis_cluster.connect().map_err(AppError::RedisError)?;

    let cache: Option<String> = redis_conn
        .get(&graphql.hash)
        .map_err(AppError::RedisError)?;

    let json_response = if let Some(cache) = cache {
        serde_json::from_str(&cache).unwrap()
    } else {
        let response = proxy
            .client
            .post(format!("http://{}:{}/v1/graphql", proxy.host, proxy.port))
            .header("x-hasura-admin-secret", &proxy.admin_secret)
            .json(&graphql.req.as_json())
            .send()
            .await
            .map_err(AppError::ReqwestError)?;

        let json_response = response
            .json::<Value>()
            .await
            .map_err(|_| AppError::ResponseNotJson)?;

        if json_response.pointer("/errors").is_none()
            && !CACHE_BLACK_LIST.contains(&graphql.req.operation_name.as_deref().unwrap_or(""))
        {
            let cached = serde_json::to_string(&json_response).unwrap();
            tokio::spawn(async move {
                redis_conn
                    .set_ex::<_, _, ()>(graphql.hash, cached, 300)
                    .unwrap();
            });
        }

        json_response
    };

    Ok(Json(json_response))
}

#[tokio::main]
async fn main() {
    let redis_host = env::var("REDIS_HOST").expect("REDIS_HOST env var should be specified");
    let redis_cluster = ClusterClient::open(vec![format!("redis://{}/", redis_host)])
        .expect("Can't open redis cluster client");

    let hasura_engine_reverse_proxy = HasuraReverseProxy::new();

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/graphql", post(post_graphql))
        .layer(Extension(redis_cluster))
        .layer(Extension(Arc::new(hasura_engine_reverse_proxy)));

    let addr = SocketAddr::from(([0, 0, 0, 0], 80));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
