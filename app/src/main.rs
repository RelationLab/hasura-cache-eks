use std::{env, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use axum::extract::rejection::JsonRejection;
use axum::{
    body::HttpBody,
    extract::{Extension, FromRequest, Json, RequestParts},
    http::StatusCode,
    routing::{get, post},
    BoxError, Router,
};
use hmac_sha256::Hash;
use redis::{aio::ConnectionManager, AsyncCommands};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

struct HasuraReverseProxy {
    host: String,
    port: u16,
    admin_secret: String,
    client: Client,
}

impl HasuraReverseProxy {
    fn new() -> HasuraReverseProxy {
        let host = env::var("HASURA_HOST").expect("HASURA_HOST env var should be specified");

        let port: u16 = env::var("HASURA_PORT")
            .expect("HASURA_PORT env var should be specified")
            .parse()
            .expect("HASURA_PORT env var should be number");

        let admin_secret = env::var("HASURA_ADMIN_SECRET")
            .expect("HASURA_ADMIN_SECRET env var should be specified");

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
    #[serde(flatten)]
    value: Value,
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
        Json::from_request(req)
            .await
            .map(|Json(req): Json<GraphQLRequest>| {
                let hash = faster_hex::hex_string(&Hash::hash(req.query.as_bytes())[..]);
                GraphQLCache { hash, req }
            })
    }
}

async fn post_graphql(
    graphql: GraphQLCache,
    Extension(redis): Extension<ConnectionManager>,
    Extension(proxy): Extension<Arc<HasuraReverseProxy>>,
) -> Result<Json<Value>, StatusCode> {
    let cache: Option<String> = redis
        .clone()
        .get(&graphql.hash)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
            .map_err(|_| StatusCode::BAD_GATEWAY)?;

        let json_response = response
            .json::<Value>()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let cached = serde_json::to_string(&json_response).unwrap();

        tokio::spawn(async move {
            redis
                .clone()
                .set_ex::<_, _, ()>(graphql.hash, cached, 300)
                .await
                .unwrap();
        });

        json_response
    };

    Ok(Json(json_response))
}

#[tokio::main]
async fn main() {
    let redis_host = env::var("REDIS_HOST").expect("REDIS_HOST env var should be specified");

    let redis_client =
        redis::Client::open(format!("redis://{}/", redis_host)).expect("Can't open redis client");

    let redis_connection_manager = redis_client
        .get_tokio_connection_manager()
        .await
        .expect("Can't get connection manager");

    let hasura_reverse_proxy = HasuraReverseProxy::new();

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/graphql", post(post_graphql))
        .layer(Extension(redis_connection_manager))
        .layer(Extension(Arc::new(hasura_reverse_proxy)));

    let addr = SocketAddr::from(([0, 0, 0, 0], 80));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
