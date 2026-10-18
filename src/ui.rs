//! An authenticated, loopback-only workbench. SQL text is never accepted for execution.
use crate::{
    commands::compile_sql,
    pg::{PgDb, SqlParam},
};
use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bson::Bson;
use eyre::{bail, Result};
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct App {
    pool: r2d2::Pool<PostgresConnectionManager<NoTls>>,
    authorization: String,
    workers: Arc<Semaphore>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Pipeline {
    database: String,
    collection: String,
    pipeline: Vec<Bson>,
}

pub fn start(address: SocketAddr, postgres_url: &str) -> Result<()> {
    if !address.ip().is_loopback() {
        bail!("the web workbench must bind to a loopback address");
    }
    let token = std::env::var("OXIDE_WEB_TOKEN").unwrap_or_default();
    if token.len() < 32 || !token.bytes().all(|c| c.is_ascii_alphanumeric()) {
        bail!("set OXIDE_WEB_TOKEN to at least 32 random alphanumeric characters");
    }
    let manager = PostgresConnectionManager::new(postgres_url.parse()?, NoTls);
    let pool = r2d2::Pool::builder()
        .max_size(4)
        .connection_timeout(Duration::from_secs(5))
        .build(manager)?;
    let state = App {
        pool,
        authorization: format!("Bearer {token}"),
        workers: Arc::new(Semaphore::new(4)),
    };
    let listener = std::net::TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    log::info!("Web workbench listening on http://{address}");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            axum::serve(listener, router(state)).await
        })?;
    Ok(())
}
fn router(state: App) -> Router {
    let api = Router::new()
        .route("/api/convert", post(convert))
        .route("/api/run", post(run))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize));
    Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../public/index.html")) }),
        )
        .merge(api)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(state)
}
async fn authorize(State(app): State<App>, request: Request, next: Next) -> Response {
    if request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        != Some(app.authorization.as_str())
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // No CORS, cookies, or URL tokens. The browser keeps the token only in memory.
    match tokio::time::timeout(Duration::from_secs(35), next.run(request)).await {
        Ok(response) => response,
        Err(_) => StatusCode::REQUEST_TIMEOUT.into_response(),
    }
}
fn error(error: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error":error.to_string()})),
    )
}
async fn convert(Json(input): Json<Pipeline>) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let sql = compile_sql(
        &SqlParam::new(&input.database, &input.collection),
        &input.pipeline,
    )
    .map_err(error)?;
    Ok(Json(json!({"sql":sql.text,"parameters":sql.params})))
}
async fn run(
    State(app): State<App>,
    Json(input): Json<Pipeline>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let permit = app.workers.try_acquire_owned().map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"workbench is busy"})),
        )
    })?;
    let rows = tokio::task::spawn_blocking(move || -> Result<Vec<bson::Document>> {
        let _permit = permit;
        let sp = SqlParam::new(&input.database, &input.collection);
        let sql = compile_sql(&sp, &input.pipeline)?;
        let mut pg = PgDb::try_from_pool(&app.pool)?;
        if !sp.exists(&mut pg)? {
            return Ok(Vec::new());
        }
        pg.documents(&sql, None)
    })
    .await
    .map_err(error)?
    .map_err(error)?;
    Ok(Json(json!({"rows":rows})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    #[test]
    fn authorization_body_limits_and_parameter_preview() {
        let manager = PostgresConnectionManager::new(
            "postgresql://127.0.0.1:1/unused".parse().unwrap(),
            NoTls,
        );
        let pool = r2d2::Pool::builder()
            .min_idle(Some(0))
            .max_size(1)
            .build_unchecked(manager);
        let app = router(App {
            pool,
            authorization: "Bearer test-token".into(),
            workers: Arc::new(Semaphore::new(1)),
        });
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let cases = [
                    (
                        "/api/run",
                        false,
                        "{}".to_string(),
                        StatusCode::UNAUTHORIZED,
                    ),
                    (
                        "/api/run",
                        true,
                        r#"{"query":"DROP SCHEMA public CASCADE"}"#.into(),
                        StatusCode::UNPROCESSABLE_ENTITY,
                    ),
                    (
                        "/api/convert",
                        true,
                        r#"{"database":"db$1","collection":"items","pipeline":[]}"#.into(),
                        StatusCode::OK,
                    ),
                    (
                        "/api/convert",
                        true,
                        " ".repeat(65 * 1024),
                        StatusCode::PAYLOAD_TOO_LARGE,
                    ),
                    ("/absent", true, "{}".into(), StatusCode::NOT_FOUND),
                ];
                for (path, auth, body, status) in cases {
                    let mut request =
                        Request::post(path).header("content-type", "application/json");
                    if auth {
                        request = request.header("authorization", "Bearer test-token");
                    }
                    let response = app
                        .clone()
                        .oneshot(request.body(Body::from(body)).unwrap())
                        .await
                        .unwrap();
                    assert_eq!(response.status(), status, "{path}");
                    if status == StatusCode::OK {
                        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                            .await
                            .unwrap();
                        let value: Value = serde_json::from_slice(&bytes).unwrap();
                        assert!(value["sql"].as_str().unwrap().contains("\"db$1\""));
                        assert_eq!(value["parameters"], json!([]));
                    }
                }
            });
    }
}
