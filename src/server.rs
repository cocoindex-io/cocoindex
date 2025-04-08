use crate::{lib_context::LibContext, service};

use anyhow::Result;
use axum::{routing, Router};
use futures::{future::BoxFuture, FutureExt};
use serde::Deserialize;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};

#[derive(Deserialize, Debug)]
pub struct ServerSettings {
    pub address: String,
    pub cors_origin: Option<String>,
}

/// Initialize the server and return a future that will actually handle requests.
pub async fn init_server(
    lib_context: Arc<LibContext>,
    settings: ServerSettings,
) -> Result<BoxFuture<'static, ()>> {
    let mut cors = CorsLayer::default();
    if let Some(ui_cors_origin) = &settings.cors_origin {
        cors = cors
            .allow_origin(AllowOrigin::exact(ui_cors_origin.parse()?))
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::DELETE,
            ])
            .allow_headers([axum::http::header::CONTENT_TYPE]);
    }
    let app = Router::new()
        .route("/api/flows", routing::get(service::flows::list_flows))
        .route(
            "/api/flows/:flowInstName",
            routing::get(service::flows::get_flow_spec),
        )
        .route(
            "/api/flows/:flowInstName/schema",
            routing::get(service::flows::get_flow_schema),
        )
        .route(
            "/api/flows/:flowInstName/keys",
            routing::get(service::flows::get_keys),
        )
        .route(
            "/api/flows/:flowInstName/data",
            routing::get(service::flows::evaluate_data),
        )
        .route(
            "/api/flows/:flowInstName/update",
            routing::post(service::flows::update),
        )
        .route(
            "/api/flows/:flowInstName/search",
            routing::get(service::search::search),
        )
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(cors),
        )
        .with_state(lib_context.clone());

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(&settings.address)
        .await
        .unwrap();

    println!("Server running at http://{}/", settings.address);
    let serve_fut = async { axum::serve(listener, app).await.unwrap() };
    Ok(serve_fut.boxed())
}
