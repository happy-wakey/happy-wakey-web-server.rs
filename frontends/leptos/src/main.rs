use std::sync::Arc;

use axum::{extract::State, http::HeaderMap, response::Html, routing::get, Router};
use happy_wakey_web_common::{flags, Config, Dashboard, Runtime, WebError};
use leptos::prelude::*;
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Some(output) = flags::process_control(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .map_err(std::io::Error::other)?
    {
        print!("{output}");
        return Ok(());
    }
    let runtime = Arc::new(Runtime::new(Config::from_env()?, "leptos")?);
    let app = Router::new()
        .route("/", get(home))
        .route("/healthz", get(|| async { "ok" }))
        .layer(TraceLayer::new_for_http())
        .with_state(runtime);
    let port = flags::var("HAPPY_WAKEY_LEPTOS_PORT").unwrap_or_else(|_| "8132".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn home(
    State(runtime): State<Arc<Runtime>>,
    headers: HeaderMap,
) -> Result<Html<String>, WebError> {
    Ok(Html(render(runtime.dashboard(&headers).await?)))
}

fn render(data: Dashboard) -> String {
    let owner = Owner::new();
    owner.with(|| {
        let alarm_views = data.alarms.into_iter().map(|alarm| view! { <article><strong>{alarm.local_time}" · "{alarm.label}</strong><p>{alarm.time_zone}</p></article> }).collect_view();
        view! { <!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>"Happy Wakey · Leptos"</title><style>{CSS}</style></head><body><main><p class="eyebrow">"HAPPY WAKEY · LEPTOS SSR"</p><h1>"Your morning, rendered from one verified state."</h1><p>"Signed in as "{data.identity_label}</p><section><h2>"Alarms"</h2>{alarm_views}</section><section><h2>"Synced layout"</h2><pre>{data.preferences.to_string()}</pre></section></main><footer>"Leptos + Axum · Shared Auth · Opto Sync · Ores OTEL"</footer></body></html> }.into_any().to_html()
    })
}

const CSS: &str = ":root{color-scheme:dark;font-family:system-ui;background:#111019;color:#fff7ed}body{margin:0;background:linear-gradient(150deg,#402312,#111019 50%)}main,footer{max-width:70rem;margin:auto;padding:2rem}h1{font-size:clamp(2.4rem,7vw,5.5rem);max-width:15ch;line-height:1}.eyebrow{letter-spacing:.18em;color:#ffbd76}section{background:#211b21;border:1px solid #5c3d31;border-radius:18px;padding:1.3rem;margin:1rem 0}article{padding:.8rem 0;border-top:1px solid #5c3d31}pre{white-space:pre-wrap}footer{color:#c9a992}";
