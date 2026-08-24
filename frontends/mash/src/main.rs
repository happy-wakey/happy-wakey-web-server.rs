use std::{env, sync::Arc};

use axum::{extract::State, http::HeaderMap, response::Html, routing::get, Router};
use happy_wakey_web_common::{Config, Dashboard, Runtime, WebError};
use maud::{html, DOCTYPE};
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::new(Config::from_env(), "mash")?);
    let app = Router::new()
        .route("/", get(home))
        .route("/healthz", get(|| async { "ok" }))
        .layer(TraceLayer::new_for_http())
        .with_state(runtime);
    let port = env::var("HAPPY_WAKEY_MASH_PORT").unwrap_or_else(|_| "8131".into());
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
    html! {
        (DOCTYPE)
        html lang="en" { head { meta charset="utf-8"; meta name="viewport" content="width=device-width,initial-scale=1"; title { "Happy Wakey · MASH" } style { (CSS) } }
        body { main { p class="eyebrow" { "HAPPY WAKEY · MASH" } h1 { "Wake up with your day already in focus." } p { "Signed in as " (data.identity_label) }
            section { h2 { "Alarms" } @if data.alarms.is_empty() { p { "No alarms yet." } } @for alarm in data.alarms { article { strong { (alarm.local_time) " · " (alarm.label) } p { (alarm.time_zone) } } } }
            section { h2 { "Synced layout" } pre { (data.preferences) } }
        } footer { "Maud + Axum · Shared Auth · Opto Sync · Ores OTEL" } } }
    }.into_string()
}

const CSS: &str = ":root{color-scheme:dark;font-family:system-ui;background:#07131d;color:#eff8ff}body{margin:0;background:radial-gradient(circle at top,#17334a,#07131d 55%)}main,footer{max-width:70rem;margin:auto;padding:2rem}h1{font-size:clamp(2.4rem,7vw,5.5rem);max-width:14ch;line-height:1}.eyebrow{letter-spacing:.18em;color:#74d9ff}section{background:#0e2232;border:1px solid #27465b;border-radius:18px;padding:1.3rem;margin:1rem 0}article{padding:.8rem 0;border-top:1px solid #27465b}pre{white-space:pre-wrap}footer{color:#92aebe}";
