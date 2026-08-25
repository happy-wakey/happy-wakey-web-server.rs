use std::{env, sync::Arc};

use axum::{extract::State, http::HeaderMap, response::Html, routing::get, Router};
use dioxus::prelude::*;
use happy_wakey_web_common::{Config, Dashboard, Runtime, WebError};
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::new(Config::from_env()?, "dioxus").await?);
    let app = Router::new()
        .route("/", get(home))
        .route("/healthz", get(|| async { "ok" }))
        .layer(TraceLayer::new_for_http())
        .with_state(runtime);
    let port = env::var("HAPPY_WAKEY_DIOXUS_PORT").unwrap_or_else(|_| "8133".into());
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

#[derive(Clone, PartialEq, Props)]
struct PageProps {
    identity: String,
    alarms: Vec<happy_wakey_web_common::Alarm>,
    preferences: String,
}

fn page(props: PageProps) -> Element {
    rsx! { head { meta { charset: "utf-8" } meta { name: "viewport", content: "width=device-width,initial-scale=1" } title { "Happy Wakey · Dioxus" } style { {CSS} } }
    body { main { p { class: "eyebrow", "HAPPY WAKEY · DIOXUS SSR" } h1 { "A calm morning starts with controlled state." } p { "Signed in as {props.identity}" }
        section { h2 { "Alarms" } for alarm in props.alarms { article { strong { "{alarm.local_time} · {alarm.label}" } p { "{alarm.time_zone}" } } } }
        section { h2 { "Synced layout" } pre { "{props.preferences}" } }
    } footer { "Dioxus + Axum · Shared Auth · Opto Sync · Ores OTEL" } } }
}

fn render(data: Dashboard) -> String {
    let props = PageProps {
        identity: data.identity_label,
        alarms: data.alarms,
        preferences: data.preferences.to_string(),
    };
    let mut dom = VirtualDom::new_with_props(page, props);
    dom.rebuild_in_place();
    format!(
        "<!DOCTYPE html><html lang=\"en\">{}</html>",
        dioxus_ssr::render(&dom)
    )
}

const CSS: &str = ":root{color-scheme:dark;font-family:system-ui;background:#071711;color:#edfff7}body{margin:0;background:linear-gradient(145deg,#163c2d,#071711 55%)}main,footer{max-width:70rem;margin:auto;padding:2rem}h1{font-size:clamp(2.4rem,7vw,5.5rem);max-width:15ch;line-height:1}.eyebrow{letter-spacing:.18em;color:#75efbb}section{background:#0d2a20;border:1px solid #285e49;border-radius:18px;padding:1.3rem;margin:1rem 0}article{padding:.8rem 0;border-top:1px solid #285e49}pre{white-space:pre-wrap}footer{color:#9ec7b5}";
