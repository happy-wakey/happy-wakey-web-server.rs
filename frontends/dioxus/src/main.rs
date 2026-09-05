use std::sync::Arc;

use axum::{extract::State, http::HeaderMap, response::Html, routing::get, Router};
use dioxus::prelude::*;
use happy_wakey_web_common::{flags, Config, Dashboard, Runtime, WebError};
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Some(output) = flags::process_control(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .map_err(std::io::Error::other)?
    {
        print!("{output}");
        return Ok(());
    }
    let runtime = Arc::new(Runtime::new(Config::from_env()?, "dioxus")?);
    let app = Router::new()
        .route("/", get(home))
        .route("/healthz", get(|| async { "ok" }))
        .layer(TraceLayer::new_for_http())
        .with_state(runtime);
    let port = flags::var("HAPPY_WAKEY_DIOXUS_PORT").unwrap_or_else(|_| "8133".into());
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
    interaction_mode: &'static str,
    usefulness_threshold: f32,
    max_cards: usize,
}

fn page(props: PageProps) -> Element {
    rsx! {
        head {
            meta { charset: "utf-8" }
            meta { name: "viewport", content: "width=device-width,initial-scale=1" }
            title { "Happy Wakey · Dioxus morning HUD" }
            style { {CSS} }
        }
        body {
            header { class: "topbar",
                a { class: "brand", href: "/", span { "HW" } "Happy Wakey" }
                nav { aria_label: "Product",
                    a { href: "#today", "Today" }
                    a { href: "#policy", "Link policy" }
                    a { href: "https://user.hawky.pro/", "Account ↗" }
                }
                p { "DIOXUS · {props.interaction_mode}" }
            }
            main {
                section { class: "welcome",
                    div {
                        p { class: "eyebrow", "YOUR FINITE MORNING BRIEF" }
                        h1 { "A calm morning starts with controlled attention." }
                        p { class: "lede", "Signed in as {props.identity}. Useful context in; algorithmic feeds out." }
                    }
                    aside {
                        span { "LINK POLICY" }
                        strong { "{props.usefulness_threshold}" }
                        small { "minimum usefulness score" }
                    }
                }
                div { class: "statusbar",
                    span { "● Shared Auth verified" }
                    span { "Ores Middleware active" }
                    span { "Card limit {props.max_cards}" }
                }
                div { class: "dashboard", id: "today",
                    section { class: "panel",
                        header { p { "NEXT UP" } h2 { "Alarms & calendar" } }
                        for alarm in props.alarms {
                            article { class: "alarm",
                                time { "{alarm.local_time}" }
                                div { strong { "{alarm.label}" } p { "{alarm.time_zone}" } }
                                span { "Scheduled" }
                            }
                        }
                    }
                    section { class: "panel policy", id: "policy",
                        header { p { "USEFUL MESSAGES" } h2 { "Only the thread. Never the feed." } }
                        strong { class: "score", "Decision ≥ {props.usefulness_threshold}" }
                        p { "A message action requires live consent, matching classifier evidence, and an unexpired item-specific HTTPS link." }
                        button { disabled: true, "No qualified message action" }
                    }
                    section { class: "panel lanes",
                        header { p { "CONTEXT LANES" } h2 { "Glance, decide, continue." } }
                        div {
                            article { b { "✦" } strong { "This day in history" } small { "Awaiting a cited source" } }
                            article { b { "≈" } strong { "Weather & outlook" } small { "Long range is uncertainty-labelled" } }
                            article { b { "✈" } strong { "Travel" } small { "Destination-aware when consented" } }
                            article { b { "↗" } strong { "KPIs & markets" } small { "Role-scoped chosen signals" } }
                        }
                    }
                    section { class: "panel audio",
                        b { "▶" }
                        div { p { "OPTIONAL AUDIO" } h2 { "The same authorized brief, out loud." } }
                        strong { "≈ 03:00" }
                    }
                    section { class: "panel sync",
                        header { p { "OPTO SYNC" } h2 { "Layout converges across devices." } }
                        pre { "{props.preferences}" }
                    }
                }
            }
            footer {
                span { "Dioxus SSR + Axum" }
                span { "Ores Middleware · Shared Auth · Opto Sync · Ores OTEL" }
                span { "No infinite scroll" }
            }
        }
    }
}

fn render(data: Dashboard) -> String {
    let props = PageProps {
        identity: data.identity_label,
        alarms: data.alarms,
        preferences: data.preferences.to_string(),
        interaction_mode: data.interaction_mode,
        usefulness_threshold: data.usefulness_threshold,
        max_cards: data.max_cards,
    };
    let mut dom = VirtualDom::new_with_props(page, props);
    dom.rebuild_in_place();
    format!(
        "<!DOCTYPE html><html lang=\"en\">{}</html>",
        dioxus_ssr::render(&dom)
    )
}

const CSS: &str = r#":root{font-family:Inter,system-ui,sans-serif;background:#f4f0e6;color:#17241f}*{box-sizing:border-box}body{margin:0}.topbar{display:grid;grid-template-columns:1fr auto 1fr;align-items:center;min-height:4.5rem;padding:0 4vw;border-bottom:1px solid #cfc8b8}.brand{display:flex;align-items:center;gap:.7rem;width:max-content;font-weight:800;text-decoration:none}.brand span{display:grid;width:2rem;height:2rem;place-items:center;color:white;background:#e95d2a;border-radius:50%;font:700 .65rem monospace}.topbar nav{display:flex;gap:2rem;font:700 .65rem monospace;text-transform:uppercase}.topbar nav a{text-decoration:none}.topbar>p{justify-self:end;color:#68736b;font:.62rem monospace;text-transform:uppercase}main{max-width:88rem;margin:auto;padding:4vw}.welcome{display:grid;grid-template-columns:1fr auto;align-items:end;gap:2rem;padding:3rem 0}.eyebrow,.panel header p,.audio p{margin:0 0 .8rem;color:#e95d2a;font:700 .63rem monospace;letter-spacing:.09em}.welcome h1{margin:0;font:500 clamp(3rem,7vw,7rem)/.9 Georgia,serif;letter-spacing:-.06em}.lede{color:#657069}.welcome aside{padding:1.2rem;border:1px solid #cfc8b8}.welcome aside span,.welcome aside small{display:block;color:#6d766f;font:.6rem monospace}.welcome aside strong{display:block;margin:.25rem 0;color:#e95d2a;font:500 2.8rem Georgia,serif}.statusbar{display:flex;gap:1.4rem;padding:.85rem 1rem;color:#367456;background:#e0e7e0;border:1px solid #bdcabf;font:.6rem monospace;text-transform:uppercase}.dashboard{display:grid;grid-template-columns:1.2fr .8fr;gap:1rem;margin-top:1rem}.panel{padding:1.4rem;background:#faf8f1;border:1px solid #cfc8b8}.panel header h2,.audio h2{margin:0;font:500 1.6rem Georgia,serif}.alarm{display:grid;grid-template-columns:auto 1fr auto;gap:1rem;align-items:center;margin-top:1rem;padding-top:1rem;border-top:1px solid #ddd7ca}.alarm time{font:500 1.7rem Georgia,serif}.alarm p{margin:.2rem 0;color:#68736b}.alarm>span{color:#367456;font:.6rem monospace}.policy{grid-row:span 2;color:#f4f0e6;background:#17241f}.policy p{color:#b6beb9;line-height:1.6}.policy .score{display:block;margin:1.5rem 0;color:#e95d2a;font:500 1.5rem Georgia,serif}.policy button{width:100%;padding:.8rem;color:#929b95;background:#2d3a34;border:1px solid #536159}.lanes>div{display:grid;grid-template-columns:1fr 1fr;margin-top:1rem}.lanes article{display:grid;grid-template-columns:auto 1fr;gap:.25rem .7rem;padding:1rem;border:1px solid #ddd7ca}.lanes b{grid-row:1/3;color:#e95d2a}.lanes small{color:#6d766f}.audio{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:1rem;background:#dce8df}.audio>b{display:grid;width:3rem;height:3rem;place-items:center;color:white;background:#e95d2a;border-radius:50%}.sync pre{overflow:auto;padding:1rem;background:#ece8de;white-space:pre-wrap}footer{display:flex;justify-content:space-between;padding:1.3rem 4vw;color:#707970;border-top:1px solid #cfc8b8;font:.58rem monospace;text-transform:uppercase}@media(max-width:800px){.topbar{grid-template-columns:1fr auto}.topbar nav{display:none}.welcome,.dashboard{grid-template-columns:1fr}.policy{grid-row:auto}}@media(max-width:520px){main{padding:1rem}.welcome h1{font-size:3.2rem}.lanes>div{grid-template-columns:1fr}.audio{grid-template-columns:auto 1fr}.audio>strong{grid-column:2}footer{flex-direction:column;gap:.6rem}}"#;
