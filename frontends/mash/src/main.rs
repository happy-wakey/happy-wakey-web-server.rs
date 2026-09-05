use std::sync::Arc;

use axum::{extract::State, http::HeaderMap, response::Html, routing::get, Router};
use happy_wakey_web_common::{flags, Config, Dashboard, Runtime, WebError};
use maud::{html, DOCTYPE};
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Some(output) = flags::process_control(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .map_err(std::io::Error::other)?
    {
        print!("{output}");
        return Ok(());
    }
    let runtime = Arc::new(Runtime::new(Config::from_env()?, "mash")?);
    let app = Router::new()
        .route("/", get(home))
        .route("/healthz", get(|| async { "ok" }))
        .layer(TraceLayer::new_for_http())
        .with_state(runtime);
    let port = flags::var("HAPPY_WAKEY_MASH_PORT").unwrap_or_else(|_| "8131".into());
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
        html lang="en" {
            head { meta charset="utf-8"; meta name="viewport" content="width=device-width,initial-scale=1"; title { "Happy Wakey · Morning HUD" } style { (CSS) } }
            body data-renderer="mash" {
                header class="topbar" {
                    a class="brand" href="/" { span { "HW" } "Happy Wakey" }
                    nav aria-label="Product" { a href="#today" { "Today" } a href="#sources" { "Sources" } a href="https://user.hawky.pro/" { "Account ↗" } }
                    p { "MASH · " (data.interaction_mode) }
                }
                main {
                    section class="welcome" {
                        div { p class="eyebrow" { "YOUR FINITE MORNING BRIEF" } h1 { "Good morning, " (data.identity_label) "." } p class="lede" { "The useful things are here. The feeds are not." } }
                        aside { span { "LINK POLICY" } strong { (data.usefulness_threshold) } small { "minimum usefulness score" } }
                    }
                    div class="statusbar" { span class="live" { "● Briefing channel ready" } span { "Shared Auth verified" } span { "Card limit " (data.max_cards) } }
                    div class="dashboard" id="today" {
                        section class="panel alarms" {
                            header { p { "NEXT UP" } h2 { "Alarms & calendar" } }
                            @if data.alarms.is_empty() {
                                div class="empty" { strong { "Your morning is clear." } p { "No alarms were returned through the selected service avenue." } }
                            } @else {
                                @for alarm in &data.alarms {
                                    article class="alarm" { time { (alarm.local_time) } div { strong { (&alarm.label) } p { (&alarm.time_zone) } } span { "Scheduled" } }
                                }
                            }
                        }
                        section class="panel message-policy" {
                            header { p { "USEFUL MESSAGES" } h2 { "Only the thread. Never the feed." } }
                            div class="decision" { span { "AI decision required" } strong { "≥ " (data.usefulness_threshold) } }
                            p { "A social or email action appears only with live connector consent, a matching decision, and an expiring item-specific HTTPS link." }
                            ul { li { "VIP or explicit reply request" } li { "Bottleneck, escalation, safety or travel impact" } li { "Generic home and timeline routes denied" } }
                            button type="button" disabled { "No qualified message action" }
                        }
                        section class="panel context" id="sources" {
                            header { p { "CONTEXT LANES" } h2 { "Glance, decide, continue." } }
                            div class="lane-grid" {
                                article { span { "✦" } strong { "This day in history" } small { "Awaiting a cited source" } }
                                article { span { "≈" } strong { "Weather & outlook" } small { "Long-range items show uncertainty" } }
                                article { span { "✈" } strong { "Travel" } small { "Destination-aware when consented" } }
                                article { span { "↗" } strong { "KPIs & markets" } small { "Role-scoped, chosen signals" } }
                            }
                        }
                        section class="panel audio" {
                            div class="play" { "▶" }
                            div { p { "OPTIONAL AUDIO" } h2 { "Your same authorized brief, out loud." } span { "No extra content hiding behind play." } }
                            strong { "≈ 03:00" }
                        }
                        section class="panel sync" {
                            header { p { "OPTO SYNC" } h2 { "Layout converges across devices." } }
                            pre { (data.preferences) }
                        }
                    }
                }
                footer { span { "MASH + Maud + Axum" } span { "Ores Middleware · Shared Auth · Opto Sync · Ores OTEL" } span { "No infinite scroll" } }
            }
        }
    }.into_string()
}

const CSS: &str = r#"
:root{font-family:Inter,system-ui,sans-serif;background:#f4f0e6;color:#17241f}*{box-sizing:border-box}body{margin:0;background:#f4f0e6}a{color:inherit}.topbar{display:grid;grid-template-columns:1fr auto 1fr;align-items:center;min-height:4.5rem;padding:0 4vw;border-bottom:1px solid #cfc8b8}.brand{display:flex;align-items:center;gap:.7rem;width:max-content;font-weight:800;text-decoration:none}.brand span{display:grid;width:2rem;height:2rem;place-items:center;color:white;background:#e95d2a;border-radius:50%;font:700 .65rem/1 monospace}.topbar nav{display:flex;gap:2rem;color:#68736b;font:700 .65rem/1 monospace;text-transform:uppercase}.topbar nav a{text-decoration:none}.topbar>p{justify-self:end;color:#68736b;font:.62rem/1 monospace;text-transform:uppercase}main{max-width:88rem;margin:auto;padding:4vw}.welcome{display:grid;grid-template-columns:1fr auto;align-items:end;gap:2rem;padding:3rem 0}.eyebrow,.panel header p,.audio p{margin:0 0 .8rem;color:#e95d2a;font:700 .63rem/1 monospace;letter-spacing:.09em}.welcome h1{margin:0;font:500 clamp(3rem,7vw,7rem)/.9 Georgia,serif;letter-spacing:-.06em}.lede{margin:1rem 0 0;color:#657069;font-size:1.1rem}.welcome aside{min-width:11rem;padding:1.2rem;border:1px solid #cfc8b8;border-radius:.4rem}.welcome aside span,.welcome aside small{display:block;color:#6d766f;font:.6rem/1.4 monospace}.welcome aside strong{display:block;margin:.25rem 0;color:#e95d2a;font:500 2.8rem/1 Georgia,serif}.statusbar{display:flex;flex-wrap:wrap;gap:1.3rem;padding:.85rem 1rem;color:#6c756e;background:#e4e0d6;border:1px solid #cfc8b8;font:.6rem/1 monospace;text-transform:uppercase}.statusbar .live{color:#2d7957}.dashboard{display:grid;grid-template-columns:1.25fr .75fr;gap:1rem;margin-top:1rem}.panel{padding:1.4rem;background:#faf8f1;border:1px solid #cfc8b8;border-radius:.45rem}.panel header h2,.audio h2{margin:0;font:500 1.65rem/1.1 Georgia,serif;letter-spacing:-.035em}.alarm{display:grid;grid-template-columns:auto 1fr auto;gap:1rem;align-items:center;margin-top:1rem;padding-top:1rem;border-top:1px solid #ded9ce}.alarm time{font:500 1.8rem/1 Georgia,serif}.alarm p{margin:.3rem 0 0;color:#6c756e;font-size:.7rem}.alarm>span{padding:.35rem .55rem;color:#2d7957;background:#dcebe3;border-radius:2rem;font:.58rem/1 monospace}.empty{margin-top:1.5rem;padding:2rem;background:#eeeae0;border-radius:.35rem}.empty p{margin:.4rem 0 0;color:#6c756e}.message-policy{grid-row:span 2;color:#f5f1e8;background:#17241f;border-color:#17241f}.message-policy header p{color:#fb7647}.message-policy p,.message-policy li{color:#b5beb8;line-height:1.6}.message-policy ul{padding-left:1.2rem}.decision{display:flex;justify-content:space-between;align-items:center;margin:1.5rem 0;padding:1rem;color:#17241f;background:#f5f1e8;border-radius:.35rem;font:.65rem/1 monospace}.decision strong{color:#e95d2a;font-size:1.35rem}.message-policy button{width:100%;padding:.8rem;color:#8b948e;background:#2b3832;border:1px solid #4c5a53;border-radius:.3rem}.lane-grid{display:grid;grid-template-columns:1fr 1fr;margin-top:1.2rem;border-top:1px solid #ded9ce;border-left:1px solid #ded9ce}.lane-grid article{display:grid;grid-template-columns:auto 1fr;gap:.25rem .7rem;padding:1rem;border-right:1px solid #ded9ce;border-bottom:1px solid #ded9ce}.lane-grid article>span{grid-row:1/3;color:#e95d2a}.lane-grid small{color:#747d76}.audio{display:grid;grid-template-columns:auto 1fr auto;gap:1rem;align-items:center;background:#dce8df}.play{display:grid;width:3rem;height:3rem;place-items:center;color:white;background:#e95d2a;border-radius:50%}.audio p{margin-bottom:.35rem}.audio span{display:block;margin-top:.45rem;color:#66716a;font-size:.72rem}.audio>strong{font:500 1.25rem/1 Georgia,serif}.sync pre{overflow:auto;margin:1rem 0 0;padding:1rem;color:#4d5d54;background:#ece8de;font-size:.72rem;white-space:pre-wrap}footer{display:flex;justify-content:space-between;gap:1rem;padding:1.3rem 4vw;color:#707970;border-top:1px solid #cfc8b8;font:.58rem/1.4 monospace;text-transform:uppercase}@media(max-width:850px){.topbar{grid-template-columns:1fr auto}.topbar nav{display:none}.dashboard{grid-template-columns:1fr}.message-policy{grid-row:auto}.welcome{grid-template-columns:1fr}.welcome aside{width:100%}}@media(max-width:520px){main{padding:1rem}.welcome{padding:3rem 0}.welcome h1{font-size:3.3rem}.lane-grid{grid-template-columns:1fr}.audio{grid-template-columns:auto 1fr}.audio>strong{grid-column:2}.topbar>p{display:none}footer{flex-direction:column}}
"#;
