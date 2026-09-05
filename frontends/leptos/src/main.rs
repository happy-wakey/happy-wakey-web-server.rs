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
        let alarm_views = data.alarms.into_iter().map(|alarm| view! { <article class="alarm"><time>{alarm.local_time}</time><div><strong>{alarm.label}</strong><p>{alarm.time_zone}</p></div><span>"Scheduled"</span></article> }).collect_view();
        view! {
            <!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>"Happy Wakey · Leptos morning HUD"</title><style>{CSS}</style></head>
            <body data-renderer="leptos">
                <header class="topbar"><a class="brand" href="/"><span>"HW"</span>"Happy Wakey"</a><nav><a href="#today">"Today"</a><a href="#policy">"Link policy"</a><a href="https://user.hawky.pro/">"Account ↗"</a></nav><p>"LEPTOS · "{data.interaction_mode}</p></header>
                <main>
                    <section class="welcome"><div><p class="eyebrow">"YOUR FINITE MORNING BRIEF"</p><h1>"Your morning, already in focus."</h1><p class="lede">"Signed in as "{data.identity_label}". The useful things are here; the feeds are not."</p></div><aside><span>"LINK POLICY"</span><strong>{data.usefulness_threshold}</strong><small>"minimum usefulness score"</small></aside></section>
                    <div class="statusbar"><span>"● Shared Auth verified"</span><span>"Ores Middleware active"</span><span>"Card limit "{data.max_cards}</span></div>
                    <div class="dashboard" id="today">
                        <section class="panel"><header><p>"NEXT UP"</p><h2>"Alarms & calendar"</h2></header>{alarm_views}</section>
                        <section class="panel policy" id="policy"><header><p>"USEFUL MESSAGES"</p><h2>"Only the thread. Never the feed."</h2></header><strong class="score">"Decision ≥ "{data.usefulness_threshold}</strong><p>"A message action requires live consent, matching classifier evidence, and an unexpired item-specific HTTPS link."</p><button disabled>"No qualified message action"</button></section>
                        <section class="panel lanes"><header><p>"CONTEXT LANES"</p><h2>"Glance, decide, continue."</h2></header><div><article><b>"✦"</b><strong>"This day in history"</strong><small>"Awaiting a cited source"</small></article><article><b>"≈"</b><strong>"Weather & outlook"</strong><small>"Long range is uncertainty-labelled"</small></article><article><b>"✈"</b><strong>"Travel"</strong><small>"Destination-aware when consented"</small></article><article><b>"↗"</b><strong>"KPIs & markets"</strong><small>"Role-scoped chosen signals"</small></article></div></section>
                        <section class="panel audio"><b>"▶"</b><div><p>"OPTIONAL AUDIO"</p><h2>"The same authorized brief, out loud."</h2></div><strong>"≈ 03:00"</strong></section>
                        <section class="panel sync"><header><p>"OPTO SYNC"</p><h2>"Layout converges across devices."</h2></header><pre>{data.preferences.to_string()}</pre></section>
                    </div>
                </main>
                <footer><span>"Leptos SSR + Axum"</span><span>"Ores Middleware · Shared Auth · Opto Sync · Ores OTEL"</span><span>"No infinite scroll"</span></footer>
            </body></html>
        }.into_any().to_html()
    })
}

const CSS: &str = ":root{font-family:Inter,system-ui,sans-serif;background:#f4f0e6;color:#17241f}*{box-sizing:border-box}body{margin:0}.topbar{display:grid;grid-template-columns:1fr auto 1fr;align-items:center;min-height:4.5rem;padding:0 4vw;border-bottom:1px solid #cfc8b8}.brand{display:flex;align-items:center;gap:.7rem;width:max-content;font-weight:800;text-decoration:none}.brand span{display:grid;width:2rem;height:2rem;place-items:center;color:white;background:#e95d2a;border-radius:50%;font:700 .65rem monospace}.topbar nav{display:flex;gap:2rem;font:700 .65rem monospace;text-transform:uppercase}.topbar nav a{text-decoration:none}.topbar>p{justify-self:end;color:#68736b;font:.62rem monospace;text-transform:uppercase}main{max-width:88rem;margin:auto;padding:4vw}.welcome{display:grid;grid-template-columns:1fr auto;align-items:end;gap:2rem;padding:3rem 0}.eyebrow,.panel header p,.audio p{margin:0 0 .8rem;color:#e95d2a;font:700 .63rem monospace;letter-spacing:.09em}.welcome h1{margin:0;font:500 clamp(3rem,7vw,7rem)/.9 Georgia,serif;letter-spacing:-.06em}.lede{color:#657069}.welcome aside{padding:1.2rem;border:1px solid #cfc8b8}.welcome aside span,.welcome aside small{display:block;color:#6d766f;font:.6rem monospace}.welcome aside strong{display:block;margin:.25rem 0;color:#e95d2a;font:500 2.8rem Georgia,serif}.statusbar{display:flex;gap:1.4rem;padding:.85rem 1rem;color:#367456;background:#e0e7e0;border:1px solid #bdcabf;font:.6rem monospace;text-transform:uppercase}.dashboard{display:grid;grid-template-columns:1.2fr .8fr;gap:1rem;margin-top:1rem}.panel{padding:1.4rem;background:#faf8f1;border:1px solid #cfc8b8}.panel header h2,.audio h2{margin:0;font:500 1.6rem Georgia,serif}.alarm{display:grid;grid-template-columns:auto 1fr auto;gap:1rem;align-items:center;margin-top:1rem;padding-top:1rem;border-top:1px solid #ddd7ca}.alarm time{font:500 1.7rem Georgia,serif}.alarm p{margin:.2rem 0;color:#68736b}.alarm>span{color:#367456;font:.6rem monospace}.policy{grid-row:span 2;color:#f4f0e6;background:#17241f}.policy p{color:#b6beb9;line-height:1.6}.policy .score{display:block;margin:1.5rem 0;color:#e95d2a;font:500 1.5rem Georgia,serif}.policy button{width:100%;padding:.8rem;color:#929b95;background:#2d3a34;border:1px solid #536159}.lanes>div{display:grid;grid-template-columns:1fr 1fr;margin-top:1rem}.lanes article{display:grid;grid-template-columns:auto 1fr;gap:.25rem .7rem;padding:1rem;border:1px solid #ddd7ca}.lanes b{grid-row:1/3;color:#e95d2a}.lanes small{color:#6d766f}.audio{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:1rem;background:#dce8df}.audio>b{display:grid;width:3rem;height:3rem;place-items:center;color:white;background:#e95d2a;border-radius:50%}.sync pre{overflow:auto;padding:1rem;background:#ece8de;white-space:pre-wrap}footer{display:flex;justify-content:space-between;padding:1.3rem 4vw;color:#707970;border-top:1px solid #cfc8b8;font:.58rem monospace;text-transform:uppercase}@media(max-width:800px){.topbar{grid-template-columns:1fr auto}.topbar nav{display:none}.welcome,.dashboard{grid-template-columns:1fr}.policy{grid-row:auto}}@media(max-width:520px){main{padding:1rem}.welcome h1{font-size:3.2rem}.lanes>div{grid-template-columns:1fr}.audio{grid-template-columns:auto 1fr}.audio>strong{grid-column:2}footer{flex-direction:column;gap:.6rem}}";
