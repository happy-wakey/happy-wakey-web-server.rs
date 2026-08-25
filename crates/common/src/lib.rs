mod transport;

use std::{env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};

use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
pub use happy_wakey_interfaces::Alarm;
use happy_wakey_interfaces::ApiError;
use next_loggers::{json, Logger, Map, Options};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use syncer_rs::{merge_json, MergeOptions};
pub use transport::TransportMode;
use transport::{credentials_path, database_flavor, ServiceGateway};

#[derive(Clone, Debug)]
pub struct Config {
    pub api_base: String,
    pub shared_auth_base: String,
    pub shared_auth_audience: String,
    pub introspect_secret: Option<String>,
    transport_mode: TransportMode,
    database_url: Option<String>,
    database_flavor: happy_wakey_lib_core::DatabaseFlavor,
    database_max_connections: u32,
    tcp_address: Option<String>,
    tcp_server_name: Option<String>,
    nats_url: Option<String>,
    nats_credentials_path: Option<PathBuf>,
    nats_response_stream: String,
    nats_response_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let transport_mode = TransportMode::parse(
            &env::var("HAPPY_WAKEY_WEB_API_TRANSPORT").unwrap_or_else(|_| "http".into()),
        )?;
        let database_max_connections = env::var("HAPPY_WAKEY_WEB_DB_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "8".into())
            .parse::<u32>()
            .context("HAPPY_WAKEY_WEB_DB_MAX_CONNECTIONS must be an integer")?;
        anyhow::ensure!(
            (1..=64).contains(&database_max_connections),
            "HAPPY_WAKEY_WEB_DB_MAX_CONNECTIONS must be between 1 and 64"
        );
        let nats_response_timeout = env::var("HAPPY_WAKEY_NATS_RESPONSE_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "15".into())
            .parse::<u64>()
            .context("HAPPY_WAKEY_NATS_RESPONSE_TIMEOUT_SECONDS must be an integer")?;
        anyhow::ensure!(
            (1..=120).contains(&nats_response_timeout),
            "HAPPY_WAKEY_NATS_RESPONSE_TIMEOUT_SECONDS must be between 1 and 120"
        );
        Ok(Self {
            api_base: env::var("HAPPY_WAKEY_API_BASE")
                .unwrap_or_else(|_| "https://api.happy-wakey.dev".into()),
            shared_auth_base: env::var("HAPPY_WAKEY_SHARED_AUTH_BASE")
                .unwrap_or_else(|_| "https://auth.oresoftware.dev".into()),
            shared_auth_audience: env::var("HAPPY_WAKEY_SHARED_AUTH_AUDIENCE")
                .unwrap_or_else(|_| "happy-wakey".into()),
            introspect_secret: env::var("HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty()),
            transport_mode,
            database_url: optional_env("DATABASE_URL"),
            database_flavor: database_flavor(
                &env::var("HAPPY_WAKEY_DATABASE_FLAVOR").unwrap_or_else(|_| "postgresql".into()),
            )?,
            database_max_connections,
            tcp_address: optional_env("HAPPY_WAKEY_API_TCP_ADDR"),
            tcp_server_name: optional_env("HAPPY_WAKEY_API_TCP_SERVER_NAME"),
            nats_url: optional_env("HAPPY_WAKEY_NATS_URL"),
            nats_credentials_path: credentials_path(optional_env(
                "HAPPY_WAKEY_NATS_CREDENTIALS_FILE",
            )),
            nats_response_stream: env::var("HAPPY_WAKEY_NATS_RESPONSE_STREAM")
                .unwrap_or_else(|_| "HAPPY_WAKEY_RESPONSES".into()),
            nats_response_timeout: Duration::from_secs(nats_response_timeout),
        })
    }
}

pub struct Runtime {
    config: Config,
    http: reqwest::Client,
    telemetry: Arc<Logger>,
    gateway: ServiceGateway,
}

#[derive(Clone, Debug, Deserialize)]
struct Introspection {
    active: bool,
    #[serde(default)]
    sub: String,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    roles: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct Identity {
    pub subject: String,
    pub email: Option<String>,
    pub roles: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Dashboard {
    pub identity_label: String,
    pub alarms: Vec<Alarm>,
    pub preferences: Value,
}

#[derive(Debug)]
pub enum WebError {
    Unauthorized,
    AuthUnavailable,
    ApiUnavailable,
    Contract(String),
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let (status, code, message, retryable) = match self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Sign in with Shared Auth",
                false,
            ),
            Self::AuthUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "auth_unavailable",
                "Shared Auth could not establish an identity",
                true,
            ),
            Self::ApiUnavailable => (
                StatusCode::BAD_GATEWAY,
                "api_unavailable",
                "Happy Wakey API is unavailable",
                true,
            ),
            Self::Contract(ref message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "contract_invalid",
                message.as_str(),
                false,
            ),
        };
        (
            status,
            Json(ApiError {
                code: code.into(),
                message: message.into(),
                retryable,
                trace_id: None,
            }),
        )
            .into_response()
    }
}

impl Runtime {
    pub async fn new(config: Config, lane: &str) -> Result<Self> {
        let telemetry = Arc::new(Logger::new(Options {
            app_name: format!("happy-wakey-{lane}"),
            ..Options::default()
        }));
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("build bounded HTTP client")?;
        let gateway = ServiceGateway::connect(&config).await?;
        Ok(Self {
            config,
            http,
            telemetry,
            gateway,
        })
    }

    pub async fn dashboard(&self, headers: &HeaderMap) -> Result<Dashboard, WebError> {
        let token = bearer(headers).ok_or(WebError::Unauthorized)?;
        let identity = self.introspect(token).await?;
        let alarms = self
            .gateway
            .list_alarms(&self.http, &self.config, token, &identity)
            .await?;
        let preferences = merge_preferences(
            r#"{"theme":"system","tiles":["clock"]}"#,
            r#"{"tiles":["clock","weather"],"density":"comfortable"}"#,
        )?;
        self.emit("dashboard", 200, false);
        Ok(Dashboard {
            identity_label: identity.email.unwrap_or(identity.subject),
            alarms,
            preferences,
        })
    }

    async fn introspect(&self, token: &str) -> Result<Identity, WebError> {
        let secret = self
            .config
            .introspect_secret
            .as_deref()
            .ok_or(WebError::AuthUnavailable)?;
        let response = self.http.post(format!("{}/auth/introspect", self.config.shared_auth_base.trim_end_matches('/')))
            .bearer_auth(secret)
            .json(&json!({"contract":"IntrospectionRequest","payload":{"token":token,"audience":self.config.shared_auth_audience,"requiredScopes":[]}}))
            .send().await.map_err(|_| WebError::AuthUnavailable)?;
        if !response.status().is_success() {
            self.emit("shared_auth.introspect", response.status().as_u16(), true);
            return Err(WebError::AuthUnavailable);
        }
        let result: Introspection = response
            .json()
            .await
            .map_err(|_| WebError::AuthUnavailable)?;
        if !result.active || result.sub.is_empty() {
            self.emit("shared_auth.introspect", 401, true);
            return Err(WebError::Unauthorized);
        }
        self.emit("shared_auth.introspect", 200, false);
        Ok(Identity {
            subject: result.sub,
            email: result.email,
            roles: result.roles,
        })
    }

    fn emit(&self, operation: &str, status: u16, failed: bool) {
        let mut fields = Map::new();
        fields.insert("operation".into(), json!(operation));
        fields.insert("status".into(), json!(status));
        fields.insert("transport".into(), json!(self.gateway.mode().as_str()));
        let event = if failed {
            self.telemetry.error(vec![json!("happy_wakey.web.request")])
        } else {
            self.telemetry.info(vec![json!("happy_wakey.web.request")])
        };
        let _ = event
            .add_fields(fields)
            .add_tags(["happy-wakey", "web"])
            .send();
    }
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| {
            !token.is_empty()
                && token.len() <= 16 * 1024
                && !token.chars().any(char::is_whitespace)
                && !token.chars().any(char::is_control)
        })
}

pub fn merge_preferences(base: &str, incoming: &str) -> Result<Value, WebError> {
    let merged = merge_json(base, incoming, &MergeOptions::default())
        .map_err(|error| WebError::Contract(error.to_string()))?;
    serde_json::from_str(&merged).map_err(|error| WebError::Contract(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn opto_sync_owns_preference_merge() {
        let merged = merge_preferences(
            r#"{"theme":"dark","nested":{"a":1}}"#,
            r#"{"nested":{"b":2}}"#,
        )
        .unwrap();
        assert_eq!(merged, json!({"theme":"dark","nested":{"a":1,"b":2}}));
    }
    #[test]
    fn bearer_is_strict() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic nope".parse().unwrap());
        assert!(bearer(&headers).is_none());
        headers.insert("authorization", "Bearer token".parse().unwrap());
        assert_eq!(bearer(&headers), Some("token"));
    }
}
