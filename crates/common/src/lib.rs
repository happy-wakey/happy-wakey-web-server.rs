use std::{env, sync::Arc};

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

#[derive(Clone, Debug)]
pub struct Config {
    pub api_base: String,
    pub shared_auth_base: String,
    pub shared_auth_audience: String,
    pub introspect_secret: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            api_base: env::var("HAPPY_WAKEY_API_BASE")
                .unwrap_or_else(|_| "https://api.happy-wakey.dev".into()),
            shared_auth_base: env::var("HAPPY_WAKEY_SHARED_AUTH_BASE")
                .unwrap_or_else(|_| "https://auth.oresoftware.dev".into()),
            shared_auth_audience: env::var("HAPPY_WAKEY_SHARED_AUTH_AUDIENCE")
                .unwrap_or_else(|_| "happy-wakey".into()),
            introspect_secret: env::var("HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET")
                .ok()
                .filter(|value| !value.is_empty()),
        }
    }
}

#[derive(Clone)]
pub struct Runtime {
    config: Config,
    http: reqwest::Client,
    telemetry: Arc<Logger>,
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
    pub fn new(config: Config, lane: &str) -> Result<Self, reqwest::Error> {
        let telemetry = Arc::new(Logger::new(Options {
            app_name: format!("happy-wakey-{lane}"),
            ..Options::default()
        }));
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            config,
            http,
            telemetry,
        })
    }

    pub async fn dashboard(&self, headers: &HeaderMap) -> Result<Dashboard, WebError> {
        let token = bearer(headers).ok_or(WebError::Unauthorized)?;
        let identity = self.introspect(token).await?;
        let alarms = self.fetch_alarms(token).await?;
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

    async fn fetch_alarms(&self, token: &str) -> Result<Vec<Alarm>, WebError> {
        let response = self
            .http
            .get(format!(
                "{}/v1/alarms",
                self.config.api_base.trim_end_matches('/')
            ))
            .bearer_auth(token)
            .send()
            .await
            .map_err(|_| WebError::ApiUnavailable)?;
        let status = response.status();
        if !status.is_success() {
            self.emit("api.list_alarms", status.as_u16(), true);
            return Err(if status == StatusCode::UNAUTHORIZED {
                WebError::Unauthorized
            } else {
                WebError::ApiUnavailable
            });
        }
        let alarms = response.json().await.map_err(|_| {
            WebError::Contract("alarm response violated happy-wakey-interfaces".into())
        })?;
        self.emit("api.list_alarms", 200, false);
        Ok(alarms)
    }

    fn emit(&self, operation: &str, status: u16, failed: bool) {
        let mut fields = Map::new();
        fields.insert("operation".into(), json!(operation));
        fields.insert("status".into(), json!(status));
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

pub fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && token.len() <= 16 * 1024)
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
