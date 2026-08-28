#![forbid(unsafe_code)]

use std::{
    env,
    io::BufReader,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use async_nats::{
    jetstream::{
        self,
        message::PublishMessage,
        stream::{RetentionPolicy, StorageType, Stream},
    },
    ConnectOptions,
};
use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
pub use happy_wakey_interfaces::Alarm;
use happy_wakey_interfaces::{
    ApiError, AsyncOperationAccepted, AsyncOperationRequest, AsyncOperationSignal,
    ServiceOperation, ServiceOperationRequest, ServiceOperationResponse, ServiceOperationStatus,
};
use happy_wakey_lib_core::{DatabaseFlavor, ReadContext};
use next_loggers::{json, Logger, Map, Options};
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use syncer_rs::{merge_json, MergeOptions};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, OnceCell},
    time::{sleep, timeout},
};
use tokio_rustls::{
    client::TlsStream,
    rustls::{self, pki_types::ServerName},
    TlsConnector,
};
use uuid::Uuid;

const SERVICE_REQUEST_SCHEMA: &str = "happy-wakey.service-operation.request.v1";
const SERVICE_RESPONSE_SCHEMA: &str = "happy-wakey.service-operation.response.v1";
const ASYNC_REQUEST_SCHEMA: &str = "happy-wakey.async-operation.request.v1";
const ASYNC_ACCEPTED_SCHEMA: &str = "happy-wakey.async-operation.accepted.v1";
const ASYNC_SIGNAL_SCHEMA: &str = "happy-wakey.async-operation.signal.v1";
const ASYNC_REQUEST_SUBJECT: &str = "dd.remote.web_api.happy-wakey.request";
const ASYNC_RESPONSE_PREFIX: &str = "happy-wakey.responses";
const MAX_RESPONSE_BYTES: usize = 900 * 1024;
const MAX_INTROSPECTION_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BYTES: usize = 32 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const ASYNC_TIMEOUT: Duration = Duration::from_secs(15);

type TcpChannel = TlsStream<TcpStream>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractionMode {
    DirectDatabaseRead,
    StatelessHttps,
    StatefulTls,
    AsyncJetStream,
}

impl InteractionMode {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "direct_db_read" => Ok(Self::DirectDatabaseRead),
            "stateless_https" => Ok(Self::StatelessHttps),
            "stateful_tls" => Ok(Self::StatefulTls),
            "async_jetstream" => Ok(Self::AsyncJetStream),
            _ => anyhow::bail!("HAPPY_WAKEY_INTERACTION_MODE must name one supported mode"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::DirectDatabaseRead => "direct_db_read",
            Self::StatelessHttps => "stateless_https",
            Self::StatefulTls => "stateful_tls",
            Self::AsyncJetStream => "async_jetstream",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub interaction_mode: String,
    pub api_base: String,
    pub shared_auth_base: String,
    pub shared_auth_audience: String,
    pub introspect_secret: Option<String>,
    pub database_url: Option<String>,
    pub database_flavor: String,
    pub database_max_connections: u32,
    pub tcp_address: Option<String>,
    pub tcp_server_name: Option<String>,
    pub tcp_ca_file: Option<PathBuf>,
    pub nats_url: Option<String>,
    pub nats_credentials_file: Option<PathBuf>,
    pub nats_request_stream: String,
    pub nats_response_stream: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            interaction_mode: env::var("HAPPY_WAKEY_INTERACTION_MODE")
                .unwrap_or_else(|_| "stateless_https".into()),
            api_base: env::var("HAPPY_WAKEY_API_BASE")
                .unwrap_or_else(|_| "https://api.happy-wakey.dev".into()),
            shared_auth_base: env::var("HAPPY_WAKEY_SHARED_AUTH_BASE")
                .unwrap_or_else(|_| "https://auth.oresoftware.dev".into()),
            shared_auth_audience: env::var("HAPPY_WAKEY_SHARED_AUTH_AUDIENCE")
                .unwrap_or_else(|_| "happy-wakey".into()),
            introspect_secret: optional_env("HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET"),
            database_url: optional_env("DATABASE_URL"),
            database_flavor: env::var("HAPPY_WAKEY_DATABASE_FLAVOR")
                .unwrap_or_else(|_| "postgres".into()),
            database_max_connections: env::var("HAPPY_WAKEY_DATABASE_MAX_CONNECTIONS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(4),
            tcp_address: optional_env("HAPPY_WAKEY_API_TCP_ADDRESS"),
            tcp_server_name: optional_env("HAPPY_WAKEY_API_TCP_SERVER_NAME"),
            tcp_ca_file: optional_env("HAPPY_WAKEY_API_TCP_CA_FILE").map(PathBuf::from),
            nats_url: optional_env("HAPPY_WAKEY_NATS_URL"),
            nats_credentials_file: optional_env("HAPPY_WAKEY_NATS_CREDENTIALS_FILE")
                .map(PathBuf::from),
            nats_request_stream: env::var("HAPPY_WAKEY_NATS_REQUEST_STREAM")
                .unwrap_or_else(|_| "DD_WEB_API_REQUESTS".into()),
            nats_response_stream: env::var("HAPPY_WAKEY_NATS_RESPONSE_STREAM")
                .unwrap_or_else(|_| "HAPPY_WAKEY_RESPONSES".into()),
        }
    }
}

pub struct Runtime {
    config: Config,
    mode: InteractionMode,
    shared_auth_secret: String,
    http: reqwest::Client,
    telemetry: Arc<Logger>,
    direct: OnceCell<ReadContext>,
    tcp: Mutex<Option<TcpChannel>>,
    nats: OnceCell<NatsTransport>,
}

struct NatsTransport {
    context: jetstream::Context,
    response_stream: Stream,
}

#[derive(Clone, Debug)]
pub struct Identity {
    pub subject: String,
    pub email: Option<String>,
    pub roles: Vec<String>,
}

#[derive(Serialize)]
struct IntrospectionEnvelope<'a> {
    contract: &'static str,
    payload: IntrospectionRequest<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IntrospectionRequest<'a> {
    token: &'a str,
    audience: &'a str,
    required_scopes: [String; 0],
}

#[derive(Deserialize)]
struct IntrospectionResponse {
    active: bool,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    roles: Vec<String>,
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
                "Happy Wakey data is unavailable",
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
    pub fn new(config: Config, lane: &str) -> Result<Self> {
        let mode = InteractionMode::parse(&config.interaction_mode)?;
        validate_config(&config, mode)?;
        let service_secret = config
            .introspect_secret
            .clone()
            .context("HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET is required")?;
        let http = reqwest::Client::builder()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(IO_TIMEOUT)
            .build()
            .context("build bounded HTTP client")?;
        Ok(Self {
            config,
            mode,
            shared_auth_secret: service_secret,
            http,
            telemetry: Arc::new(Logger::new(Options {
                app_name: format!("happy-wakey-{lane}"),
                ..Options::default()
            })),
            direct: OnceCell::new(),
            tcp: Mutex::new(None),
            nats: OnceCell::new(),
        })
    }

    pub async fn dashboard(&self, headers: &HeaderMap) -> Result<Dashboard, WebError> {
        let token = bearer(headers).ok_or(WebError::Unauthorized)?;
        let identity = self.introspect(token).await?;
        let alarms = match self.mode {
            InteractionMode::DirectDatabaseRead => self.fetch_direct(&identity.subject).await,
            InteractionMode::StatelessHttps => self.fetch_http(token).await,
            InteractionMode::StatefulTls => self.fetch_tcp(token).await,
            InteractionMode::AsyncJetStream => self.fetch_async(token).await,
        }?;
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
        let response = self
            .http
            .post(format!(
                "{}/auth/introspect",
                self.config.shared_auth_base.trim_end_matches('/')
            ))
            .bearer_auth(&self.shared_auth_secret)
            .json(&IntrospectionEnvelope {
                contract: "IntrospectionRequest",
                payload: IntrospectionRequest {
                    token,
                    audience: &self.config.shared_auth_audience,
                    required_scopes: [],
                },
            })
            .send()
            .await
            .map_err(|_| {
                self.emit("shared_auth.introspect", 503, true);
                WebError::AuthUnavailable
            })?;
        if !response.status().is_success() {
            let failure = if response.status() == StatusCode::UNAUTHORIZED {
                WebError::Unauthorized
            } else {
                WebError::AuthUnavailable
            };
            self.emit("shared_auth.introspect", failure_status(&failure), true);
            return Err(failure);
        }
        let body = bounded_auth_body(response).await.inspect_err(|failure| {
            self.emit("shared_auth.introspect", failure_status(failure), true);
        })?;
        let result: IntrospectionResponse = serde_json::from_slice(&body).map_err(|_| {
            self.emit("shared_auth.introspect", 503, true);
            WebError::AuthUnavailable
        })?;
        if !result.active {
            self.emit("shared_auth.introspect", 401, true);
            return Err(WebError::Unauthorized);
        }
        let subject = result
            .sub
            .filter(|value| {
                !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_whitespace)
            })
            .ok_or(WebError::Unauthorized)?;
        self.emit("shared_auth.introspect", 200, false);
        Ok(Identity {
            subject,
            email: result.email,
            roles: result.roles,
        })
    }

    async fn fetch_direct(&self, subject: &str) -> Result<Vec<Alarm>, WebError> {
        let database_url = self
            .config
            .database_url
            .as_deref()
            .ok_or(WebError::ApiUnavailable)?;
        let flavor = match self.config.database_flavor.as_str() {
            "postgres" => DatabaseFlavor::PostgreSql,
            "cockroach" => DatabaseFlavor::CockroachDb,
            _ => return Err(WebError::ApiUnavailable),
        };
        let max_connections = self.config.database_max_connections;
        let context = self
            .direct
            .get_or_try_init(|| async {
                ReadContext::connect(database_url, flavor, max_connections)
                    .await
                    .map_err(|_| WebError::ApiUnavailable)
            })
            .await?;
        let alarms = context
            .alarms_for_subject(subject)
            .await
            .map_err(|_| WebError::ApiUnavailable)?;
        self.emit("alarms.list", 200, false);
        Ok(alarms)
    }

    async fn fetch_http(&self, token: &str) -> Result<Vec<Alarm>, WebError> {
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
            self.emit("alarms.list", status.as_u16(), true);
            return Err(if status == StatusCode::UNAUTHORIZED {
                WebError::Unauthorized
            } else {
                WebError::ApiUnavailable
            });
        }
        let bytes = bounded_body(response).await?;
        let alarms = serde_json::from_slice(&bytes).map_err(|_| {
            WebError::Contract("alarm response violated happy-wakey-interfaces".into())
        })?;
        self.emit("alarms.list", 200, false);
        Ok(alarms)
    }

    async fn fetch_tcp(&self, token: &str) -> Result<Vec<Alarm>, WebError> {
        let operation_id = Uuid::new_v4();
        let payload = serde_json::to_vec(&service_request(operation_id, token))
            .map_err(|_| WebError::Contract("service request could not serialize".into()))?;
        if payload.len() > MAX_REQUEST_BYTES {
            return Err(WebError::Contract("service request was too large".into()));
        }
        for attempt in 0..2 {
            let mut channel = self.tcp.lock().await;
            if channel.is_none() {
                *channel = Some(self.connect_tcp().await?);
            }
            match tcp_exchange(channel.as_mut().expect("channel initialized"), &payload).await {
                Ok(bytes) => {
                    let alarms = decode_service_response(&bytes, operation_id)?;
                    self.emit("alarms.list", 200, false);
                    return Ok(alarms);
                }
                Err(_) => {
                    *channel = None;
                    if attempt == 1 {
                        self.emit("alarms.list", 503, true);
                        return Err(WebError::ApiUnavailable);
                    }
                }
            }
        }
        Err(WebError::ApiUnavailable)
    }

    async fn connect_tcp(&self) -> Result<TcpChannel, WebError> {
        let address = self
            .config
            .tcp_address
            .as_deref()
            .ok_or(WebError::ApiUnavailable)?;
        let server_name = self
            .config
            .tcp_server_name
            .as_deref()
            .ok_or(WebError::ApiUnavailable)?;
        let ca_file = self
            .config
            .tcp_ca_file
            .as_ref()
            .ok_or(WebError::ApiUnavailable)?;
        let ca_bytes = tokio::fs::read(ca_file)
            .await
            .map_err(|_| WebError::ApiUnavailable)?;
        let mut reader = BufReader::new(ca_bytes.as_slice());
        let certificates = rustls_pemfile::certs(&mut reader)
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|_| WebError::ApiUnavailable)?;
        if certificates.is_empty() {
            return Err(WebError::ApiUnavailable);
        }
        let mut roots = rustls::RootCertStore::empty();
        for certificate in certificates {
            roots
                .add(certificate)
                .map_err(|_| WebError::ApiUnavailable)?;
        }
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tcp = timeout(Duration::from_secs(5), TcpStream::connect(address))
            .await
            .map_err(|_| WebError::ApiUnavailable)?
            .map_err(|_| WebError::ApiUnavailable)?;
        tcp.set_nodelay(true)
            .map_err(|_| WebError::ApiUnavailable)?;
        let name =
            ServerName::try_from(server_name.to_owned()).map_err(|_| WebError::ApiUnavailable)?;
        let stream = timeout(
            Duration::from_secs(5),
            TlsConnector::from(Arc::new(tls)).connect(name, tcp),
        )
        .await
        .map_err(|_| WebError::ApiUnavailable)?
        .map_err(|_| WebError::ApiUnavailable)?;
        Ok(stream)
    }

    async fn fetch_async(&self, token: &str) -> Result<Vec<Alarm>, WebError> {
        let operation_id = Uuid::new_v4();
        let request = AsyncOperationRequest {
            schema: ASYNC_REQUEST_SCHEMA.into(),
            operation_id: operation_id.to_string(),
            operation: ServiceOperation::ListAlarms,
        };
        let response = self
            .http
            .post(format!(
                "{}/v1/async-operations",
                self.config.api_base.trim_end_matches('/')
            ))
            .bearer_auth(token)
            .json(&request)
            .send()
            .await
            .map_err(|_| WebError::ApiUnavailable)?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(if response.status() == StatusCode::UNAUTHORIZED {
                WebError::Unauthorized
            } else {
                WebError::ApiUnavailable
            });
        }
        let accepted: AsyncOperationAccepted =
            serde_json::from_slice(&bounded_body(response).await?).map_err(|_| {
                WebError::Contract("async acceptance violated the interface".into())
            })?;
        let expected_subject = response_subject(operation_id);
        if accepted.schema != ASYNC_ACCEPTED_SCHEMA
            || accepted.operation_id != operation_id.to_string()
            || accepted.response_subject != expected_subject
        {
            return Err(WebError::Contract(
                "async acceptance did not match the registered operation".into(),
            ));
        }
        let nats = self.nats.get_or_try_init(|| self.connect_nats()).await?;
        let signal = serde_json::to_vec(&AsyncOperationSignal {
            schema: ASYNC_SIGNAL_SCHEMA.into(),
            operation_id: operation_id.to_string(),
        })
        .map_err(|_| WebError::Contract("async signal could not serialize".into()))?;
        nats.context
            .send_publish(
                ASYNC_REQUEST_SUBJECT,
                PublishMessage::build()
                    .payload(Bytes::from(signal))
                    .message_id(format!("happy-wakey-request-{operation_id}")),
            )
            .await
            .map_err(|_| WebError::ApiUnavailable)?
            .await
            .map_err(|_| WebError::ApiUnavailable)?;
        let started = Instant::now();
        loop {
            if let Ok(message) = nats
                .response_stream
                .direct_get_last_for_subject(expected_subject.clone())
                .await
            {
                let alarms = decode_service_response(&message.payload, operation_id)?;
                self.emit("alarms.list", 200, false);
                return Ok(alarms);
            }
            if started.elapsed() >= ASYNC_TIMEOUT {
                self.emit("alarms.list", 504, true);
                return Err(WebError::ApiUnavailable);
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn connect_nats(&self) -> Result<NatsTransport, WebError> {
        let url = self
            .config
            .nats_url
            .as_deref()
            .ok_or(WebError::ApiUnavailable)?;
        let mut options = if let Some(credentials) = self.config.nats_credentials_file.as_ref() {
            ConnectOptions::with_credentials_file(credentials)
                .await
                .map_err(|_| WebError::ApiUnavailable)?
        } else {
            ConnectOptions::new()
        };
        if url.starts_with("tls://") {
            options = options.require_tls(true);
        }
        let options = options
            .name("happy-wakey-web-server")
            .connection_timeout(Duration::from_secs(5))
            .subscription_capacity(128);
        let context = jetstream::new(
            options
                .connect(url)
                .await
                .map_err(|_| WebError::ApiUnavailable)?,
        );
        let request_stream = context
            .get_stream(&self.config.nats_request_stream)
            .await
            .map_err(|_| WebError::ApiUnavailable)?;
        validate_request_stream(request_stream.cached_info())?;
        let response_stream = context
            .get_stream(&self.config.nats_response_stream)
            .await
            .map_err(|_| WebError::ApiUnavailable)?;
        validate_response_stream(response_stream.cached_info())?;
        Ok(NatsTransport {
            context,
            response_stream,
        })
    }

    fn emit(&self, operation: &str, status: u16, failed: bool) {
        let mut fields = Map::new();
        fields.insert("operation".into(), json!(operation));
        fields.insert("status".into(), json!(status));
        fields.insert("mode".into(), json!(self.mode.label()));
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

async fn tcp_exchange(channel: &mut TcpChannel, payload: &[u8]) -> Result<Vec<u8>> {
    anyhow::ensure!(payload.len() <= MAX_REQUEST_BYTES, "request exceeded limit");
    let length = u32::try_from(payload.len()).context("request length exceeded u32")?;
    timeout(IO_TIMEOUT, async {
        channel.write_all(&length.to_be_bytes()).await?;
        channel.write_all(payload).await?;
        channel.flush().await
    })
    .await
    .context("persistent TLS write timed out")??;

    let mut length = [0_u8; 4];
    timeout(IO_TIMEOUT, channel.read_exact(&mut length))
        .await
        .context("persistent TLS response length timed out")??;
    let length = u32::from_be_bytes(length) as usize;
    anyhow::ensure!(length <= MAX_RESPONSE_BYTES, "response exceeded limit");
    let mut response = vec![0_u8; length];
    timeout(IO_TIMEOUT, channel.read_exact(&mut response))
        .await
        .context("persistent TLS response payload timed out")??;
    Ok(response)
}

async fn bounded_body(response: reqwest::Response) -> Result<Bytes, WebError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(WebError::Contract(
            "response exceeded its byte limit".into(),
        ));
    }
    let mut bytes = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| WebError::ApiUnavailable)?;
        extend_bounded(&mut bytes, &chunk)?;
    }
    Ok(bytes.freeze())
}

async fn bounded_auth_body(response: reqwest::Response) -> Result<Bytes, WebError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_INTROSPECTION_RESPONSE_BYTES as u64)
    {
        return Err(WebError::AuthUnavailable);
    }
    let mut bytes = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| WebError::AuthUnavailable)?;
        let next_len = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or(WebError::AuthUnavailable)?;
        if next_len > MAX_INTROSPECTION_RESPONSE_BYTES {
            return Err(WebError::AuthUnavailable);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes.freeze())
}

fn extend_bounded(buffer: &mut BytesMut, chunk: &[u8]) -> Result<(), WebError> {
    let next_len = buffer
        .len()
        .checked_add(chunk.len())
        .ok_or_else(|| WebError::Contract("response exceeded its byte limit".into()))?;
    if next_len > MAX_RESPONSE_BYTES {
        return Err(WebError::Contract(
            "response exceeded its byte limit".into(),
        ));
    }
    buffer.extend_from_slice(chunk);
    Ok(())
}

fn service_request(operation_id: Uuid, token: &str) -> ServiceOperationRequest {
    ServiceOperationRequest {
        schema: SERVICE_REQUEST_SCHEMA.into(),
        operation_id: operation_id.to_string(),
        bearer_token: token.into(),
        operation: ServiceOperation::ListAlarms,
    }
}

fn decode_service_response(bytes: &[u8], operation_id: Uuid) -> Result<Vec<Alarm>, WebError> {
    if bytes.len() > MAX_RESPONSE_BYTES {
        return Err(WebError::Contract("service response was too large".into()));
    }
    let response: ServiceOperationResponse = serde_json::from_slice(bytes)
        .map_err(|_| WebError::Contract("service response violated the interface".into()))?;
    if response.schema != SERVICE_RESPONSE_SCHEMA
        || response.operation_id != operation_id.to_string()
    {
        return Err(WebError::Contract(
            "service response did not match its request".into(),
        ));
    }
    match response.status {
        ServiceOperationStatus::Ok if response.error.is_none() => Ok(response.alarms),
        ServiceOperationStatus::Unauthorized => Err(WebError::Unauthorized),
        ServiceOperationStatus::Invalid | ServiceOperationStatus::Unavailable => {
            Err(WebError::ApiUnavailable)
        }
        ServiceOperationStatus::Ok => Err(WebError::Contract(
            "successful response unexpectedly contained an error".into(),
        )),
    }
}

fn response_subject(operation_id: Uuid) -> String {
    format!("{ASYNC_RESPONSE_PREFIX}.{operation_id}")
}

fn validate_request_stream(info: &jetstream::stream::Info) -> Result<(), WebError> {
    let config = &info.config;
    if config.storage != StorageType::File
        || config.retention != RetentionPolicy::WorkQueue
        || !config
            .subjects
            .iter()
            .any(|subject| subject == ASYNC_REQUEST_SUBJECT)
        || config.duplicate_window.is_zero()
    {
        return Err(WebError::ApiUnavailable);
    }
    Ok(())
}

fn validate_response_stream(info: &jetstream::stream::Info) -> Result<(), WebError> {
    let config = &info.config;
    if config.storage != StorageType::File
        || config.retention != RetentionPolicy::Limits
        || !config.allow_direct
        || !config
            .subjects
            .iter()
            .any(|subject| subject == &format!("{ASYNC_RESPONSE_PREFIX}.*"))
        || config.duplicate_window.is_zero()
    {
        return Err(WebError::ApiUnavailable);
    }
    Ok(())
}

fn validate_config(config: &Config, mode: InteractionMode) -> Result<()> {
    validate_shared_auth_base(&config.shared_auth_base)?;
    anyhow::ensure!(
        config
            .introspect_secret
            .as_ref()
            .is_some_and(|secret| !secret.is_empty() && secret.len() <= 16 * 1024),
        "Shared Auth service credential is required"
    );
    anyhow::ensure!(
        !config.shared_auth_audience.is_empty()
            && config.shared_auth_audience.len() <= 256
            && !config.shared_auth_audience.chars().any(char::is_whitespace),
        "Shared Auth audience is invalid"
    );
    anyhow::ensure!(
        (1..=16).contains(&config.database_max_connections),
        "database connection limit must be between 1 and 16"
    );
    match mode {
        InteractionMode::DirectDatabaseRead => {
            anyhow::ensure!(config.database_url.is_some(), "DATABASE_URL is required");
            anyhow::ensure!(
                matches!(config.database_flavor.as_str(), "postgres" | "cockroach"),
                "database flavor must be postgres or cockroach"
            );
        }
        InteractionMode::StatelessHttps => validate_api_base(&config.api_base)?,
        InteractionMode::StatefulTls => {
            anyhow::ensure!(config.tcp_address.is_some(), "TCP address is required");
            anyhow::ensure!(
                config.tcp_server_name.is_some(),
                "TCP server name is required"
            );
            anyhow::ensure!(config.tcp_ca_file.is_some(), "TCP CA file is required");
        }
        InteractionMode::AsyncJetStream => {
            validate_api_base(&config.api_base)?;
            let url = config.nats_url.as_deref().context("NATS URL is required")?;
            let in_cluster = url == "nats://dd-nats.messaging.svc.cluster.local:4222";
            anyhow::ensure!(
                url.starts_with("tls://") || in_cluster,
                "NATS must use tls:// or the in-cluster dd-nats URL"
            );
            if in_cluster {
                let authority = url
                    .strip_prefix("nats://")
                    .unwrap_or_default()
                    .split('/')
                    .next()
                    .unwrap_or_default();
                anyhow::ensure!(
                    !authority.contains('@'),
                    "NATS credentials must come from a credentials file"
                );
            } else {
                let authority = url
                    .strip_prefix("tls://")
                    .unwrap_or_default()
                    .split('/')
                    .next()
                    .unwrap_or_default();
                anyhow::ensure!(
                    !authority.is_empty() && !authority.contains('@'),
                    "NATS credentials must come from a credentials file"
                );
                anyhow::ensure!(
                    config.nats_credentials_file.is_some(),
                    "NATS credentials file is required"
                );
            }
            anyhow::ensure!(
                safe_topology_name(&config.nats_request_stream)
                    && safe_topology_name(&config.nats_response_stream),
                "invalid NATS stream name"
            );
        }
    }
    Ok(())
}

fn validate_shared_auth_base(base: &str) -> Result<()> {
    let url = reqwest::Url::parse(base).context("Shared Auth base must be an absolute URL")?;
    anyhow::ensure!(url.scheme() == "https", "Shared Auth base must use HTTPS");
    anyhow::ensure!(
        url.host_str().is_some() && url.username().is_empty() && url.password().is_none(),
        "Shared Auth base must not contain credentials"
    );
    anyhow::ensure!(
        matches!(url.path(), "" | "/") && url.query().is_none() && url.fragment().is_none(),
        "Shared Auth base must not contain a path, query, or fragment"
    );
    Ok(())
}

fn validate_api_base(base: &str) -> Result<()> {
    let url = reqwest::Url::parse(base).context("API base must be an absolute URL")?;
    anyhow::ensure!(url.scheme() == "https", "API base must use HTTPS");
    anyhow::ensure!(
        url.host_str().is_some() && url.username().is_empty() && url.password().is_none(),
        "API base must not contain credentials"
    );
    anyhow::ensure!(
        matches!(url.path(), "" | "/") && url.query().is_none() && url.fragment().is_none(),
        "API base must not contain a path, query, or fragment"
    );
    Ok(())
}

fn safe_topology_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn failure_status(error: &WebError) -> u16 {
    match error {
        WebError::Unauthorized => 401,
        WebError::AuthUnavailable | WebError::ApiUnavailable => 503,
        WebError::Contract(_) => 422,
    }
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub fn bearer(headers: &HeaderMap) -> Option<&str> {
    let token = headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    (!token.is_empty()
        && token.len() <= 16 * 1024
        && !token.chars().any(char::is_whitespace)
        && !token.chars().any(char::is_control))
    .then_some(token)
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
    fn bearer_is_strict_and_bounded() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic nope".parse().unwrap());
        assert!(bearer(&headers).is_none());
        headers.insert("authorization", "Bearer token".parse().unwrap());
        assert_eq!(bearer(&headers), Some("token"));
        headers.insert("authorization", "Bearer bad token".parse().unwrap());
        assert!(bearer(&headers).is_none());
    }

    #[test]
    fn all_interaction_modes_are_explicit() {
        assert_eq!(
            InteractionMode::parse("direct_db_read").unwrap(),
            InteractionMode::DirectDatabaseRead
        );
        assert_eq!(
            InteractionMode::parse("stateless_https").unwrap(),
            InteractionMode::StatelessHttps
        );
        assert_eq!(
            InteractionMode::parse("stateful_tls").unwrap(),
            InteractionMode::StatefulTls
        );
        assert_eq!(
            InteractionMode::parse("async_jetstream").unwrap(),
            InteractionMode::AsyncJetStream
        );
        assert!(InteractionMode::parse("automatic").is_err());
    }

    #[test]
    fn transport_contracts_resist_cross_mode_identity_injection() {
        let operation_id = Uuid::parse_str("018f5cc6-6d8b-7b2a-9f38-269e6a7b1f11").unwrap();
        let tcp = serde_json::to_value(service_request(operation_id, "synthetic.token")).unwrap();
        assert_eq!(tcp["bearer_token"], "synthetic.token");
        assert!(tcp.get("owner_id").is_none());
        let request = serde_json::to_value(AsyncOperationRequest {
            schema: ASYNC_REQUEST_SCHEMA.into(),
            operation_id: operation_id.to_string(),
            operation: ServiceOperation::ListAlarms,
        })
        .unwrap();
        let signal = serde_json::to_value(AsyncOperationSignal {
            schema: ASYNC_SIGNAL_SCHEMA.into(),
            operation_id: operation_id.to_string(),
        })
        .unwrap();
        for durable in [request, signal] {
            assert!(durable.get("bearer_token").is_none());
            assert!(durable.get("owner_id").is_none());
            assert!(!durable.to_string().contains("token"));
        }
    }

    #[test]
    fn service_responses_must_correlate_to_the_request() {
        let operation_id = Uuid::parse_str("018f5cc6-6d8b-7b2a-9f38-269e6a7b1f11").unwrap();
        let response = ServiceOperationResponse {
            schema: SERVICE_RESPONSE_SCHEMA.into(),
            operation_id: Uuid::new_v4().to_string(),
            status: ServiceOperationStatus::Ok,
            alarms: Vec::new(),
            error: None,
        };
        let bytes = serde_json::to_vec(&response).unwrap();
        assert!(matches!(
            decode_service_response(&bytes, operation_id),
            Err(WebError::Contract(_))
        ));
    }

    #[test]
    fn cleartext_and_url_credentials_are_rejected() {
        assert!(validate_shared_auth_base("http://auth.example.test").is_err());
        assert!(validate_shared_auth_base("https://user:pass@auth.example.test").is_err());
        assert!(validate_shared_auth_base("https://auth.example.test/path").is_err());
        assert!(validate_shared_auth_base("https://auth.example.test").is_ok());
        assert!(validate_api_base("http://api.example.test").is_err());
        assert!(validate_api_base("https://user:pass@api.example.test").is_err());
        assert!(validate_api_base("https://api.example.test").is_ok());
        assert!(!safe_topology_name("bad.stream"));
    }

    #[test]
    fn chunked_http_responses_stop_before_exceeding_the_limit() {
        let mut body = BytesMut::from(&b"ok"[..]);
        let oversized_chunk = vec![b'x'; MAX_RESPONSE_BYTES - 1];
        assert!(matches!(
            extend_bounded(&mut body, &oversized_chunk),
            Err(WebError::Contract(_))
        ));
        assert_eq!(&body[..], b"ok");
    }

    #[test]
    fn chunked_http_responses_allow_the_exact_limit() {
        let mut body = BytesMut::new();
        let exact_chunk = vec![b'x'; MAX_RESPONSE_BYTES];
        extend_bounded(&mut body, &exact_chunk).unwrap();
        assert_eq!(body.len(), MAX_RESPONSE_BYTES);
    }
}
