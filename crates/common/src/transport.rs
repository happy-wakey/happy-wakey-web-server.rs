use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_nats::{
    jetstream::{self, message::PublishMessage, stream::Stream},
    ConnectOptions,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use happy_wakey_interfaces::{
    Alarm, AsyncOperationAccepted, AsyncOperationRequest, AsyncOperationSignal, ServiceOperation,
    ServiceOperationRequest, ServiceOperationResponse, ServiceOperationStatus,
};
use happy_wakey_lib_core::{DatabaseFlavor, ReadContext};
use tokio::{
    net::TcpStream,
    sync::Mutex,
    time::{sleep, timeout, Instant},
};
use tokio_rustls::{
    client::TlsStream,
    rustls::{self, pki_types::ServerName},
    TlsConnector,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

use crate::{Config, Identity, WebError};

const SERVICE_REQUEST_SCHEMA: &str = "happy-wakey.service-operation.request.v1";
const SERVICE_RESPONSE_SCHEMA: &str = "happy-wakey.service-operation.response.v1";
const ASYNC_REQUEST_SCHEMA: &str = "happy-wakey.async-operation.request.v1";
const ASYNC_ACCEPTED_SCHEMA: &str = "happy-wakey.async-operation.accepted.v1";
const ASYNC_SIGNAL_SCHEMA: &str = "happy-wakey.async-operation.signal.v1";
const REQUEST_SUBJECT: &str = "happy-wakey.operations";
const RESPONSE_SUBJECT_PREFIX: &str = "happy-wakey.responses";
const MAX_REQUEST_BYTES: usize = 32 * 1024;
const MAX_RESPONSE_BYTES: usize = 900 * 1024;
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportMode {
    DirectDb,
    Http,
    Tcp,
    Nats,
}

impl TransportMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "direct-db" | "direct_db" => Ok(Self::DirectDb),
            "http" | "https" => Ok(Self::Http),
            "tcp" | "tls" => Ok(Self::Tcp),
            "nats" | "jetstream" | "mq" => Ok(Self::Nats),
            _ => {
                anyhow::bail!("HAPPY_WAKEY_WEB_API_TRANSPORT must be direct-db, http, tcp, or nats")
            }
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirectDb => "direct-db",
            Self::Http => "http",
            Self::Tcp => "tcp",
            Self::Nats => "nats",
        }
    }
}

pub struct ServiceGateway {
    mode: TransportMode,
    direct: Option<ReadContext>,
    tcp: Option<TcpLane>,
    nats: Option<NatsLane>,
}

impl ServiceGateway {
    pub async fn connect(config: &Config) -> Result<Self> {
        let mut direct = None;
        let mut tcp = None;
        let mut nats = None;
        match config.transport_mode {
            TransportMode::DirectDb => {
                let database_url = config
                    .database_url
                    .as_deref()
                    .context("DATABASE_URL is required for direct-db transport")?;
                direct = Some(
                    ReadContext::connect(
                        database_url,
                        config.database_flavor,
                        config.database_max_connections,
                    )
                    .await
                    .context("connect read-only direct database lane")?,
                );
            }
            TransportMode::Http => {}
            TransportMode::Tcp => tcp = Some(TcpLane::new(config)?),
            TransportMode::Nats => nats = Some(NatsLane::connect(config).await?),
        }
        Ok(Self {
            mode: config.transport_mode,
            direct,
            tcp,
            nats,
        })
    }

    pub const fn mode(&self) -> TransportMode {
        self.mode
    }

    pub async fn list_alarms(
        &self,
        http: &reqwest::Client,
        config: &Config,
        token: &str,
        identity: &Identity,
    ) -> Result<Vec<Alarm>, WebError> {
        match self.mode {
            TransportMode::DirectDb => self
                .direct
                .as_ref()
                .ok_or(WebError::ApiUnavailable)?
                .alarms_for_subject(&identity.subject)
                .await
                .map_err(|_| WebError::ApiUnavailable),
            TransportMode::Http => fetch_http(http, config, token).await,
            TransportMode::Tcp => {
                self.tcp
                    .as_ref()
                    .ok_or(WebError::ApiUnavailable)?
                    .list_alarms(token)
                    .await
            }
            TransportMode::Nats => {
                self.nats
                    .as_ref()
                    .ok_or(WebError::ApiUnavailable)?
                    .list_alarms(http, config, token)
                    .await
            }
        }
    }
}

async fn fetch_http(
    http: &reqwest::Client,
    config: &Config,
    token: &str,
) -> Result<Vec<Alarm>, WebError> {
    let response = http
        .get(format!(
            "{}/v1/alarms",
            config.api_base.trim_end_matches('/')
        ))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|_| WebError::ApiUnavailable)?;
    let status = response.status();
    if !status.is_success() {
        return Err(if status == reqwest::StatusCode::UNAUTHORIZED {
            WebError::Unauthorized
        } else {
            WebError::ApiUnavailable
        });
    }
    response
        .json()
        .await
        .map_err(|_| WebError::Contract("alarm response violated happy-wakey-interfaces".into()))
}

struct TcpLane {
    address: String,
    server_name: String,
    connector: TlsConnector,
    connection: Mutex<Option<TcpFramed>>,
}

type TcpFramed = Framed<TlsStream<TcpStream>, LengthDelimitedCodec>;

impl TcpLane {
    fn new(config: &Config) -> Result<Self> {
        let address = config
            .tcp_address
            .clone()
            .context("HAPPY_WAKEY_API_TCP_ADDR is required for tcp transport")?;
        let server_name = config
            .tcp_server_name
            .clone()
            .context("HAPPY_WAKEY_API_TCP_SERVER_NAME is required for tcp transport")?;
        ServerName::try_from(server_name.clone()).context("invalid API TLS server name")?;
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self {
            address,
            server_name,
            connector: TlsConnector::from(Arc::new(tls)),
            connection: Mutex::new(None),
        })
    }

    async fn list_alarms(&self, token: &str) -> Result<Vec<Alarm>, WebError> {
        let operation_id = Uuid::new_v4().to_string();
        let request = ServiceOperationRequest {
            schema: SERVICE_REQUEST_SCHEMA.into(),
            operation_id: operation_id.clone(),
            bearer_token: token.into(),
            operation: ServiceOperation::ListAlarms,
        };
        let payload = serde_json::to_vec(&request)
            .map_err(|_| WebError::Contract("service request could not serialize".into()))?;
        if payload.len() > MAX_REQUEST_BYTES {
            return Err(WebError::Contract(
                "service request exceeded its byte limit".into(),
            ));
        }

        let mut connection = self.connection.lock().await;
        for attempt in 0..2 {
            if connection.is_none() {
                *connection = Some(self.connect().await.map_err(|_| WebError::ApiUnavailable)?);
            }
            let framed = connection.as_mut().expect("connection was initialized");
            let exchange = async {
                framed
                    .send(Bytes::from(payload.clone()))
                    .await
                    .context("send persistent TLS operation")?;
                framed
                    .next()
                    .await
                    .context("persistent TLS connection closed")?
                    .context("read persistent TLS response")
            };
            match timeout(EXCHANGE_TIMEOUT, exchange).await {
                Ok(Ok(frame)) => return decode_response(&operation_id, &frame),
                _ => {
                    *connection = None;
                    if attempt == 1 {
                        return Err(WebError::ApiUnavailable);
                    }
                }
            }
        }
        Err(WebError::ApiUnavailable)
    }

    async fn connect(&self) -> Result<TcpFramed> {
        let socket = timeout(EXCHANGE_TIMEOUT, TcpStream::connect(&self.address))
            .await
            .context("persistent TLS connect timed out")?
            .context("connect persistent TLS socket")?;
        socket
            .set_nodelay(true)
            .context("configure persistent TLS socket")?;
        let server_name = ServerName::try_from(self.server_name.clone())
            .context("invalid API TLS server name")?;
        let stream = timeout(
            EXCHANGE_TIMEOUT,
            self.connector.connect(server_name, socket),
        )
        .await
        .context("persistent TLS handshake timed out")?
        .context("complete persistent TLS handshake")?;
        Ok(LengthDelimitedCodec::builder()
            .length_field_length(4)
            .max_frame_length(MAX_RESPONSE_BYTES)
            .new_framed(stream))
    }
}

struct NatsLane {
    context: jetstream::Context,
    response_stream: Stream,
    response_timeout: Duration,
}

impl NatsLane {
    async fn connect(config: &Config) -> Result<Self> {
        let url = config
            .nats_url
            .as_deref()
            .context("HAPPY_WAKEY_NATS_URL is required for nats transport")?;
        anyhow::ensure!(url.starts_with("tls://"), "NATS URL must use tls://");
        let authority = url
            .strip_prefix("tls://")
            .unwrap_or_default()
            .split('/')
            .next()
            .unwrap_or_default();
        anyhow::ensure!(
            !authority.is_empty() && !authority.contains('@'),
            "NATS credentials must come from the credentials file"
        );
        let credentials = config
            .nats_credentials_path
            .as_ref()
            .context("HAPPY_WAKEY_NATS_CREDENTIALS_FILE is required for nats transport")?;
        let options = ConnectOptions::with_credentials_file(credentials)
            .await
            .context("load NATS credentials")?
            .require_tls(true)
            .name("happy-wakey-web-server")
            .connection_timeout(Duration::from_secs(5));
        let client = options
            .connect(url)
            .await
            .context("connect web server to NATS over TLS")?;
        let context = jetstream::new(client);
        let response_stream = context
            .get_stream(&config.nats_response_stream)
            .await
            .context("get pre-provisioned response stream")?;
        let stream_config = &response_stream.cached_info().config;
        anyhow::ensure!(
            stream_config.allow_direct,
            "response stream must allow direct reads"
        );
        anyhow::ensure!(
            stream_config
                .subjects
                .iter()
                .any(|subject| subject == &format!("{RESPONSE_SUBJECT_PREFIX}.*")),
            "response stream does not own canonical response subjects"
        );
        Ok(Self {
            context,
            response_stream,
            response_timeout: config.nats_response_timeout,
        })
    }

    async fn list_alarms(
        &self,
        http: &reqwest::Client,
        config: &Config,
        token: &str,
    ) -> Result<Vec<Alarm>, WebError> {
        let operation_id = Uuid::new_v4().to_string();
        let registration = AsyncOperationRequest {
            schema: ASYNC_REQUEST_SCHEMA.into(),
            operation_id: operation_id.clone(),
            operation: ServiceOperation::ListAlarms,
        };
        let response = http
            .post(format!(
                "{}/v1/operations",
                config.api_base.trim_end_matches('/')
            ))
            .bearer_auth(token)
            .json(&registration)
            .send()
            .await
            .map_err(|_| WebError::ApiUnavailable)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(WebError::Unauthorized);
        }
        if response.status() != reqwest::StatusCode::ACCEPTED {
            return Err(WebError::ApiUnavailable);
        }
        let accepted: AsyncOperationAccepted = response
            .json()
            .await
            .map_err(|_| WebError::Contract("async acceptance response was malformed".into()))?;
        let expected_subject = format!("{RESPONSE_SUBJECT_PREFIX}.{operation_id}");
        if accepted.schema != ASYNC_ACCEPTED_SCHEMA
            || accepted.operation_id != operation_id
            || accepted.response_subject != expected_subject
        {
            return Err(WebError::Contract(
                "async acceptance response violated the interfaces contract".into(),
            ));
        }

        let signal = AsyncOperationSignal {
            schema: ASYNC_SIGNAL_SCHEMA.into(),
            operation_id: operation_id.clone(),
        };
        let signal = serde_json::to_vec(&signal)
            .map_err(|_| WebError::Contract("async signal could not serialize".into()))?;
        self.context
            .send_publish(
                REQUEST_SUBJECT,
                PublishMessage::build()
                    .payload(Bytes::from(signal))
                    .message_id(format!("happy-wakey-request-{operation_id}")),
            )
            .await
            .map_err(|_| WebError::ApiUnavailable)?
            .await
            .map_err(|_| WebError::ApiUnavailable)?;

        let deadline = Instant::now() + self.response_timeout;
        loop {
            if let Ok(message) = self
                .response_stream
                .get_last_raw_message_by_subject(&expected_subject)
                .await
            {
                return decode_response(&operation_id, &message.payload);
            }
            if Instant::now() >= deadline {
                return Err(WebError::ApiUnavailable);
            }
            sleep(Duration::from_millis(100)).await;
        }
    }
}

fn decode_response(operation_id: &str, payload: &[u8]) -> Result<Vec<Alarm>, WebError> {
    if payload.len() > MAX_RESPONSE_BYTES {
        return Err(WebError::Contract(
            "service response exceeded its byte limit".into(),
        ));
    }
    let response: ServiceOperationResponse = serde_json::from_slice(payload)
        .map_err(|_| WebError::Contract("service response was malformed".into()))?;
    if response.schema != SERVICE_RESPONSE_SCHEMA || response.operation_id != operation_id {
        return Err(WebError::Contract(
            "service response correlation was invalid".into(),
        ));
    }
    match response.status {
        ServiceOperationStatus::Ok if response.error.is_none() => Ok(response.alarms),
        ServiceOperationStatus::Unauthorized => Err(WebError::Unauthorized),
        ServiceOperationStatus::Unavailable | ServiceOperationStatus::Invalid => {
            Err(WebError::ApiUnavailable)
        }
        ServiceOperationStatus::Ok => Err(WebError::Contract(
            "successful service response contained an error".into(),
        )),
    }
}

pub fn database_flavor(value: &str) -> Result<DatabaseFlavor> {
    match value.trim().to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" => Ok(DatabaseFlavor::PostgreSql),
        "cockroach" | "cockroachdb" => Ok(DatabaseFlavor::CockroachDb),
        _ => anyhow::bail!("HAPPY_WAKEY_DATABASE_FLAVOR must be postgresql or cockroachdb"),
    }
}

pub fn credentials_path(value: Option<String>) -> Option<PathBuf> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_mode_is_explicit_and_total() {
        assert_eq!(
            TransportMode::parse("direct-db").unwrap(),
            TransportMode::DirectDb
        );
        assert_eq!(TransportMode::parse("https").unwrap(), TransportMode::Http);
        assert_eq!(TransportMode::parse("tls").unwrap(), TransportMode::Tcp);
        assert_eq!(
            TransportMode::parse("jetstream").unwrap(),
            TransportMode::Nats
        );
        assert!(TransportMode::parse("auto").is_err());
    }

    #[test]
    fn service_response_requires_exact_correlation() {
        let response = ServiceOperationResponse {
            schema: SERVICE_RESPONSE_SCHEMA.into(),
            operation_id: Uuid::new_v4().to_string(),
            status: ServiceOperationStatus::Ok,
            alarms: Vec::new(),
            error: None,
        };
        let bytes = serde_json::to_vec(&response).unwrap();
        assert!(decode_response(&response.operation_id, &bytes).is_ok());
        assert!(decode_response(&Uuid::new_v4().to_string(), &bytes).is_err());
    }

    #[test]
    fn durable_signal_contains_no_credential_or_owner() {
        let signal = AsyncOperationSignal {
            schema: ASYNC_SIGNAL_SCHEMA.into(),
            operation_id: Uuid::new_v4().to_string(),
        };
        let value = serde_json::to_value(signal).unwrap();
        assert!(value.get("bearer_token").is_none());
        assert!(value.get("owner_id").is_none());
    }
}
