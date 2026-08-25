# happy-wakey-web-server.rs

Three independently deployable, server-rendered Rust frontends over one Happy
Wakey service boundary:

| Binary | Stack | Default port |
| --- | --- | --- |
| `happy-wakey-mash` | Maud + Axum + server-driven HTML | 8131 |
| `happy-wakey-leptos` | Leptos 0.8 SSR + Axum | 8132 |
| `happy-wakey-dioxus` | Dioxus 0.7 SSR + Axum | 8133 |

All renderers call `crates/common`; none owns authentication, API contracts,
sync conflict policy, or telemetry independently.

## Four web-to-API avenues

`HAPPY_WAKEY_WEB_API_TRANSPORT` selects one explicit lane. There is no silent
fallback between trust boundaries:

| Value | Behavior |
| --- | --- |
| `direct-db` | Uses `happy-wakey-lib-core::ReadContext`, a read-only SeaORM capability, and binds every query to the already verified Shared Auth subject. |
| `http` | Sends a bounded stateless HTTPS request to the API cluster. This is the default. |
| `tcp` | Reuses a length-delimited, bounded TLS connection to the API cluster. Every operation is independently re-authenticated by the API. |
| `nats` | Registers an authenticated outbox operation over HTTPS, publishes a credential-free JetStream signal, and retrieves the durable correlated response from the response stream. |

All lanes return the same `happy-wakey-interfaces::Alarm` contracts. Direct DB
is intentionally read-only; writes always cross the API authority. The TCP and
NATS paths use per-operation UUID correlation, byte and time limits, and no
token or owner data in telemetry or durable NATS messages.

## Real organization integrations

- **Shared Auth:** the common boundary performs protected
  `POST /auth/introspect` using the current `IntrospectionRequest` envelope.
  Missing service credentials, network failures, inactive tokens, and malformed
  responses fail closed. User tokens are forwarded only to the Happy Wakey API.
- **Opto Sync:** preference overlays call the pinned `syncer.rs` merge engine;
  renderers cannot invent their own JSON merge behavior.
- **Ores OTEL:** every auth/API/render outcome is emitted through the pinned
  Rust SDK from `ores-otel/ores.otel.log`. Tokens and response bodies are never
  log fields.
- **Interfaces:** API responses deserialize into the exact pinned `Alarm`
  contract from `happy-wakey-interfaces`.

The `shared-auth-lib` repository is private and laid out as a polyglot package,
not a root Cargo workspace. This standalone public build therefore uses the
same protected HTTP introspection contract directly. In
`ORESoftware/k8s-cluster`, the service may additionally be compiled against
the private path library without changing this trust boundary.

## Configuration

```text
HAPPY_WAKEY_API_BASE=https://api.happy-wakey.dev
HAPPY_WAKEY_WEB_API_TRANSPORT=http
HAPPY_WAKEY_SHARED_AUTH_BASE=https://auth.oresoftware.dev
HAPPY_WAKEY_SHARED_AUTH_AUDIENCE=happy-wakey
HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET=<runtime secret>
HAPPY_WAKEY_MASH_PORT=8131
HAPPY_WAKEY_LEPTOS_PORT=8132
HAPPY_WAKEY_DIOXUS_PORT=8133
```

Lane-specific configuration:

```text
# direct-db
DATABASE_URL=<runtime secret reference>
HAPPY_WAKEY_DATABASE_FLAVOR=postgresql
HAPPY_WAKEY_WEB_DB_MAX_CONNECTIONS=8

# tcp
HAPPY_WAKEY_API_TCP_ADDR=api-tcp.happy-wakey.svc:8443
HAPPY_WAKEY_API_TCP_SERVER_NAME=api-tcp.happy-wakey.dev

# nats
HAPPY_WAKEY_NATS_URL=tls://nats.happy-wakey.svc:4222
HAPPY_WAKEY_NATS_CREDENTIALS_FILE=/var/run/secrets/nats/web.creds
HAPPY_WAKEY_NATS_RESPONSE_STREAM=HAPPY_WAKEY_RESPONSES
HAPPY_WAKEY_NATS_RESPONSE_TIMEOUT_SECONDS=15
```

The introspection secret belongs in External Secrets in Kubernetes, never in
Git, images, Worker variables, or browser code.

```sh
RUSTUP_TOOLCHAIN=stable cargo fmt --all -- --check
RUSTUP_TOOLCHAIN=stable cargo clippy --workspace --all-targets -- -D warnings
RUSTUP_TOOLCHAIN=stable cargo test --workspace
```
