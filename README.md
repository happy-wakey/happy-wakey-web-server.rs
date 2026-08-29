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

## Real organization integrations

- **Shared Auth:** the common boundary calls the canonical versioned HTTPS
  introspection contract with an independent service credential and the exact
  audience. Redirects are disabled and responses are streamed into a 64 KiB
  bound. Missing credentials, network failures, inactive tokens, and malformed
  responses fail closed. No renderer owns a separate authentication path.
- **Opto Sync:** preference overlays call the pinned `syncer.rs` merge engine;
  renderers cannot invent their own JSON merge behavior.
- **Ores OTEL:** every auth/API/render outcome is emitted through the pinned
  Rust SDK from `ores-otel/ores.otel.log`. Tokens and response bodies are never
  log fields.
- **Interfaces:** API responses deserialize into the exact pinned `Alarm`
  contract from `happy-wakey-interfaces`.

The finalized wire contract is `shared-auth-interfaces` commit
`e60d862a59828a3690852252adcafaea1266268a`. Happy Wakey interfaces are pinned
at `d6278ec8f6b2263678728b147a32dff92d52d8c8`, the read-only core at
`9638429097bc68b2aac280d4e3edaa92db96f85a`, and ores-otel logging at
`ca176fb6768a9750d262a536952268625ffd3a8a`.

## Four interaction modes

Set `HAPPY_WAKEY_INTERACTION_MODE` to exactly one value. There is no automatic
fallback between modes, so an outage cannot silently weaken the selected trust
or durability boundary.

- `direct_db_read` authenticates first, then calls only the subject-scoped
  `happy-wakey-lib-core::ReadContext::alarms_for_subject` capability. The web
  server receives no write or raw-database API.
- `stateless_https` sends a bounded request to the API with redirects disabled.
  Non-HTTPS API bases and URL-embedded credentials are rejected at startup.
- `stateful_tls` maintains a TLS connection with a configured CA and server
  name. Requests and responses are bounded length-delimited frames. Every frame
  carries the current user bearer, so the API re-introspects every operation;
  the connection never caches identity.
- `async_jetstream` first registers an idempotent operation over authenticated
  HTTPS. It then publishes a credential-free signal with a deterministic NATS
  message ID, waits for the JetStream publish acknowledgement, and polls the
  durable response stream by the unique response subject. It validates
  pre-provisioned file-backed stream topology and never substitutes Core NATS.

Bearers appear only in the protected Shared Auth call, stateless HTTPS header,
or transient TLS request frame. They never enter the database outbox,
JetStream payloads, response stream, dead-letter paths, or ores-otel fields.

## Configuration

```text
HAPPY_WAKEY_API_BASE=https://api.happy-wakey.dev
HAPPY_WAKEY_SHARED_AUTH_BASE=https://auth.oresoftware.dev
HAPPY_WAKEY_SHARED_AUTH_AUDIENCE=happy-wakey
HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET=<runtime secret>
HAPPY_WAKEY_INTERACTION_MODE=stateless_https

# direct_db_read
DATABASE_URL=postgres://read-only-runtime-credential@database/happy_wakey
HAPPY_WAKEY_DATABASE_FLAVOR=postgres
HAPPY_WAKEY_DATABASE_MAX_CONNECTIONS=4

# stateful_tls
HAPPY_WAKEY_API_TCP_ADDRESS=api.internal:8443
HAPPY_WAKEY_API_TCP_SERVER_NAME=api.internal
HAPPY_WAKEY_API_TCP_CA_FILE=/var/run/secrets/happy-wakey/ca.pem

# async_jetstream
HAPPY_WAKEY_NATS_URL=nats://dd-nats.messaging.svc.cluster.local:4222
HAPPY_WAKEY_NATS_CREDENTIALS_FILE=/var/run/secrets/happy-wakey/web.creds
HAPPY_WAKEY_NATS_REQUEST_STREAM=DD_WEB_API_REQUESTS
HAPPY_WAKEY_NATS_RESPONSE_STREAM=HAPPY_WAKEY_RESPONSES

HAPPY_WAKEY_MASH_PORT=8131
HAPPY_WAKEY_LEPTOS_PORT=8132
HAPPY_WAKEY_DIOXUS_PORT=8133
```

The introspection secret belongs in External Secrets in Kubernetes, never in
Git, images, Worker variables, or browser code.

```sh
zed validate
zed install --adapter rust
rustup run 1.88.0 cargo fmt --all -- --check
rustup run 1.88.0 cargo clippy --workspace --all-targets --locked -- -D warnings
rustup run 1.88.0 cargo test --workspace --locked
rustup run 1.88.0 cargo build --workspace --release --locked
```
