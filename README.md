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
HAPPY_WAKEY_SHARED_AUTH_BASE=https://auth.oresoftware.dev
HAPPY_WAKEY_SHARED_AUTH_AUDIENCE=happy-wakey
HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET=<runtime secret>
HAPPY_WAKEY_MASH_PORT=8131
HAPPY_WAKEY_LEPTOS_PORT=8132
HAPPY_WAKEY_DIOXUS_PORT=8133
```

The introspection secret belongs in External Secrets in Kubernetes, never in
Git, images, Worker variables, or browser code.

```sh
RUSTUP_TOOLCHAIN=stable cargo fmt --all -- --check
RUSTUP_TOOLCHAIN=stable cargo clippy --workspace --all-targets -- -D warnings
RUSTUP_TOOLCHAIN=stable cargo test --workspace
```

