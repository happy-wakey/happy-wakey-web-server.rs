# syntax=docker/dockerfile:1.7
FROM rust:1.88-bookworm AS builder
ARG FRONTEND=mash
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p "happy-wakey-${FRONTEND}"
RUN cp "target/release/happy-wakey-${FRONTEND}" /tmp/server

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /tmp/server /usr/local/bin/happy-wakey-web
EXPOSE 8131
ENTRYPOINT ["/usr/local/bin/happy-wakey-web"]
