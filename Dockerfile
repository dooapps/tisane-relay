# Multi-stage Dockerfile for Cloud Run
FROM rust:1.73 as builder
WORKDIR /usr/src/tisane-relay
COPY . .
RUN cargo install --path . --locked --root /usr/local/cargo

FROM debian:bullseye-slim
# TLS and other dependencies
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/tisane-relay /usr/local/bin/tisane-relay
ENV RUST_LOG=info
EXPOSE 8080
CMD ["/usr/local/bin/tisane-relay"]
