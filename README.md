# Tisane Relay

Agnostic Event Relay for sovereign identities.

## Identity & Validation

Tisane Relay uses the **Infusion Protocol** for identity and validation. Every event pushed to the relay MUST be signed by the author's private key.

### Authentication Format

- **Author Public Key**: Hex-encoded Ed25519 public key (32 bytes -> 64 hex chars).
- **Signature**: Hex-encoded Ed25519 signature (64 bytes -> 128 hex chars).
- **Encoding**: All cryptographic fields are **HEX encoded**.

### Hashing Protocol

The `payload_hash` is calculated by applying **BLAKE3** to the stable string representation of the `payload_json`.

## API: POST /relay/push

### Example Request

```json
[
  {
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "author_pubkey": "d64315263a2c445caf75790784b4e913bc1915223a2c445caf75790784b4e913",
    "signature": "e64a8b7... (128 hex chars)",
    "payload_json": {
      "type": "message",
      "text": "Hello World"
    }
  }
]
```

## Running Tests

Integration tests require a running Postgres instance. Set `DATABASE_URL` and run:

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/db cargo test
```

## Local Development

```bash
cargo build
PORT=8080 DATABASE_URL=... ./target/debug/tisane-relay
```
