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

## Consumer Payloads

`tisane-relay` transporta payload assinado, mas nao define schema de dominio para consumidores.

Ele e agnostico.
Nao valida regras de negocio da `Mellis`, nao define schema financeiro da `Mellis` e nao deve virar dono desses contratos.

Se um payload financeiro precisar de validacao semantica, essa validacao deve acontecer dentro da `Mellis`.

Regra de ouro do canal agnostico de erro:

- estado esperado de negocio nao entra no canal agnostico
- falha operacional, contratual ou de integracao entra

## Scoped delivery for peers

`tisane-relay` distribui eventos para peers autenticados.
Para leitura seletiva de `signal.error`, o contrato agora e:

- `owner_unit_ref` e a chave de roteamento
- `X-Peer-Token` autentica o peer
- `owner_unit_refs` configurado no peer define o escopo autorizado
- o filtro acontece no servidor, nao no cliente

Implicacoes praticas:

- `GET /relay/errors` exige `X-Peer-Token`
- `GET /relay/owned` exige `X-Peer-Token`
- o peer so recebe `signal.error` cujo `owner_unit_ref` pertence ao seu escopo
- o peer tambem pode ler eventos de negocio com `owner_unit_ref` por `GET /relay/owned`
- a replicacao entre relays tambem filtra `signal.error` por `owner_unit_refs`

Adicionar peer com escopo explicito:

```bash
cargo run -- add-peer \
  --url http://meinn-relay.local:8080 \
  --secret relay-secret \
  --owner-unit-ref meinn.app \
  --database-url "$DATABASE_URL"
```

Leitura recomendada:

- [docs/OPAQUE_PAYLOADS.md](./docs/OPAQUE_PAYLOADS.md)
- [docs/ERROR_SIGNALS.md](./docs/ERROR_SIGNALS.md)

## Running Tests

Integration tests require a running Postgres instance. Set `DATABASE_URL` and run:

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/db cargo test
```

## Local Development

```bash
cargo build
PORT=8080 DATABASE_URL=... ./target/debug/tisane-relay serve --port 8080 --database-url ...
DB_MAX_CONNECTIONS=10 DB_MIN_CONNECTIONS=1 DB_ACQUIRE_TIMEOUT_SECS=5 ./target/debug/tisane-relay serve --port 8080 --database-url ...
```

Server pool tuning:

- `DB_MAX_CONNECTIONS`
- `DB_MIN_CONNECTIONS`
- `DB_ACQUIRE_TIMEOUT_SECS`
- `DB_IDLE_TIMEOUT_SECS`
- `DB_MAX_LIFETIME_SECS`

### Distillery-Only Dev Mode

For local algorithm work without Postgres:

```bash
cargo run -- serve-distillery --port 8080
```

### Manual Smoke Test

For a local end-to-end run with Postgres + relay + Distillery endpoints:

```bash
./scripts/manual_test.sh
```

This exercises:

- `GET /health`
- `POST /distillery/rank`
- `POST /distillery/distribute`

If Docker is unavailable, the script automatically falls back to `serve-distillery`.

### Phase 3 Validation

For a full local validation of the current Postgres path:

```bash
./scripts/test_local.sh
```

For an `EXPLAIN ANALYZE` of the canonical feed aggregation with synthetic load:

```bash
./scripts/explain_feed_query.sh
```

## Event-Derived Distillery Endpoints

When the relay is running with Postgres, the primary Distillery flow can derive
candidate signals from the relay ledger instead of receiving pre-aggregated
counters from the client. This keeps ranking/distribution decisions inside the
relay + Distillery boundary.

Endpoints:

- `POST /distillery/feed-from-events`
- `POST /distillery/rank-from-events`
- `POST /distillery/distribute-from-events`

Supported filters:

- `surface`
- `account_id`
- `channel`
- `since_hours`
- `limit`

`/distillery/feed-from-events` is the canonical feed endpoint for the current
phase. It aggregates `read.completed`, `citation.created`, `derivative.created`
and `value.snapshot` from the relay ledger, ranks the resulting candidates and
returns distributed slots.
