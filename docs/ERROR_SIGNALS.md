# Error Signals

## What `tisane-relay` is

Technically, `tisane-relay` is an agnostic real-time event relay.

You can also think of it as:

- event relay
- sync relay
- event distribution service
- event bus edge

What it is not:

- a finance rules engine
- a Mellis schema registry
- a Me Inn-specific backend

## Error channel in the relay

The relay now treats `signal.error` as a first-class agnostic event family.

It still does not own business semantics.
It only validates the minimal generic shape needed to route and replicate the signal safely.

## What this channel is for

## Golden rule

- expected business state does not enter the agnostic channel
- operational, contractual, or integration failure does

## Regra de ouro

- estado esperado de negocio nao entra no canal agnostico
- falha operacional, contratual ou de integracao entra

Use `signal.error` for:

- integration failures
- contract failures
- authorization failures
- processing failures
- transport failures
- replication failures
- infrastructure failures

Do not use `signal.error` for:

- normal domain outcomes
- expected refusals already represented as business state
- any domain-specific state transition that is not an operational incident

The relay should carry incident signals, not replace domain truth.

## Minimal validation

For `signal.error`, the relay validates:

- `signal_id`
- `code`
- `summary`
- `source`
- `owner_unit_ref`
- `severity`
- `status`
- `occurred_at` in payload or envelope

## Read API

- `GET /relay/errors`

Supported filters:

- `since`
- `limit`
- `owner_unit_ref`
- `source`
- `severity`
- `status`

Authorization:

- `GET /relay/errors` requires `X-Peer-Token`
- the peer token resolves a configured peer
- the server intersects the requested `owner_unit_ref` with the peer `owner_unit_refs`
- if the requested scope is outside the peer ACL, the relay returns `403`

Replication rule:

- `signal.error` is replicated only to peers whose `owner_unit_refs` include the payload `owner_unit_ref`
- non-error domain events remain generic relay traffic; the relay still does not become a business schema owner

Adjacent scoped business read:

- `GET /relay/owned` uses the same peer authentication and `owner_unit_ref` routing model
- applications such as Me Inn can read `mellis.checkout.created` and `mellis.payment.confirmed` there without falling back to open `pull`

## Why this matters

- Me Inn can listen to one error stream
- Mellis can emit one error stream
- the relay stays agnostic
- sensitive detail can stay behind sealed references
- the platform gains one unified operational error corpus for later analytics and AI

## Return path

The intended pattern is:

1. an upstream application publishes intent into the mesh
2. `Mellis` consumes what belongs to it
3. if `Mellis` hits an operational or contractual failure, it emits `signal.error`
4. `tisane-relay` replicates that signal to peers
5. `tisane` becomes the single local listening surface for that error
