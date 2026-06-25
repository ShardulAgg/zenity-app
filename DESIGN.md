# Design

## What this is

A service that consumes the **Bluesky Jetstream** firehose (one high-volume event
stream), classifies each event by type, and fans it out to **type-specific workers
that run, fail, and scale independently**. It's built as a producer/consumer split
over a small **custom message broker**, deployed on local Kubernetes.

## Architecture at a glance

```
Bluesky Jetstream (wss)
        │  one connection
        ▼
   ┌──────────┐   publish     ┌────────┐   round-robin   ┌────────────┐
   │  ingest  │ ─────────────▶│ broker │ ───────────────▶│  worker ×N │
   │ (router) │  per subject  │ (ours) │   per group     │  handlers  │
   └──────────┘               └────────┘                 └────────────┘
   replicas: 1                replicas: 1                 scale freely
```

Three binaries, one Go module:

- **`cmd/ingest`** (producer) — connects to Jetstream, classifies each event, publishes it to a subject.
- **`cmd/broker`** (ours) — subject routing + queue-group round-robin + bounded per-consumer backpressure.
- **`cmd/worker`** (consumer) — subscribes to subjects and runs the matching handler.

`internal/shared` holds the **only** things both sides share — the `event` wire
contract and the `broker` client. `internal/producer` (jetstream, router) and
`internal/worker` (handlers) are private to each side. The broker is the hard
boundary: the two sides never import each other.

## The event source

Jetstream is a public, no-auth WebSocket firehose of all Bluesky activity as JSON.
Measured live before designing:

- **~420 events/sec** typical, coefficient of variation **~8.5%** (steady, mildly bursty).
- Documented all-time peak **~2,000 events/sec** (Nov 2024 surge).
- Event type = `kind` + `commit.collection` + `commit.operation`. `kind` is a
  **closed** set (`commit`/`identity`/`account`); collections are an **open** set
  (57+ seen in 90s, many third-party) — so the router needs a **default/drop** path,
  not an enumerated list.

**Consequence:** at 400–2,000 eps this is trivial for one goroutine. The system is
**isolation-bound, not throughput-bound** — the design budget goes to isolation,
backpressure, and independent scaling, not raw speed.

## Routing

`subjectFor(event)` applies a precedence (order matters):

1. `kind != "commit"` → drop (identity/account have no commit; guards a nil deref).
2. `operation == "delete"` → `bsky.cleanup` (deletes carry no record; one retraction path).
3. exact `kind|collection|operation` match → its subject.
4. else → drop (counted).

Likes and reposts deliberately share `bsky.aggregation`. Four behaviors:
**notification** (posts → keyword match), **aggregation** (likes/reposts → rolling
count + threshold), **burst** (follows → sliding window per account), **cleanup**
(deletes).

## The broker (ours)

~200 lines, stdlib only. Wire format: **newline-delimited JSON** over TCP —
`{"sub":true,"subject":..,"group":..}` to subscribe, `{"subject":..,"data":<event>}`
to publish/deliver.

- **Routing:** per subject, per group, a list of client connections.
- **Scalability (round-robin):** consumers join a `(subject, group)`; each message
  goes to the *next* consumer in the group. Add consumers → load splits.
  *Verified:* 3 workers split evenly — 42 / 42 / 42 events per 15s.
- **Backpressure:** every connection has a bounded send queue; a slow consumer fills
  its queue and the broker **drops + counts** (`select { case out<-msg: default: drop }`).
  Publishers never block on slow consumers.
- **Startup resilience:** clients retry the dial with backoff, so start order doesn't
  matter. *Verified:* with the broker scaled to 0, clients log `waiting for broker…`
  and connect the moment it returns — zero crash-loops, zero restarts.

## Isolation, backpressure, "one path slow"

- **Isolation is physical:** each behavior is a separate process, reachable only via
  the broker. A worker crash/OOM cannot touch ingest or sibling workers.
- **One path slow:** that subject's slow consumer fills only its own broker-side
  queue and sheds; other subjects are unaffected — and you can scale just that path.
- **The policy is ours**, in the broker (bounded queue + drop-with-count), not
  delegated to a library default.

## Scaling

- **ingest — `replicas: 1` (singleton).** It holds the one firehose connection; a
  second instance would double-consume. Scales vertically only.
- **worker — horizontal.** `kubectl scale deployment worker --replicas=N`; the broker
  round-robins across them.
- **broker — single instance** (no clustering — see limitations).

### The stateful-scaling caveat (important)

Worker scaling is clean only for **stateless** handlers (notification, cleanup). The
**stateful** ones — aggregation (counts per post) and burst (windows per account) —
keep per-key state in memory, so N replicas each see a fraction of a key's events and
under-count. To scale those correctly you must **partition by key**: route all events
for a given post/account to the same replica (consistent hashing on the subject key).
We document this rather than build it; for the prototype, stateful handlers run at
`replicas: 1` (correct) while stateless ones scale.

## Delivery semantics

- The broker is **in-memory, fire-and-forget → at-most-once**; under backpressure we
  drop (and count). No exactly-once claim.
- The producer tracks Jetstream's `time_us` cursor and **resumes after a Jetstream
  disconnect**, but the broker doesn't persist, so a broker restart loses in-flight
  messages. The aggregation/burst signals ("unusual traction", "follow spike") are
  tolerant of approximate counts, which suits at-most-once. A production version would
  add dedup/idempotency.

## Limitations / honest trade-offs

Rolling our own broker means giving up what a real one provides:

- **No durability** — broker restart drops in-flight messages; subscriptions re-establish.
- **No mid-stream reconnect** — clients retry the *initial* connect (implemented) but
  don't yet auto-reconnect if the broker dies *after* connecting.
- **Single broker instance** — a SPOF and the one tier that doesn't horizontally scale.

We chose this deliberately: a lightweight, dependency-free broker keeps the whole
system understandable end-to-end (the heart of the exercise), accepting these costs.

## Prototype → production

- Swap our broker for **NATS JetStream / Kafka** (durability, clustering, reconnect)
  behind the same `Publisher` interface — small code change.
- **Partition stateful subjects by key** so aggregation/burst scale correctly.
- Add **mid-stream reconnect** + dedup/idempotency for at-least-once.
- **Observability:** Prometheus metrics for per-subject throughput, queue depth, drops.
- **Bound handler state:** TTL/LRU on the aggregation map (the burst window already evicts).

## Decisions considered

- **In-process vs broker-split vs hybrid** → chose broker-split for *physical*
  independent scaling (the brief's "fail and scale independently"); the `Publisher`
  interface keeps the transport swappable.
- **NATS vs own broker** → wrote our own to own/understand routing, round-robin, and
  backpressure, accepting the trade-offs above.
- **Config** → kept minimal (only `BROKER_ADDR` / `BROKER_LISTEN` via env);
  thresholds/keywords hardcoded. A ConfigMap/config layer was deliberately out of scope.

## Testing

- **router** — routing precedence (each type → right subject; delete → cleanup;
  non-commit/unhandled → dropped) and `Dispatch` publishes routed / drops the rest.
- **handler** — aggregation threshold fires exactly once; counts are per-subject.
- **broker** — round-robin distribution and drop-on-full backpressure.

All race-clean: `go test -race ./...`.

## Running it

See `README.md`. In short: build the three images → `kind load` → `kubectl apply -f
deploy/` → `kubectl scale deployment worker --replicas=3` to watch the load split.
