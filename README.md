# zenity

Consumes the **Bluesky Jetstream** firehose, classifies each event by type, and
fans it out to type-specific workers over a small **custom message broker** —
producer and consumers as separate processes that scale independently. Runs on a
local Kubernetes cluster.

```
Jetstream ──▶ ingest (router) ──publish──▶ broker (ours) ──round-robin──▶ worker ×N ──▶ handlers
              replicas: 1                  replicas: 1                    scale freely
```

See **[DESIGN.md](DESIGN.md)** for architecture and decisions, **[AI.md](AI.md)** for how AI was used.

## Prerequisites

- Go 1.26+
- Docker (running)
- `kind` and `kubectl`

Tested on macOS (arm64) with Docker Desktop.

## Run locally (no Kubernetes)

Three terminals — defaults wire them together (broker on `:9000`, clients dial `localhost:9000`):

```bash
go run ./cmd/broker     # terminal 1
go run ./cmd/worker     # terminal 2
go run ./cmd/ingest     # terminal 3  (connects to the live firehose)
```

You'll see the broker log subscriptions, ingest connect to Jetstream, and the
worker log handler activity (e.g. `[cleanup] retract …`).

## Tests

```bash
go test -race ./...
```

Covers the routing precedence, the aggregation threshold, and the broker's
round-robin + drop-on-full backpressure.

## Deploy to Kubernetes (kind)

```bash
# 1. cluster
kind create cluster --name zenity

# 2. build the three images
for c in broker ingest worker; do
  docker build -f Dockerfile.$c -t zenity-$c:dev .
done

# 3. load them into the cluster (no registry needed)
kind load docker-image zenity-broker:dev zenity-ingest:dev zenity-worker:dev --name zenity

# 4. apply manifests
kubectl apply -f deploy/

# 5. wait for ready
kubectl rollout status deploy/broker
kubectl rollout status deploy/ingest
kubectl rollout status deploy/worker
kubectl get pods
```

## Watch it run

```bash
kubectl logs deploy/ingest        # connected to broker + streaming from Jetstream
kubectl logs deploy/broker        # subject subscriptions
kubectl logs -f deploy/worker     # live handler activity
```

## Demonstrate independent scaling

Add worker replicas; the broker's round-robin splits the load across them
(ingest stays a singleton):

```bash
kubectl scale deployment worker --replicas=3
kubectl get pods -l app=worker

# events handled per pod — should split roughly evenly:
for p in $(kubectl get pods -l app=worker -o name); do
  echo "$p: $(kubectl logs "$p" | grep -c '\[cleanup\]') events"
done
```

## Demonstrate startup resilience

Clients retry the dial, so start order doesn't matter and the broker can restart:

```bash
kubectl scale deployment broker --replicas=0
kubectl logs deploy/ingest --tail=3   # "[broker] waiting for broker:9000 …" — pod stays Running, no crash-loop
kubectl scale deployment broker --replicas=1   # clients reconnect automatically
```

## Teardown

```bash
kind delete cluster --name zenity
```

## Configuration

Only the broker address is configurable (the rest is intentionally hardcoded for
this exercise):

| Var | Used by | Default |
|---|---|---|
| `BROKER_LISTEN` | broker | `:9000` |
| `BROKER_ADDR` | ingest, worker | `localhost:9000` (manifests set `broker:9000`) |

## Assumptions

- Thresholds, keywords, and the four `wantedCollections` are hardcoded — the
  assignment tests routing/fan-out, not handler tuning (see DESIGN).
- The broker is a single in-memory instance (no durability/clustering); stateful
  handlers (aggregation, burst) run at one replica — scaling them correctly needs
  partition-by-key, which is documented, not built (see DESIGN).

## Layout

```
cmd/        ingest · broker · worker            (three binaries)
internal/
  shared/   event (wire contract) · broker      (imported by both sides)
  producer/ jetstream · router                  (producer-only)
  worker/   handler ×4                           (consumer-only)
deploy/     broker.yaml · ingest.yaml · worker.yaml
Dockerfile.{broker,ingest,worker}
```
