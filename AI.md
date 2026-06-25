# AI usage

## Tools & models

- **Claude (Opus) via Claude Code** — the single tool used, for design dialogue,
  scaffolding, the broker implementation, the Kubernetes manifests, throwaway
  measurement scripts, and the verification runs.
- No extra MCP servers, custom rules, or system-prompt files beyond the defaults.

## How it was used

The leverage was in **design exploration plus fast, verified iteration** — not in
generating a finished system in one shot.

1. **Understand the source by measuring it.** Rather than trust the brief's sample,
   used throwaway Python to connect to the live Jetstream and measure throughput,
   distribution, and the event taxonomy: ~420 events/sec (CV ~8.5%), likes ~70% of
   traffic, 57+ distinct collections in 90s. These numbers drove real decisions — the
   "isolation-bound, not throughput-bound" framing and the open-taxonomy default route.
2. **Explore the architecture out loud.** Compared in-process vs broker-split vs
   hybrid with trade-offs on the table, then chose broker-split for physical
   independent scaling — and later chose to write our **own** broker instead of
   depending on NATS, to keep the mechanics understandable.
3. **Implement incrementally.** Scaffolded the packages (`event`, `router`,
   `jetstream`, handlers), then the custom TCP broker (frame protocol, round-robin
   queue groups, bounded-queue backpressure), then the Dockerfiles and k8s manifests.
4. **Verify every step against reality.** Ran the binaries against the live firehose
   and a real `kind` cluster at each stage — smoke tests, the scaling demo (42/42/42
   split across 3 workers), and reproducing/fixing the startup crash-loop with a
   connect-retry. Each decision was checked, not assumed.

## Design decisions made during the build

- **Measured the source before designing it.** Sampled the live firehose to quantify
  load (~420 ev/s typical, ~2k peak) and the taxonomy (an open-ended collection set).
  Two decisions followed: treat the system as *isolation-bound, not throughput-bound*,
  and route with a default-drop catch-all rather than an enumerated type list.
- **Broker-split over in-process fan-out.** A single-binary, channel-based fan-out
  only gives logical isolation inside one process. Split ingest and workers into
  separate processes so a path can fail and scale independently — the requirement
  that's hardest to fake — with the `Publisher` interface as the seam.
- **A custom broker, not an off-the-shelf one.** Built a ~200-line TCP broker
  (subject routing, queue-group round-robin, bounded-queue backpressure) instead of
  pulling in NATS, to own the routing and backpressure mechanics end-to-end and keep
  the system dependency-free — accepting, and documenting, the durability/clustering/
  reconnect it trades away.
- **Backpressure as an explicit, owned policy.** A bounded per-consumer queue with
  drop-on-full lives in the broker, so load-shedding is a deliberate decision with a
  counter, not an emergent library default.
- **Minimal shared surface.** `shared/` holds only the wire contract and broker
  client; everything else lives with the side that owns it, so the producer/consumer
  boundary is enforced by the broker, not by convention.
- **Scope discipline.** Cut a config/ConfigMap layer and kept handlers deliberately
  thin — the exercise tests routing, fan-out, and isolation, not handler logic — and
  documented the stateful-scaling partition-by-key problem rather than half-building it.

## Where AI helped vs. where I steered or overrode

- **AI helped with:** boilerplate and package scaffolding; the broker's framing
  protocol and concurrency; the Kubernetes manifests; the live-measurement scripts;
  and catching the startup crash-loop and proposing the connect-retry fix.
- **I steered / overrode:**
  - Chose **broker-split over the simpler in-process design** AI first proposed.
  - Chose a **custom broker over NATS** for understandability.
  - Enforced **scope discipline** — dropped a `config` package + ConfigMap as
    over-build, and kept the handlers intentionally simple (the assignment tests the
    routing/fan-out architecture, not handler business logic).
  - Set the **directory boundary** (shared contract vs. per-side code).
  - Decided to **document the stateful-scaling caveat** (partition-by-key) rather than
    build partitioning — knowing where to stop.
