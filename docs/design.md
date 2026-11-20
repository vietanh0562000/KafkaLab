# Kafka Lab — design and build plan

## Context

The user wants to learn Kafka comprehensively (consumer, producer, broker, transactions, compaction)
by experiencing the failure modes a real system hits, using `gap-traffic-data-processing` as the
system under test. They asked for a UI to interact with the service plus a simulator. They chose:

- **Scope:** comprehensive Kafka, not only this service's consumer side.
- **Who codes:** Claude builds everything; the user learns by running scenarios in a
  *predict → run → explain* loop (matches their memory: open questions, no answers given up front).
- **Approach:** one lab-control app + docker compose (approach A), **written in Go** (user's call, for a
  lighter footprint: single binary, fast start, low RAM next to 3 brokers and 3 JVMs).

Facts from the codebase that shape the design:

- The service has **no business API**. The only ways to interact: write to Kafka (input), read actuator
  `/manage/health` + `/manage/prometheus` on port 8181 (pipeline state RUNNING/PAUSED/RECOVERING), read
  TimescaleDB (output). The UI is therefore an observation dashboard + scenario panel.
- `listener.concurrency: 1` and single-partition topics in prod hide rebalancing, partition assignment,
  hot partitions and key ordering. The lab uses a 3-partition topic and runs 1–3 service instances in
  one consumer group; no service code changes needed (group id = `spring.kafka.consumer.group-id`,
  `PipelineConfiguration.java:93`).
- Failure behaviour already in the code, which the lab will make visible:
  - Malformed JSON / missing `permanentId` → dropped with a log line and `deserialization_error`
    counter (`ShortTermDataProcessor.java:166`), offset committed. Not stuck.
  - A record that parses but violates a DB constraint (e.g. `permanentId` > 255 chars →
    SQLState 22001) is non-retryable → seek back + pause, forever, with exponential backoff 30 s → 300 s
    (`BatchRetryErrorHandler`, `PipelineRecoverer`). DLQ is off by default.
  - With `PIPELINE_DLQ_ENABLED=true` the **whole batch** goes to `dlq-<topic>`, good records included
    (`DlqPublisher.java:93`). Retry-exhausted (DB down) batches are not diverted by default.
  - `auto-offset-reset: latest` → a new consumer group silently skips the backlog.
  - `isolation.level` is not set → `read_uncommitted` → the service ingests records from aborted
    producer transactions.
  - DB writes use `ON CONFLICT DO NOTHING` fallback → redelivered duplicates are absorbed (and
    producer-side duplicates become invisible in the DB).
- Existing assets: `../gap-ingest-tool/docker-compose.yml` (Kafka 3.9.0 single node, kafka-ui,
  TimescaleDB) and `../gap-data-simulator` (realistic data, behind Keycloak JWT, no failure injection).
  The lab reuses neither directly: it needs 3 brokers and fault injection, but copies their image choices.
- The service Dockerfile needs private Maven repo credentials, so the lab runs the **locally built jar**
  in an `eclipse-temurin:21-jre` container. Host has JDK 25; if Lombok breaks on it, build inside
  `maven:3.9-eclipse-temurin-21` mounting `~/.m2` (the private `de/e21x` deps are already there).
- Minimal valid short-term payload: `ShortTermPayloads.json(...)` in the service's tests.

## Goals / non-goals / anti-goals

**Goals**
- About 22 scenarios across 7 modules covering ~80% of what goes wrong with Kafka in a real system.
- Each scenario: open prediction question → one-click run → live view → reveal (what happened, the
  mechanism, the service code or config responsible) → equivalent CLI commands → open confirm questions.
- Every scenario resets to a known baseline before running.

**Non-goals** (the remaining ~20%, listed in the UI so the user knows what is not covered)
- Security (SASL/SSL/ACLs), Schema Registry/Avro, Kafka Connect, Kafka Streams, MirrorMaker,
  KIP-848 new consumer protocol, tiered storage, multi-DC.
- Production quality: no auth, single user, localhost only.

**Anti-goals** (failure even if it runs)
- A reveal that does not match what actually happened. Every scenario asserts its expected outcome,
  so a wrong explanation surfaces as a FAIL rather than as something the user learns.
- The UI becomes a crutch: every action shows the real CLI command that does the same thing in prod.
- Any change to the `gap-traffic-data-processing` repo or its CI.

## Approaches considered

| | Approach | Verdict |
|---|---|---|
| A | One lab-control app on the host (simulator + chaos + observer + static UI) + compose | **Chosen.** One process, one place to read |
| B | Bash scripts + kafka CLI + kafka-ui + Grafana + markdown workbook | Rejected: no single live view; user asked for a UI |
| C | Separate React frontend + simulator service + control service | Rejected: 3 projects, most effort goes to frontend, no extra Kafka learning |

Language: Go (user's call). Cost: not the user's stack, but acceptable because they will not edit it.

## Architecture

Location: `/home/anh-dinh/EGS/kafka-lab/` (own git repo, sibling of the service).

```
host
├── lab-control (Go, :7070) ── UI (embedded static HTML/JS, SSE)
│     ├── producer modes (franz-go kgo)       ──► kafka-1/2/3
│     ├── admin + lag (franz-go kadm)          ──► kafka-1/2/3
│     ├── chaos (os/exec `docker ...`)         ──► containers
│     ├── observer: actuator poll              ──► gap-svc-1..3 :18181-18183
│     └── observer: row counts (pgx)           ──► timescaledb :15432
docker compose (network `lab`)
├── controller                  apache/kafka:3.9.1, KRaft controller-only (heap 256 MB)
├── kafka-1, kafka-2, kafka-3   apache/kafka:3.9.1, broker-only
├── kafka-ui                    kafbat/kafka-ui (pinned; provectuslabs is unmaintained) :18080
├── timescaledb                 timescale/timescaledb (pinned, pg16) :15432
└── gap-svc-1..3                eclipse-temurin:21-jre, mounts build/service.jar; svc-3 behind a profile
```

### Why franz-go
Pure Go (no cgo); supports idempotence, transactions, and admin (`kadm`: offsets, lag, group
describe, offset reset, configs); its default partitioner hashes keys with murmur2 like Java, so a key
lands on the same partition as with the service's Java client. `segmentio/kafka-go` lacks
transactions; `confluent-kafka-go` needs cgo, and librdkafka's default partitioner differs from Java's.
The UI always shows the **Java property name** (`acks`, `enable.idempotence`, `isolation.level`…),
since that is what the user meets in the service.

### Compose specifics
- Topology (user's call): 3 broker-only nodes + 1 controller-only node. Brokers and controller fail
  independently, so ISR loss (5.2) and controller loss (5.3) are different actions, and 1.3 really
  goes through unclean election. Cost: about 400 MB more RAM. Lost: the majority math of a 3-voter
  quorum; the 5.3 reveal says production runs 3 or 5 controllers and explains the majority in text.
- Brokers: listeners INTERNAL `:9092` (compose network), EXTERNAL advertised
  `localhost:19092/19093/19094` for lab-control. Controller: `:9093`, voters `0@controller:9093`.
- Broker defaults chosen so failures show up within seconds:
  `default.replication.factor=3`, `min.insync.replicas=2`, `offsets.topic.replication.factor=3`,
  `transaction.state.log.replication.factor=3`, `transaction.state.log.min.isr=2`,
  `replica.lag.time.max.ms=10000`, `log.retention.check.interval.ms=10000`,
  `group.initial.rebalance.delay.ms=0`, `auto.create.topics.enable=false`,
  `KAFKA_HEAP_OPTS=-Xmx512m`.
- Service env (per instance): `SPRING_PROFILES_ACTIVE=local`, `SPRING_CLOUD_CONFIG_ENABLED=false`,
  `SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092`,
  `SPRING_DATASOURCE_URL=jdbc:postgresql://timescaledb:5432/postgres`,
  `KAFKA_SHORT_TERM_TOPIC=lab.short-term`, `SPRING_KAFKA_CONSUMER_GROUP_ID=gap-lab`,
  `JAVA_TOOL_OPTIONS=-Xmx384m`, `HOSTNAME=gap-svc-N`.
- Shortened service timings (user's call), set through `SPRING_APPLICATION_JSON`: Hikari
  `connection-timeout` 5 s, `socketTimeout` 5 s, JDBC query-timeout 5 s, `pause-duration-ms` 10 s,
  `max-pause-duration-ms` 60 s. Every reveal that involves time shows **lab: X / prod: Y** and the
  formula, e.g. "first PAUSED ≈ (1 + retries) × (socket timeout + backoff)". Per-scenario overrides (DLQ on, low
  `max.poll.interval.ms` via `SPRING_APPLICATION_JSON`) are written to `build/svc.env` and applied
  with `docker compose up -d --force-recreate`.
- Ports avoid the user's running containers: 7070, 18080, 15432, 19092–19094, 18181–18183.
- RAM budget about 4.5 GB, peak about 5 GB during 2.1 (host has ~7 GB free): 2 service instances by
  default, the 3rd and 4th on demand.

### Topics (created by lab-control at startup and on reset, via kadm)
- `lab.short-term`: 3 partitions, RF 3, `min.insync.replicas=2`, short `segment.ms` so retention
  scenarios run in seconds.
- `dlq-lab.short-term`: 3 partitions (the service could auto-create it, but explicit is clearer).
- `lab.compacted`: `cleanup.policy=compact`, `segment.ms=10000`, `min.cleanable.dirty.ratio=0.01`,
  `delete.retention.ms=10000` (not consumed by the service).

### Go layout
```
kafka-lab/
  compose.yaml   Makefile (up, down, jar, run, selftest)   README.md   docs/design.md (copy of this)
  cmd/lab/main.go
  internal/kafkax/    client builder from a ProducerProfile {acks, idempotent, txnID, keyMode, maxRequestSize…}; admin helpers
  internal/payload/   short-term JSON builder (from ShortTermPayloads); seq number carried in header.tag
  internal/chaos/     docker wrapper: stop/start/kill/pause/unpause, network disconnect/connect, recreate svc with env
  internal/observe/   1 s poller → Snapshot{brokers, partitions(leader, ISR, end offset, committed, lag, owner),
                      instances(container status, pipeline state, polled/accepted/dedup/failed), dbRows, lateRows}
  internal/scenario/  Scenario{ID, Module, Title, Question, Baseline, Steps[], Reveal, CLI[], ConfirmQs[]};
                      Step = action | waitUntil(pred, timeout) | expect(pred); runner emits events
  internal/web/       net/http: GET /api/scenarios, POST /api/scenarios/{id}/run, POST /api/reset,
                      POST /api/produce, GET /api/events (SSE: snapshots + step events); embed web/static
  web/static/         index.html, app.js, style.css (vanilla JS, no build step)
```
Deps: `github.com/twmb/franz-go` (kgo, kadm), `github.com/jackc/pgx/v5`. Standard library for the rest.

### UI (one page)
```
┌ Modules ─────────┐┌ Scenario: Poison pill ─────────────────────┐┌ Live ────────────────────────────┐
│ M0 Basics        ││ ? Predict: one record with a 300-char      ││ Brokers  k1● k2● k3●             │
│ M1 Producer      ││   permanentId arrives in a batch of 500.   ││ P0 L=k1 ISR[1,2,3] end 1200 lag 0│
│ M2 Consumer grp  ││   What happens to the other 499, and to    ││ P1 L=k2 ISR[2,3,1] end 1180 lag 0│
│ M3 Failures      ││   the records behind them?                 ││ P2 L=k3 ISR[3,1,2] end 1210 lag 340│
│ M4 Offsets       ││ [ Run ]  [ Reset baseline ]                ││ svc-1 RUNNING  P0,P1             │
│ M5 Cluster       ││ ── timeline ──                             ││ svc-2 PAUSED   P2  next resume 60s│
│ M6 Semantics     ││ 12:01:03 produced 500 (1 poison) → P2      ││ DB rows 2380   late 0            │
│ Not covered (20%)││ 12:01:04 svc-2 PAUSED ✓ expected           ││ Event log …                      │
│ [Free play]      ││ [ Reveal ] → mechanism + code refs + CLI   ││                                  │
└──────────────────┘└────────────────────────────────────────────┘└──────────────────────────────────┘
```
"Free play" panel: manual produce (rate, key mode, acks, idempotence, poison %), and buttons to
kill/pause/start each broker, the DB and each service instance. Scenario text is in Vietnamese with
English Kafka terms (matches how the user talks).

## Scenario catalogue

Expected outcomes are hypotheses from reading the code. Each one is asserted, and confirmed or
corrected while building. "svc" means the gap service.

| # | Scenario | Trigger | Expected / assert | CLI shown |
|---|---|---|---|---|
| **M0 Basics** |||||
| 0.1 | Topic, partition, offset, group | produce 1 000 keyed msgs, 1 svc | lag → 0, DB rows = 1 000, each partition's end offset = committed | `kafka-topics --describe`, `kafka-consumer-groups --describe` |
| 0.2 | Lag and throughput | burst 200 k, then trickle 1 msg/s | lag drains; at trickle, latency floor ≈ `fetch.max.wait` 500 ms because `fetch.min.bytes`=5 MB | `kafka-consumer-groups --describe` |
| **M1 Producer** |||||
| 1.1 | Keys and ordering | key=permanentId vs null key | same key always the same partition; null key → sticky batches | `kafka-console-producer --property parse.key=true` |
| 1.2 | Hot partition | 90% of traffic on one key, 3 svc | one partition lags while 2 instances idle; adding consumers doesn't help | describe group |
| 1.3 | acks and durability | pause both followers → controller shrinks ISR to the leader (10 s) → produce → kill leader → unpause followers → partition offline → force an unclean election (kadm `ElectLeaders` unclean; verify KRaft 3.9 honours it, else set the topic config) | acks=1: writes accepted, then lost when a stale follower becomes leader; acks=all: rejected with NOT_ENOUGH_REPLICAS, nothing falsely acked | `kafka-topics --describe` (ISR), `kafka-leader-election --election-type UNCLEAN` |
| 1.4 | Idempotence | idempotence off, short `request.timeout.ms`, pause leader 3 s mid-send | duplicate seqs in topic (probabilistic: assert ≥0, show count); DB count unchanged (ON CONFLICT hides it) | `kafka-console-consumer --from-beginning` |
| 1.5 | Message too large | 2 MB record | `MESSAGE_TOO_LARGE`/`RecordTooLarge`; client-side vs broker-side limit | `kafka-configs --describe` |
| **M2 Consumer group** |||||
| 2.1 | Scale out / in | 1 → 2 → 3 → 4 instances | rebalance on each change; the 4th instance gets 0 partitions | describe group `--members --verbose` |
| 2.2 | Crash mid-batch | `docker kill` svc during burst | batch redelivered to another member; DB rows exact (dedup counter > 0) | describe group |
| 2.3 | Zombie consumer | `docker network disconnect` svc for > session.timeout 30 s, reconnect | evicted, partitions reassigned; on return commit fails → redelivery | describe group |
| 2.4 | Slow consumer | svc with `max.poll.interval.ms`=15 s + DB table lock held by lab | member leaves group, rebalance; retry guard (retries×backoff < interval) still passes | describe group |
| **M3 Failure handling (svc)** |||||
| 3.1 | DB down | `docker stop lab-timescaledb` 60 s | retry ×2 → PAUSED → backoff 10 s, 20 s, 40 s (prod 30, 60, 120…) → start DB → RECOVERING → RUNNING; lag drains; no loss | — |
| 3.2 | Poison pill | 1 record with 300-char permanentId in a batch of 500 | PAUSED forever, same offset redelivered, lag grows | `kafka-consumer-groups --describe` |
| 3.3 | DLQ | 3.2 with `PIPELINE_DLQ_ENABLED=true` | whole batch (500) in `dlq-lab.short-term`; pipeline continues; then "replay minus poison" restores 499 | `kafka-console-consumer --topic dlq-…` |
| 3.4 | Malformed and schema drift | invalid JSON; renamed `permanentId`; extra unknown field | first two dropped and committed (counter only); unknown field ignored | — |
| **M4 Offsets and time** |||||
| 4.1 | New group, `latest` | recreate svc with a new group id while backlog exists | backlog skipped silently; DB rows < produced | `kafka-consumer-groups --list` |
| 4.2 | Replay | stop all svc, reset group to datetime, start | replays; DB rows unchanged (idempotent sink); reset refused while group active | `--reset-offsets --to-datetime --execute` |
| 4.3 | Retention gap | stop svc, produce, `retention.ms`=10 s, wait, start | offset out of range → reset to latest → gap | `kafka-configs --alter` |
| 4.4 | Late data | startTime yesterday and 7 days ago | rows in late-arrival tracking classified SHORT_LATE / LONG_LATE (verify `LateArrivalDetector.classify`) | — |
| **M5 Cluster** |||||
| 5.1 | Leader failover | kill leader of P0 under load | new leader from ISR, brief producer/consumer stall, no loss with acks=all | `kafka-topics --describe` |
| 5.2 | ISR and min.insync | kill 2 brokers (controller stays up) | ISR shrinks to 1; acks=all producer → NOT_ENOUGH_REPLICAS; consumers still read; acks=1 still writes | describe topic |
| 5.3 | Controller loss | kill the controller, then kill a broker | brokers keep serving existing leaders; no leader re-election for the dead broker's partitions, no topic creation | `kafka-metadata-quorum describe --status` |
| **M6 Semantics** |||||
| 6.1 | Aborted transaction | produce 100 in a transaction, abort | svc (read_uncommitted) writes 100 rows; read_committed probe sees 0 | `kafka-console-consumer --isolation-level read_committed` |
| 6.2 | Hanging transaction (LSO) | open a transaction, write, never commit | read_committed probe lag stuck at the LSO; svc unaffected | `kafka-transactions --list` |
| 6.3 | Log compaction | 50 updates × 10 keys + 2 tombstones on `lab.compacted` | after the cleaner runs, from-beginning read = 8 latest values | `kafka-configs`, console consumer |

Baseline (each scenario's `Baseline`, plus the global Reset): all brokers up, DB up, N svc with default
env, topics deleted and recreated, group offsets deleted, `traffic_data` short-term table truncated.

## Build plan (milestones, each runnable)

1. **Infra:** compose + Makefile (`jar` copies `gap-streaming/target/gap-streaming-*.jar` →
   `build/service.jar`). Done when 3 brokers are healthy, 1 svc reaches RUNNING, and kafka-ui shows the cluster.
2. **lab-control skeleton:** kadm topic setup, observer Snapshot, SSE, static UI with the Live panel
   and Free play. Done when manual produce of 1 000 msgs shows lag → 0 and DB rows = 1 000 live.
3. **Scenario engine + M0, M3:** these exercise the service's own code paths first.
4. **M2, M4.**
5. **M1, M5, M6.**
6. **`--selftest`:** runs every scenario headless and prints PASS/FAIL per assertion; README with a
   learning order (M0 → M3 → M2 → M4 → M1 → M5 → M6).

After approval: copy this design to `kafka-lab/docs/design.md` and break milestones into tasks
(writing-plans skill).

## Verification

- `go vet ./...` and `go test ./...` (payload builder, Prometheus text parsing, scenario runner with a
  fake chaos/observer).
- `make up && make jar && go run ./cmd/lab` → open `http://localhost:7070`, run 0.1, check numbers
  against kafka-ui (`:18080`) and `psql -p 15432`.
- `go run ./cmd/lab --selftest`: all deterministic scenarios PASS; probabilistic ones (1.3, 1.4)
  report observed counts. Any FAIL means the reveal text is wrong: fix the text, not the assertion.
- Use the Playwright MCP to screenshot the UI during 3.1 and 2.1 to confirm the live panel updates.

## Calls made after the builder check (Claude's, correctable)
- **Docker safety:** compose project name `kafkalab`, every container named `lab-*`; the chaos package
  only acts on containers carrying the `com.docker.compose.project=kafkalab` label and refuses
  anything else. The user has other containers running (`timescaledb`, `kafka`…), so a bare
  `docker stop timescaledb` could hit a real database.
- **Two networks:** `lab-kafka` (brokers, svc, kafka-ui) and `lab-db` (timescaledb, svc). Scenario 2.3
  disconnects svc from `lab-kafka` only, so the zombie still reaches its DB.
- **Reset race:** after recreating topics, reset waits for the group to be Stable with all partitions
  assigned, then commits offset 0 on each partition. Otherwise `auto-offset-reset: latest` skips the
  first records produced.
- **3.4 split in two:** text without `"intervalLength":"PT1M"` is dropped by `IntervalFilter` before
  parsing (anomalous-drop log, `records.filtered{result=drop}`); text that contains it but is invalid
  JSON hits `deserialization_error`. Same outcome (dropped and committed), but a different counter is
  the only trace. Assert each separately.
- **2.1 needs a 4th instance:** `lab-gap-svc-4` behind a compose profile, started only for 2.1.

## Left to the builder
- Exact image tags (pin existing versions), the actuator health JSON path for pipeline state, and
  Prometheus metric names (`gap.traffic-data-processing.pipeline.*` per `application.yml`).
- Mechanics of the "slow DB" trigger (table lock vs `docker pause`) as long as 2.4 asserts the rebalance.
- How scenario text is stored (Go strings vs embedded markdown).

## Facts confirmed in Task 1 (2026-11-20)

- Tables: `traffic_data.traffic_data_sensor_short_term_data` (PK `start_time`, `permanent_id varchar(255)`),
  `traffic_data.late_arrival_tracking`.
- The service's view migrations need tables owned by `gap-td-devices-catalog`
  (`traffic_data_sensors`, `detection_sites`, `virtual_detection_sites`, `virtual_detection_site_sensors`);
  the lab creates them from `db/init/10-catalog-tables.sql`. Migrations also need TimescaleDB ≥ 2.2x
  (`CREATE TABLE ... WITH (tsdb.*)`), so the lab runs 2.27.0.
- Each service instance runs **two consumer groups** on `lab.short-term`: `gap-lab` (ingestion) and
  `gap-lab-tracking` (late-arrival tracking). Both commit independently.
- Prometheus label `pipeline` is `short_term` (and `short_term_tracking`), with an underscore.
- Counter meanings: `records_polled_total` = delivered by Kafka; `records_filtered_total{result}` = source
  filter; `records_accepted_total` = handed to the processor (not "written"); `records_failed_total{error_type}`
  = rejected by the processor; `records_deduplicated_total` = in-batch dedup in the processor (not the DB's
  ON CONFLICT). Known traffic (3 valid, 1 renamed id, 1 without intervalLength) gave polled 5, pass 4,
  drop 1, accepted 4, failed{missing_required_fields} 1, DB rows 3.

## Measured (2026-11-20)

- 0.2 burst: 200,000 records drained by 2 instances in ~6 s (~30k rows/s); peak lag seen ~190,000.
- 0.2 trickle with jittered gaps: ack→row latency 21–518 ms (p50 148, p90 356), consistent with
  `fetch.min.bytes`=5 MB never being reached and the broker answering at `fetch.max.wait.ms`=500.
- A steady 1/s rhythm phase-locks onto the 500 ms fetch cycle: console-consumer delays sat at ~159 ms every time.
- With `segment.ms=10000` on the topic, the same consumer saw ~20 ms delays: the broker answered the waiting
  fetch early. `lab.short-term` therefore keeps the default `segment.ms`; scenario 4.3 must set it at runtime.
- The service image (eclipse-temurin:21-jre-alpine, same as production) cannot load snappy-java's native library
  (needs glibc `ld-linux-x86-64.so.2`). Snappy-compressed batches make the consumer throw a non-record
  KafkaException in a loop: polled stays 0, lag grows, the pipeline gauge stays RUNNING. Candidate for an M1 scenario.
