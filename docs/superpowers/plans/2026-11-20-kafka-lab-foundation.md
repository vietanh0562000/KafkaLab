# Kafka Lab — Foundation (infra, lab-control, M0, M3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A runnable Kafka lab: 3 brokers + 1 controller + TimescaleDB + the real gap service, driven by a Go
app on `:7070` with a live dashboard, free-play controls, and the first 6 scenarios (M0 basics, M3 the
service's own failure handling), each asserting its expected outcome.

**Architecture:** One Go binary (`cmd/lab`) on the host. It produces with franz-go, administers with
kadm, injects faults via the `docker` CLI (guarded to compose project `kafkalab`), polls Kafka/actuator/
Postgres every second into a `Snapshot`, and pushes snapshots and step events to an embedded static
page over SSE. Scenarios are Go values: baseline + steps (action / wait-until / expect) + Vietnamese text.

**Tech Stack:** Go 1.26, `github.com/twmb/franz-go v1.22.3` (+ `pkg/kadm v1.20.0`), `github.com/jackc/pgx/v5 v5.13.0`,
Docker Compose v5, apache/kafka 3.9.0, TimescaleDB 2.17.2-pg16, vanilla JS.

**Spec:** `docs/design.md` (copy of the approved design). Read it before any task.

**Scope:** Design milestones 1–3 only. M2/M4 and M1/M5/M6 + `--selftest` get their own plans after this
one runs, because their assertions depend on numbers this plan measures (real timings, metric names).
Known input for the M4 plan: late-arrival tracking only records rows when a traffic centre resolves
(`LateArrivalDetector.detect` skips `vz == null`), so 4.4 needs `TRAFFIC_CENTER` set on the service.

## Global Constraints

- Never modify `/home/anh-dinh/EGS/gap-traffic-data-processing` sources; building its jar into its `target/` is allowed.
- Go module path `kafkalab`. Dependencies: only franz-go (kgo, kadm), pgx v5; standard library otherwise. Import `_ "time/tzdata"` in `cmd/lab`.
- Compose project `kafkalab`; containers `lab-controller`, `lab-kafka-1..3`, `lab-kafka-ui`, `lab-timescaledb`, `lab-gap-svc-1..4`; networks named exactly `lab-kafka`, `lab-db`.
- Images: `apache/kafka:3.9.0`, `timescale/timescaledb:2.17.2-pg16`, `eclipse-temurin:21-jre-alpine`, `ghcr.io/kafbat/kafka-ui:v1.3.0` (3.9.0 instead of the design's 3.9.1: cached locally, same as `gap-ingest-tool`).
- Host ports: lab 7070, kafka-ui 18080, timescaledb 15432, brokers 19092/19093/19094, service management 18181–18184.
- Topics: `lab.short-term` (3 partitions, RF 3, `min.insync.replicas=2`, `segment.ms=10000`), `dlq-lab.short-term` (3, RF 3), `lab.compacted` (3, RF 3, `cleanup.policy=compact`, `segment.ms=10000`, `min.cleanable.dirty.ratio=0.01`, `delete.retention.ms=10000`). Default consumer group `gap-lab`.
- Service lab timings vs prod: Hikari connection-timeout 5000 / 30000 ms, `socketTimeout` 5 / 30 s, JDBC query-timeout 5 / 30 s, pause 10000 / 30000 ms, max pause 60000 / 300000 ms. Retries 2 × 1000 ms in both.
- DB: table `traffic_data.traffic_data_sensor_short_term_data` (columns `permanent_id`, `start_time`), late table `traffic_data.late_arrival_tracking`. Task 1 confirms; if different, Task 1 records the real names in `docs/design.md` and every later task uses those.
- Prometheus names start `gap_traffic_data_processing_pipeline_`; pipeline state gauge 0=RUNNING, 1=RECOVERING, 2=PAUSED; label `pipeline="short-term"`.
- JSON produced by the Go API uses lowerCamel names of the Go fields (`json:"dbRows"` for `DBRows`).
- UI and scenario text in Vietnamese with English Kafka terms. Every scenario lists its Java property names and CLI commands in the form `docker exec lab-kafka-1 /opt/kafka/bin/<tool>.sh --bootstrap-server kafka-1:9092 ...`.
- Confirm questions are open: no answers anywhere in the UI.
- Code comments: one line, only the why; no change-history comments.

## Review Focus

1. **A dependency is down while the observer polls** (DB stopped, broker killed, service container gone, compose not up): each tick finishes within ~1 s and reports a per-source error; lab-control never stalls or exits. Test in Task 5; manual check in Task 9.
2. **A second run or a reset while a scenario runs:** rejected with `ErrBusy` (HTTP 409); the running one is unaffected. Test in Task 7.
3. **A chaos action aimed at a container outside `kafkalab`** (the user's real `timescaledb`): `ErrNotLab`, and no mutating docker command is issued. Test in Task 2.
4. **Reset after a scenario left the lab broken** (paused broker, stopped DB, service with DLQ override): baseline restored. Test in Task 6.
5. **A slow or closed browser tab on SSE:** publishers never block. Test in Task 6.

---

### Task 1: Compose infrastructure, service jar, observed facts

**Files:**
- Create: `.gitignore`, `compose.yaml`, `Makefile`, `README.md`, `internal/observe/testdata/prometheus.txt`
- Modify: `docs/design.md` (append "Facts confirmed in Task 1")

**Interfaces:**
- Produces: compose service names `controller`, `kafka-1..3`, `kafka-ui`, `timescaledb`, `gap-svc-1..4` (3 and 4 under profile `scale`); compose variables `SVC_GROUP_ID` (default `gap-lab`), `SVC_DLQ_ENABLED` (default `false`), `SVC_EXTRA_OPTS` (default empty) read from `.env`; `build/service.jar`; the Prometheus fixture.

- [ ] **Step 1: `.gitignore`** with `build/`, `.env`, `*.out`.

- [ ] **Step 2: `compose.yaml`** with top-level `name: kafkalab`, networks `lab-kafka: {name: lab-kafka}` and `lab-db: {name: lab-db}`. Values that matter:
  - `controller`: `KAFKA_NODE_ID=0`, `KAFKA_PROCESS_ROLES=controller`, `KAFKA_LISTENERS=CONTROLLER://:9093`, `KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER`, `KAFKA_CONTROLLER_QUORUM_VOTERS=0@controller:9093`, `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT`, `KAFKA_HEAP_OPTS=-Xmx256m -Xms256m`, fixed `CLUSTER_ID=a2Fma2FsYWJjbHVzdGVyMQ` (any valid 22-char base64 works, but it must match on every node).
  - `kafka-N` (N=1..3, via a YAML anchor): `KAFKA_NODE_ID=N`, `KAFKA_PROCESS_ROLES=broker`, `KAFKA_LISTENERS=INTERNAL://:9092,EXTERNAL://:1909(1+N)`, `KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka-N:9092,EXTERNAL://localhost:1909(1+N)`, `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT`, `KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL`, `KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER`, `KAFKA_CONTROLLER_QUORUM_VOTERS=0@controller:9093`, the same `CLUSTER_ID`, `KAFKA_HEAP_OPTS=-Xmx512m -Xms512m`, and the broker defaults from the design: `KAFKA_DEFAULT_REPLICATION_FACTOR=3`, `KAFKA_MIN_INSYNC_REPLICAS=2`, `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3`, `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2`, `KAFKA_REPLICA_LAG_TIME_MAX_MS=10000`, `KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=10000`, `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`. Ports `1909(1+N):1909(1+N)`. Network `lab-kafka`.
  - `kafka-ui`: env `KAFKA_CLUSTERS_0_NAME=lab`, `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092`; port `18080:8080`; network `lab-kafka`.
  - `timescaledb`: `POSTGRES_PASSWORD=postgres`, port `15432:5432`, `shm_size: 512m`, network `lab-db`, no named volume (state is disposable).
  - `gap-svc-N` (anchor, N=1..4; 3 and 4 get `profiles: [scale]`): image temurin, `command: ["java","-jar","/app/service.jar"]`, volume `./build/service.jar:/app/service.jar:ro`, port `1818N:8181`, networks `lab-kafka` and `lab-db`, `stop_grace_period: 10s`, environment:

    ```yaml
    SPRING_PROFILES_ACTIVE: local
    SPRING_CLOUD_CONFIG_ENABLED: "false"
    SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
    SPRING_KAFKA_CONSUMER_GROUP_ID: ${SVC_GROUP_ID:-gap-lab}
    SPRING_DATASOURCE_URL: jdbc:postgresql://timescaledb:5432/postgres
    SPRING_DATASOURCE_USERNAME: postgres
    SPRING_DATASOURCE_PASSWORD: postgres
    DEFAULT_SCHEMA: traffic_data
    KAFKA_SHORT_TERM_TOPIC: lab.short-term
    PIPELINE_DLQ_ENABLED: ${SVC_DLQ_ENABLED:-false}
    HOSTNAME: gap-svc-N
    JAVA_TOOL_OPTIONS: -Xmx384m ${SVC_EXTRA_OPTS:-}
    SPRING_APPLICATION_JSON: '{"spring":{"datasource":{"hikari":{"connection-timeout":5000,"data-source-properties":{"socketTimeout":5}}},"jdbc":{"template":{"query-timeout":5}}},"pipelines":{"error-handling":{"pause-duration-ms":10000,"max-pause-duration-ms":60000}}}'
    ```

- [ ] **Step 3: `Makefile`** targets:
  - `jar`: `cd ../gap-traffic-data-processing && ./mvnw -q -pl gap-streaming -am package -DskipTests`, then copy `gap-streaming/target/gap-streaming-*.jar` (not `*-plain.jar`) to `build/service.jar`. If the host JDK 25 build fails, fall back to `docker run --rm -v $$HOME/.m2:/root/.m2 -v $(abspath ../gap-traffic-data-processing):/src -w /src maven:3.9-eclipse-temurin-21 mvn -q -o -pl gap-streaming -am package -DskipTests`.
  - `up`: `touch .env && docker compose up -d controller kafka-1 kafka-2 kafka-3 kafka-ui timescaledb gap-svc-1`.
  - `down`: `docker compose --profile scale down`.
  - `run`: `go run ./cmd/lab`. `test`: `go test ./...`. `itest`: `go test -tags integration ./...`.

- [ ] **Step 4: Build the jar.** Run `make jar`. Expected: `build/service.jar` exists, more than 50 MB.

- [ ] **Step 5: Start the stack and create the topic by hand.**
  Run `make up`, then `docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic lab.short-term --partitions 3 --replication-factor 3`.
  Expected: `docker exec lab-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status` prints `LeaderId: 0`; `--describe --topic lab.short-term` shows 3 partitions with `Isr` of length 3.

- [ ] **Step 6: Confirm the service is up and the schema names.**
  Run `curl -s localhost:18181/manage/prometheus | grep pipeline_state` → a line with `pipeline="short-term"` and value `0.0`.
  Run `docker exec lab-timescaledb psql -U postgres -c '\dt traffic_data.*'` and `\d traffic_data.traffic_data_sensor_short_term_data`.
  Expected: both tables exist and the PK columns are `permanent_id`, `start_time`. Write whatever you actually saw under "Facts confirmed in Task 1" in `docs/design.md`.

- [ ] **Step 7: Produce known traffic and capture the fixture.** With `kafka-console-producer.sh` (`--property parse.key=true --property key.separator=|`), send 5 records:
  - 3 valid, built from `ShortTermPayloads.json` with keys and permanentIds `lab-sensor-001..003`, `intervalLength` `PT1M`, and today's startTime;
  - 1 with `permanentID` (capital D) instead of `permanentId`;
  - 1 valid JSON without `intervalLength`.

  Wait 5 s, then run `curl -s localhost:18181/manage/prometheus | grep '^gap_traffic_data_processing_pipeline_' > internal/observe/testdata/prometheus.txt`.
  Expected in the file: polled 5, filtered pass 4, filtered drop 1, accepted 3, failed `error_type="missing_required_fields"` 1, state 0. If a name differs from the Global Constraints, note it in `docs/design.md`.

- [ ] **Step 8: kafka-ui.** Open `http://localhost:18080`. Expected: cluster `lab` with 3 brokers online.

- [ ] **Step 9: README** (what this is, `make jar up run`, ports table, `make down`), then commit:

```bash
git add .gitignore compose.yaml Makefile README.md docs internal/observe/testdata
git commit -m "feat: lab infrastructure with 3 brokers, controller, TimescaleDB and the gap service"
```

---

### Task 2: `internal/chaos` — guarded docker wrapper

**Files:**
- Create: `go.mod`, `internal/chaos/docker.go`, `internal/chaos/docker_test.go`

**Interfaces:**
- Produces:
  ```go
  type Runner interface{ Run(ctx context.Context, name string, args ...string) (string, error) }
  type ExecRunner struct{}                       // os/exec, combined output, error includes output
  var ErrNotLab = errors.New("refusing to touch a container outside compose project kafkalab")
  type SvcEnv struct{ GroupID string; DLQ bool; ExtraOpts string }
  func DefaultSvcEnv() SvcEnv                    // {GroupID: "gap-lab"}
  func (e SvcEnv) DotEnv() string
  type Docker struct{ R Runner; Dir, Project string; OnCommand func(cmd string) }
  func New(dir string) *Docker                   // ExecRunner, Project "kafkalab"
  func (d *Docker) Stop/Start/Kill/Pause/Unpause(ctx context.Context, container string) error
  func (d *Docker) Disconnect/Connect(ctx context.Context, network, container string) error
  func (d *Docker) States(ctx context.Context) (map[string]string, error)        // name -> running|exited|paused|...
  func (d *Docker) IPs(ctx context.Context, network string) (map[string]string, error) // ip -> container name
  func (d *Docker) ComposeUp(ctx context.Context, services ...string) error       // up -d, no recreate
  func (d *Docker) RecreateSvc(ctx context.Context, env SvcEnv, n int) error      // writes Dir/.env, recreates gap-svc-1..n, stops the rest
  func (d *Docker) StopSvc(ctx context.Context, services ...string) error          // compose stop
  ```
  Every container-level method first runs `docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' <container>` and returns `ErrNotLab` unless the trimmed output equals `Project`. Compose calls are `docker compose -p kafkalab -f <Dir>/compose.yaml --project-directory <Dir> ...`. `OnCommand` receives each mutating command as one shell string.

- [ ] **Step 1: Write failing tests** (a `fakeRunner` records calls and returns canned output per first arg pair):

```go
func TestRefusesContainerOutsideProject(t *testing.T) {
	f := &fakeRunner{out: map[string]string{"inspect": "\n"}}
	d := &Docker{R: f, Project: "kafkalab"}
	err := d.Stop(context.Background(), "timescaledb")
	if !errors.Is(err, ErrNotLab) { t.Fatalf("want ErrNotLab, got %v", err) }
	if f.called("stop") { t.Fatal("stop must not run") }
}
func TestStopLabContainer(t *testing.T) {
	f := &fakeRunner{out: map[string]string{"inspect": "kafkalab\n"}}
	var seen string
	d := &Docker{R: f, Project: "kafkalab", OnCommand: func(c string) { seen = c }}
	if err := d.Stop(context.Background(), "lab-timescaledb"); err != nil { t.Fatal(err) }
	if seen != "docker stop lab-timescaledb" { t.Fatalf("got %q", seen) }
}
func TestRecreateSvcWritesEnvAndStopsExtras(t *testing.T) {
	dir := t.TempDir(); f := &fakeRunner{}
	d := &Docker{R: f, Dir: dir, Project: "kafkalab"}
	if err := d.RecreateSvc(context.Background(), SvcEnv{GroupID: "gap-lab", DLQ: true}, 2); err != nil { t.Fatal(err) }
	env, _ := os.ReadFile(filepath.Join(dir, ".env"))
	if string(env) != "SVC_GROUP_ID=gap-lab\nSVC_DLQ_ENABLED=true\nSVC_EXTRA_OPTS=\n" { t.Fatalf("env %q", env) }
	// expect one call containing: up -d --force-recreate gap-svc-1 gap-svc-2
	// and one call containing: stop gap-svc-3 gap-svc-4
}
func TestStatesParsesPs(t *testing.T) {
	f := &fakeRunner{out: map[string]string{"ps": "lab-kafka-1\trunning\nlab-timescaledb\texited\n"}}
	got, _ := (&Docker{R: f, Project: "kafkalab"}).States(context.Background())
	// want map[lab-kafka-1:running lab-timescaledb:exited]
}
func TestIPsParsesNetworkInspect(t *testing.T) {
	// output "lab-gap-svc-1 172.20.0.7/16\nlab-kafka-1 172.20.0.3/16\n" -> map["172.20.0.7":"lab-gap-svc-1", ...]
}
```

- [ ] **Step 2:** `go mod init kafkalab && go test ./internal/chaos/` → FAIL (undefined).
- [ ] **Step 3: Implement** `docker.go`. `States` uses `docker ps -a --filter label=com.docker.compose.project=kafkalab --format '{{.Names}}\t{{.State}}'`. `IPs` uses `docker network inspect <net> -f '{{range .Containers}}{{.Name}} {{.IPv4Address}}{{"\n"}}{{end}}'` and strips the `/prefix`.
- [ ] **Step 4:** `go test ./internal/chaos/` → PASS.
- [ ] **Step 5: Commit** `feat(chaos): docker wrapper scoped to the kafkalab project`.

---

### Task 3: `internal/payload` — messages and a collision-free timeline

**Files:**
- Create: `internal/payload/payload.go`, `internal/payload/payload_test.go`

**Interfaces:**
- Produces:
  ```go
  func SensorID(i int) string                                  // "lab-sensor-%03d"
  func ShortTerm(permanentID string, start time.Time, seq int64) []byte
  func Poison(start time.Time, seq int64) []byte               // permanentId = 300 x "p"
  func NoInterval(permanentID string, start time.Time, seq int64) []byte
  func BrokenJSON(seq int64) []byte
  func UnknownField(permanentID string, start time.Time, seq int64) []byte
  func RenamedID(permanentID string, start time.Time, seq int64) []byte  // key "permanentID"
  func Seq(value []byte) (int64, bool)                         // from header.tag "seq-<n>"
  func TodayBerlinUTC(now time.Time) time.Time                 // midnight Europe/Berlin, in UTC
  type Point struct{ SensorID string; Start time.Time }
  type Timeline struct{ /* minute cursor */ }
  func NewTimeline(start time.Time) *Timeline
  func (t *Timeline) Batch(n, sensors int) []Point             // sensor = i % sensors, minute = cursor + i/sensors; cursor advances
  ```
  Envelope as in the service's `ShortTermPayloads.json`, plus `"header":{"source":"kafka-lab","tag":"seq-<n>"}`; startTime as RFC 3339 UTC (`2026-11-19T23:00:00Z`); `intervalLength` written exactly as `"intervalLength":"PT1M"` (no space; `IntervalFilter` matches bytes).

- [ ] **Step 1: Failing tests**

```go
var t0 = time.Date(2026, 11, 19, 23, 0, 0, 0, time.UTC)
func TestShortTermValidWithSeq(t *testing.T) {
	b := ShortTerm("lab-sensor-001", t0, 42)
	if !json.Valid(b) || !bytes.Contains(b, []byte(`"intervalLength":"PT1M"`)) { t.Fatal(string(b)) }
	if s, ok := Seq(b); !ok || s != 42 { t.Fatal(s, ok) }
	if !bytes.Contains(b, []byte(`"startTime":"2026-11-19T23:00:00Z"`)) { t.Fatal(string(b)) }
}
func TestPoisonID300(t *testing.T)     { /* unmarshal; len(payload.permanentId) == 300; contains interval needle */ }
func TestNoIntervalLacksField(t *testing.T) { /* json.Valid; !Contains(`"intervalLength"`) */ }
func TestBrokenJSONHasNeedleButInvalid(t *testing.T) { /* Contains(`"intervalLength":"PT1M"`) && !json.Valid */ }
func TestRenamedIDHasNoPermanentId(t *testing.T) { /* !Contains(`"permanentId"`) && Contains(`"permanentID"`) */ }
func TestTodayBerlinUTC(t *testing.T) {
	got := TodayBerlinUTC(time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC))
	if !got.Equal(time.Date(2026, 11, 19, 23, 0, 0, 0, time.UTC)) { t.Fatal(got) }
}
func TestTimelineNeverRepeats(t *testing.T) {
	tl := NewTimeline(t0)
	seen := map[Point]bool{}
	for _, p := range append(tl.Batch(7, 3), tl.Batch(5, 2)...) {
		if seen[p] { t.Fatalf("duplicate %v", p) }; seen[p] = true
	}
	got := NewTimeline(t0).Batch(4, 3)
	want := []Point{{"lab-sensor-000", t0}, {"lab-sensor-001", t0}, {"lab-sensor-002", t0}, {"lab-sensor-000", t0.Add(time.Minute)}}
	if !slices.Equal(got, want) { t.Fatalf("%v", got) }
}
```

- [ ] **Step 2:** `go test ./internal/payload/` → FAIL.
- [ ] **Step 3: Implement** with `encoding/json` over small structs (so tests unmarshal what production writes); `BrokenJSON` is a literal `{"payload":{"intervalLength":"PT1M","permanentId":` + truncated tail.
- [ ] **Step 4:** `go test ./internal/payload/` → PASS.
- [ ] **Step 5: Commit** `feat(payload): short-term messages, poison/malformed variants, timeline`.

---

### Task 4: `internal/kafkax` — producer profiles and admin

**Files:**
- Create: `internal/kafkax/kafkax.go`, `internal/kafkax/admin.go`, `internal/kafkax/kafkax_test.go`, `internal/kafkax/admin_integration_test.go` (`//go:build integration`)

**Interfaces:**
- Produces:
  ```go
  const Topic, DLQTopic, CompactedTopic, Group = "lab.short-term", "dlq-lab.short-term", "lab.compacted", "gap-lab"
  var Brokers = []string{"localhost:19092", "localhost:19093", "localhost:19094"}
  type ProducerProfile struct{ Acks string /* "all"|"1"|"0" */; Idempotent bool; TxnID string; RequestTimeout time.Duration; MaxRequestBytes int32 }
  func DefaultProfile() ProducerProfile                       // {Acks: "all", Idempotent: true}
  func (p ProducerProfile) JavaProps() []string               // e.g. ["acks=all", "enable.idempotence=true"], plus transactional.id / request.timeout.ms / max.request.size when set
  func NewProducer(p ProducerProfile) (*kgo.Client, error)    // error if Idempotent && Acks != "all" (Java rejects it too)
  type Record struct{ Key string; Value []byte }
  type Result struct{ Partition int32; Offset int64; Err error }
  func ProduceSync(ctx context.Context, cl *kgo.Client, recs []Record) []Result  // same order as recs
  type TopicSpec struct{ Name string; Partitions int32; RF int16; Configs map[string]*string }
  func LabTopics() []TopicSpec
  type PartitionInfo struct{ Partition, Leader int32; Replicas, ISR []int32; End int64 }
  type PartitionLag struct{ Partition int32; Committed, End, Lag int64; OwnerHost string }  // host without leading "/"
  type GroupState struct{ State string; Members, Assigned int; Lags []PartitionLag }
  type Admin struct{ /* *kadm.Client */ }
  func NewAdmin() (*Admin, error)                              // lazy: never fails when brokers are down
  func (a *Admin) BrokerCount(ctx context.Context) (int, error)
  func (a *Admin) RecreateTopics(ctx context.Context, specs []TopicSpec) error   // delete, wait until gone (≤30 s), create
  func (a *Admin) Partitions(ctx context.Context, topic string) ([]PartitionInfo, error)
  func (a *Admin) Group(ctx context.Context, group, topic string) (GroupState, error)
  func (a *Admin) CommitOffsets(ctx context.Context, group, topic string, offsets map[int32]int64) error
  func (a *Admin) DeleteGroup(ctx context.Context, group string) error           // not-found is not an error
  func (a *Admin) EndOffsetSum(ctx context.Context, topic string) (int64, error)
  ```
  The producer uses franz-go's default partitioner (murmur2, Java-compatible). `Acks` maps to `kgo.AllISRAcks/LeaderAck/NoAck`; non-idempotent profiles add `kgo.DisableIdempotentWrite()`.

- [ ] **Step 1: Failing unit tests**

```go
func TestJavaProps(t *testing.T) {
	if got := DefaultProfile().JavaProps(); !slices.Equal(got, []string{"acks=all", "enable.idempotence=true"}) { t.Fatal(got) }
	if got := (ProducerProfile{Acks: "1"}).JavaProps(); !slices.Equal(got, []string{"acks=1", "enable.idempotence=false"}) { t.Fatal(got) }
}
func TestIdempotenceNeedsAcksAll(t *testing.T) {
	if _, err := NewProducer(ProducerProfile{Acks: "1", Idempotent: true}); err == nil { t.Fatal("want error") }
}
func TestLabTopics(t *testing.T) {
	// 3 specs; lab.short-term: 3 partitions, RF 3, min.insync.replicas "2", segment.ms "10000";
	// lab.compacted: cleanup.policy "compact"
}
```

- [ ] **Step 2: Failing integration tests** (need `make up`):

```go
func TestRecreateDescribeProduce(t *testing.T) {
	a := mustAdmin(t)
	if err := a.RecreateTopics(ctx, LabTopics()); err != nil { t.Fatal(err) }
	parts, _ := a.Partitions(ctx, Topic) // len 3, each len(ISR) == 3
	cl, _ := NewProducer(DefaultProfile())
	recs := make([]Record, 10) // keys "k0".."k9"
	for _, r := range ProduceSync(ctx, cl, recs) { if r.Err != nil { t.Fatal(r.Err) } }
	if n, _ := a.EndOffsetSum(ctx, Topic); n != 10 { t.Fatal(n) }
}
func TestCommitOffsetsOnEmptyGroup(t *testing.T) {
	// CommitOffsets("itest-group", Topic, {0:0,1:0,2:0}); Group(...).Lags all Committed == 0; DeleteGroup cleans up
}
```

- [ ] **Step 3:** `go test ./internal/kafkax/` → FAIL; `make itest` → FAIL.
- [ ] **Step 4: Implement.** `Group` combines `DescribeGroups` (state, members, assignment → `OwnerHost` per partition), `FetchOffsets`, and `ListEndOffsets`; a partition with no commit has `Committed = -1`, and its lag counts from 0.
- [ ] **Step 5:** `go test ./internal/kafkax/` PASS and `make itest` PASS with the stack up.
- [ ] **Step 6: Commit** `feat(kafkax): producer profiles with Java property names, admin helpers`.

---

### Task 5: `internal/observe` — metrics parser and a poller that never stalls

**Files:**
- Create: `internal/observe/metrics.go`, `internal/observe/poller.go`, `internal/observe/observe_test.go`

**Interfaces:**
- Consumes: `kafkax.PartitionInfo`, `kafkax.GroupState`; fixture from Task 1.
- Produces:
  ```go
  type Instance struct{ Name, Container, State, Err string; Polled, Accepted, Dedup, FilteredDrop float64; Failed map[string]float64; Partitions []int32 }
  type Snapshot struct{ At time.Time; Containers map[string]string; Partitions []kafkax.PartitionInfo; Group kafkax.GroupState; DLQEnd int64; Instances []Instance; DBRows, LateRows int64; Errors map[string]string }
  func ParseMetrics(r io.Reader) (Instance, error)   // only pipeline="short-term"; State "RUNNING"|"RECOVERING"|"PAUSED"
  type Poller struct {
  	Containers func(ctx context.Context) (map[string]string, error)
  	IPs        func(ctx context.Context) (map[string]string, error)
  	Kafka      func(ctx context.Context) ([]kafkax.PartitionInfo, kafkax.GroupState, int64, error)
  	Metrics    func(ctx context.Context, svc string) (Instance, error)  // svc "lab-gap-svc-N"
  	DB         func(ctx context.Context) (rows, late int64, err error)
  	Services   []string                                                // lab-gap-svc-1..4
  	Timeout    time.Duration                                           // per source, default 800ms
  }
  func NewPoller(d *chaos.Docker, a *kafkax.Admin, db *pgxpool.Pool, group func() string) *Poller
  func (p *Poller) Tick(ctx context.Context) Snapshot   // sources in parallel, each under Timeout
  func (p *Poller) Run(ctx context.Context, every time.Duration, publish func(Snapshot))
  func (p *Poller) Latest() Snapshot
  ```
  `Errors` keys: `containers`, `kafka`, `db`, or the service name. Metrics are fetched only for services whose container is `running`. `Instance.Partitions` comes from `Group.Lags[].OwnerHost` mapped through `IPs`. `group` returns the current consumer group, because scenario 4.1 changes it.

- [ ] **Step 1: Failing tests**

```go
func TestParseFixture(t *testing.T) {
	f, _ := os.Open("testdata/prometheus.txt")
	in, err := ParseMetrics(f)
	if err != nil { t.Fatal(err) }
	if in.State != "RUNNING" || in.Polled != 5 || in.Accepted != 3 || in.FilteredDrop != 1 ||
		in.Failed["missing_required_fields"] != 1 { t.Fatalf("%+v", in) }
}
func TestTickSurvivesHangingSource(t *testing.T) {
	p := fakePoller() // all sources return quickly, except DB, which blocks until ctx is done
	start := time.Now()
	s := p.Tick(context.Background())
	if time.Since(start) > 1100*time.Millisecond { t.Fatal("tick stalled") }
	if s.Errors["db"] == "" || s.Containers["lab-kafka-1"] != "running" { t.Fatalf("%+v", s) }
}
func TestOwnersMappedToInstances(t *testing.T) {
	// Group.Lags: P0, P1 OwnerHost "172.20.0.7"; P2 "172.20.0.8"; IPs map those to lab-gap-svc-1/2
	// want Instances["lab-gap-svc-1"].Partitions == [0 1], ["lab-gap-svc-2"] == [2]
}
func TestSkipsMetricsForStoppedService(t *testing.T) { /* container "exited" -> Metrics never called, Instance.Container "exited" */ }
```

- [ ] **Step 2:** `go test ./internal/observe/` → FAIL.
- [ ] **Step 3: Implement.** Line-based Prometheus text parser (skip `#`; split name, `{labels}`, value). `NewPoller` wires real sources: HTTP GET `http://localhost:1818N/manage/prometheus`; SQL `SELECT count(*) FROM traffic_data.traffic_data_sensor_short_term_data` and the same for `late_arrival_tracking`; an undefined-table error (42P01) is reported as 0 rows, not an error.
- [ ] **Step 4:** `go test ./internal/observe/` → PASS.
- [ ] **Step 5: Commit** `feat(observe): snapshot poller with per-source timeouts`.

---

### Task 6: `internal/events` and `internal/lab` — hub, produce, chaos, reset

**Files:**
- Create: `internal/events/hub.go`, `internal/events/hub_test.go`, `internal/lab/lab.go`, `internal/lab/reset.go`, `internal/lab/lab_test.go`

**Interfaces:**
- Consumes: chaos, payload, kafkax, observe.
- Produces:
  ```go
  // events
  type Event struct{ Type string; At time.Time; Data any }   // Type: snapshot|log|step|scenario
  func NewHub() *Hub
  func (h *Hub) Publish(e Event)                              // never blocks; drops for a full subscriber (buffer 64)
  func (h *Hub) Subscribe() (<-chan Event, func())

  // lab
  type Admin interface {
  	BrokerCount(context.Context) (int, error); DeleteGroup(context.Context, string) error
  	RecreateTopics(context.Context, []kafkax.TopicSpec) error
  	CommitOffsets(context.Context, string, string, map[int32]int64) error
  	Group(context.Context, string, string) (kafkax.GroupState, error)
  }
  type DB interface {
  	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
  	QueryRow(context.Context, string, ...any) pgx.Row
  }
  type Lab struct{ Docker *chaos.Docker; Admin Admin; DB DB; Hub *events.Hub; Snap func() observe.Snapshot; /* timeline, seq, group */ }
  type ResetOpts struct{ Instances int; Env chaos.SvcEnv }
  type ProduceReq struct{ Count, Sensors, RatePerSec, PoisonEvery int; Acks string; Idempotent bool }
  func (l *Lab) NextRecords(n, sensors int) []kafkax.Record          // fresh (sensor, minute) pairs, key = sensor id
  func (l *Lab) NextSeq() int64
  func (l *Lab) ProduceRecords(ctx context.Context, p kafkax.ProducerProfile, recs []kafkax.Record) ([]kafkax.Result, error)
  func (l *Lab) Produce(ctx context.Context, r ProduceReq) ([]kafkax.Result, error) // RatePerSec 0 = burst; every PoisonEvery-th record is Poison
  func (l *Lab) Chaos(ctx context.Context, action, target string) error  // stop|start|kill|pause|unpause|disconnect|connect (network lab-kafka)
  func (l *Lab) Reset(ctx context.Context, o ResetOpts) error
  func (l *Lab) Group() string                                     // group of the last Reset
  func (l *Lab) RowExists(ctx context.Context, permanentID string, start time.Time) (bool, error)
  func (l *Lab) Snapshot() observe.Snapshot
  func (l *Lab) Log(format string, args ...any)                    // Publish {Type:"log"}
  ```

- [ ] **Step 1: Failing tests**

```go
func TestPublishNeverBlocksOnSlowSubscriber(t *testing.T) {
	h := NewHub(); _, cancel := h.Subscribe(); defer cancel() // never read
	fast, c2 := h.Subscribe(); defer c2()
	done := make(chan struct{})
	go func() { for i := 0; i < 1000; i++ { h.Publish(Event{Type: "log"}) }; close(done) }()
	go func() { for range fast {} }()
	select { case <-done: case <-time.After(100 * time.Millisecond): t.Fatal("publish blocked") }
}
func TestResetRestoresBrokenLab(t *testing.T) {
	// fake Runner: ps reports lab-kafka-2 paused, lab-timescaledb exited; inspect returns kafkalab
	// fake Admin: BrokerCount 3; Group returns {State:"Stable", Assigned:3}
	// fake DB records Exec SQL
	err := l.Reset(ctx, ResetOpts{Instances: 2, Env: chaos.DefaultSvcEnv()})
	// want, in order: "docker unpause lab-kafka-2"; compose up -d controller kafka-1 kafka-2 kafka-3 kafka-ui timescaledb;
	// compose stop gap-svc-1..4; DeleteGroup("gap-lab"); RecreateTopics(LabTopics());
	// CommitOffsets("gap-lab", Topic, {0:0,1:0,2:0}); Exec "TRUNCATE traffic_data.traffic_data_sensor_short_term_data, traffic_data.late_arrival_tracking";
	// compose up -d --force-recreate gap-svc-1 gap-svc-2; .env has SVC_DLQ_ENABLED=false
}
func TestResetWithNewGroupSkipsCommit(t *testing.T) {
	// Env.GroupID "gap-lab-new" -> CommitOffsets never called for it (scenario 4.1 relies on that)
}
func TestProducePoisonEvery(t *testing.T) { /* Count 10, PoisonEvery 5 -> records 5 and 10 are Poison payloads (via a fake send func) */ }
```

- [ ] **Step 2:** `go test ./internal/events/ ./internal/lab/` → FAIL.
- [ ] **Step 3: Implement.**
  - `Reset` steps, each logged with `l.Log`, timeouts in brackets:
    1. unpause every `paused` container;
    2. `ComposeUp` the base services;
    3. stop all service instances;
    4. wait until `BrokerCount` == 3 [60 s] and the DB answers `SELECT 1` [60 s];
    5. `DeleteGroup` both the old group and `o.Env.GroupID`;
    6. `RecreateTopics`;
    7. if `o.Env.GroupID == "gap-lab"`, commit offset 0 on all 3 partitions;
    8. truncate, ignoring 42P01;
    9. `RecreateSvc(o.Env, o.Instances)`;
    10. wait until `Group` is `Stable` with `Assigned == 3` [120 s];
    11. reset the timeline to `TodayBerlinUTC(now)`.
  - `Produce` with `RatePerSec > 0` sends one record per tick.
- [ ] **Step 4:** `go test ./internal/events/ ./internal/lab/` → PASS.
- [ ] **Step 5: Commit** `feat(lab): event hub, produce, guarded chaos, baseline reset`.

---

### Task 7: `internal/scenario` — engine

**Files:**
- Create: `internal/scenario/scenario.go`, `internal/scenario/runner.go`, `internal/scenario/runner_test.go`

**Interfaces:**
- Consumes: `lab.Lab`, `lab.ResetOpts`, `events.Event`, `observe.Snapshot`.
- Produces:
  ```go
  type Ctx struct{ Lab *lab.Lab; Snapshot func() observe.Snapshot; Vars map[string]any }
  type Step struct{ Name string; Do func(context.Context, *Ctx) (observed string, err error) }
  func Action(name string, fn func(context.Context, *Ctx) error) Step
  func WaitUntil(name string, timeout time.Duration, pred func(observe.Snapshot, *Ctx) bool) Step   // checks every 250 ms; observed = "after 12.3s"
  func Expect(name string, check func(observe.Snapshot, *Ctx) (ok bool, observed string)) Step
  type Scenario struct{ ID, Module, Title, Question, Reveal string; Baseline lab.ResetOpts; Steps []Step; JavaProps, CLI, Confirm []string }
  type Summary struct{ ID, Module, Title, Question, Reveal string; JavaProps, CLI, Confirm []string }
  type StepResult struct{ Name string; Passed bool; Observed, Err string; Took time.Duration }
  type Result struct{ ID string; Passed bool; Steps []StepResult }
  var ErrBusy, ErrUnknown error
  func NewRunner(l *lab.Lab, hub *events.Hub, all []Scenario) *Runner
  func (r *Runner) Summaries() []Summary                   // registry order
  func (r *Runner) Start(id string) error                  // async; ErrBusy / ErrUnknown
  func (r *Runner) RunSync(ctx context.Context, id string) (Result, error)
  func (r *Runner) Reset() error                           // async lab.Reset(default 2 instances) under the same busy lock
  func All() []Scenario                                    // M0 then M3; filled by Tasks 10–11
  ```
  Events: `{Type:"step", Data:{scenario, step, status: running|pass|fail, observed, err}}` and `{Type:"scenario", Data: Result}`. The first failing step stops the run; nothing cleans up afterwards, the next baseline does. The baseline itself is step 0, named "Baseline".

- [ ] **Step 1: Failing tests** (runner built through an unexported `newRunner(reset func(context.Context, lab.ResetOpts) error, snap func() observe.Snapshot, emit func(events.Event), all []Scenario)`):

```go
func TestPassingScenarioEmitsSteps(t *testing.T)      { /* Action ok + Expect true -> Result.Passed; step events running,pass x3 (incl. Baseline) */ }
func TestWaitUntilTimesOut(t *testing.T)              { /* pred false, timeout 300ms -> Passed false, Observed contains "timeout" */ }
func TestFailureStopsRemainingSteps(t *testing.T)     { /* step 2 fails -> step 3 Do never called */ }
func TestBusyRejectsRunAndReset(t *testing.T) {
	// scenario whose Action blocks on a channel; Start("a") ok; Start("b") == ErrBusy; Reset() == ErrBusy;
	// release; wait for the scenario event; Start("b") ok
}
func TestUnknownScenario(t *testing.T)                { /* Start("nope") == ErrUnknown */ }
```

- [ ] **Step 2:** `go test ./internal/scenario/` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** `go test ./internal/scenario/` → PASS.
- [ ] **Step 5: Commit** `feat(scenario): step engine with busy guard and step events`.

---

### Task 8: `internal/web` — HTTP API, SSE, the page

**Files:**
- Create: `internal/web/server.go`, `internal/web/server_test.go`, `web/embed.go` (`//go:embed static`), `web/static/index.html`, `web/static/app.js`, `web/static/style.css`

**Interfaces:**
- Consumes: `events.Hub`, `scenario.Summary`, `lab.ProduceReq`, `chaos.ErrNotLab`, `scenario.ErrBusy/ErrUnknown`, `observe.Snapshot`.
- Produces:
  ```go
  type Backend interface {
  	Summaries() []scenario.Summary
  	Start(id string) error
  	Reset() error
  	Produce(ctx context.Context, r lab.ProduceReq) error
  	Chaos(ctx context.Context, action, target string) error
  	Snapshot() observe.Snapshot
  }
  func NewServer(b Backend, hub *events.Hub, static fs.FS) http.Handler
  ```
  Routes (Go 1.22 patterns):
  - `GET /` static
  - `GET /api/events` SSE: `event: <type>\ndata: <json>\n\n`, plus a `:ping` every 15 s
  - `GET /api/snapshot`
  - `GET /api/scenarios`
  - `POST /api/scenarios/{id}/run` → 202 / 404 / 409
  - `POST /api/reset` → 202 / 409
  - `POST /api/produce` → 202; runs async, errors go to the event log
  - `POST /api/chaos` `{action,target}` → 200, 403 on ErrNotLab, 400 otherwise

- [ ] **Step 1: Failing tests** with a fake Backend and `httptest`:

```go
func TestRunBusyIs409(t *testing.T)      { /* Start -> ErrBusy => 409 */ }
func TestUnknownRunIs404(t *testing.T)   { /* ErrUnknown => 404 */ }
func TestChaosNotLabIs403(t *testing.T)  { /* Chaos -> chaos.ErrNotLab => 403 */ }
func TestSSEDeliversPublishedEvent(t *testing.T) {
	// GET /api/events on httptest.Server; hub.Publish(Event{Type:"snapshot", Data: map[string]int{"dbRows": 7}})
	// read until a line "event: snapshot" followed by a data line containing "\"dbRows\":7"
}
func TestIndexServed(t *testing.T)       { /* GET / -> 200, body contains "Kafka Lab" */ }
```

- [ ] **Step 2:** `go test ./internal/web/` → FAIL.
- [ ] **Step 3: Implement the server.** The SSE handler unsubscribes when the request context ends.
- [ ] **Step 4: Write the page** following the design's three-column sketch:
  - **Left:** modules M0–M6 with their scenarios (unbuilt modules greyed out "sắp có"), plus "Free play" and "Chưa cover (20%)" listing the design's non-goals.
  - **Centre:**
    - the scenario card: Question, Run, Reset, a live step timeline (✓/✗ + observed);
    - a Reveal button enabled after the run ends;
    - Java properties and CLI in `<pre>`, Confirm questions.
  - **Free play:**
    - produce form (count, sensors, rate/s, acks all|1|0, idempotent, poison every N);
    - one row per lab container with stop/start/kill/pause/unpause, and disconnect/connect for services;
    - the event log.
  - **Right Live panel, from `snapshot` events:**
    - container states;
    - partitions table (P, leader, ISR, end, committed, lag, owner instance) with the group state;
    - instances (container, pipeline state coloured RUNNING green / RECOVERING amber / PAUSED red, polled/accepted/dedup/drop, failed by type, partitions);
    - DB rows, late rows, DLQ end;
    - source errors in red.

  The layout must work at 1280 px wide; mobile is a non-goal.
- [ ] **Step 5:** `go test ./internal/web/` → PASS.
- [ ] **Step 6: Commit** `feat(web): API, SSE stream and dashboard page`.

---

### Task 9: `cmd/lab` — wiring, headless run, milestone check

**Files:**
- Create: `cmd/lab/main.go`

**Interfaces:**
- Consumes: everything above.
- Produces: flags `-addr :7070`, `-dir .`, `-run <id>`. With `-run`, it runs `RunSync` and prints each step as `PASS|FAIL <step> — <observed>`; exit code 0 if passed, 1 otherwise.

- [ ] **Step 1: Implement.**
  - Create `chaos.New(dir)`, `kafkax.NewAdmin()`, and `pgxpool.New` for `postgres://postgres:postgres@localhost:15432/postgres`. pgxpool connects lazily, so startup must not fail.
  - `Docker.OnCommand` → `lab.Log`.
  - Run the poller every 1 s, publishing `{Type:"snapshot"}`.
  - The backend adapts Runner + Lab to `web.Backend`.
  - Shut down cleanly on SIGINT.
- [ ] **Step 2: Startup with nothing running (Review Focus 1).** Run `make down && go run ./cmd/lab`, then open `http://localhost:7070`.
  Expected: the page loads; the Live panel shows errors for `kafka` and `db`; the process keeps running.
- [ ] **Step 3: Milestone 2 check.** Run `make up`, then:
  - `curl -XPOST localhost:7070/api/reset` and wait for the log line `reset done`;
  - `curl -XPOST localhost:7070/api/produce -d '{"count":1000,"sensors":100,"acks":"all","idempotent":true}'`.

  Expected within 30 s: `curl -s localhost:7070/api/snapshot | jq '.dbRows, ([.group.lags[].lag] | add)'` prints `1000` and `0`; the page updates live.
- [ ] **Step 4: Guard check (Review Focus 3).** Run `curl -s -o /dev/null -w '%{http_code}' -XPOST localhost:7070/api/chaos -d '{"action":"stop","target":"timescaledb"}'` → `403`.
- [ ] **Step 5: Commit** `feat: lab-control entrypoint with headless scenario runs`.

---

### Task 10: M0 scenarios

**Files:**
- Create: `internal/scenario/m0.go`
- Modify: `internal/scenario/scenario.go` (`All()` includes M0)

**Interfaces:**
- Consumes: `Action`, `WaitUntil`, `Expect`, `lab.Lab.NextRecords/ProduceRecords/RowExists`, `kafkax.DefaultProfile`.

- [ ] **Step 1: 0.1 "Topic, partition, offset, group"** — Baseline `{Instances: 1}`.
  - Steps: produce `NextRecords(1000, 100)` with acks=all; WaitUntil [60 s] lag sum 0 and `DBRows == 1000`.
  - Expect: committed == end for every partition; the end sum == 1000; every partition end > 0.
  - Reveal points:
    - partition = murmur2(key) % 3, so one sensor always lands on one partition;
    - an offset is per partition;
    - committed = the next offset to read; lag = end − committed;
    - the service commits only after the DB write: `AckMode.MANUAL` in `KafkaConfiguration.java`, `ack.acknowledge()` after the handler in `FilteredKafkaDataSource.java`.
  - JavaProps: `enable.auto.commit=false`, `spring.kafka.listener.ack-mode=manual`, `max.poll.records=500`.
  - CLI: `kafka-topics.sh --describe --topic lab.short-term`, `kafka-consumer-groups.sh --describe --group gap-lab`.
  - Confirm (Vietnamese):
    1. "Nếu service commit offset trước khi ghi DB rồi pod chết, chuyện gì xảy ra với batch đó?"
    2. "Tăng topic từ 3 lên 4 partition thì sensor nào đổi partition, và thứ tự record của sensor đó bị ảnh hưởng thế nào?"
    3. "Vì sao lag tính bằng end − committed chứ không phải số record đã xử lý?"
- [ ] **Step 2: 0.2 "Lag and throughput"** — Baseline `{Instances: 2}`.
  - Steps:
    1. burst `NextRecords(200000, 1000)`;
    2. WaitUntil [30 s] lag sum > 10000 (record the peak lag in Vars);
    3. WaitUntil [240 s] lag 0 and `DBRows == 200000`;
    4. Action "trickle": 10 records, 1 per second; for each, poll `RowExists` every 20 ms; store the latencies.
  - Expect: p90 latency ≥ 200 ms and max ≤ 2000 ms; observed prints all latencies.
  - Reveal hypothesis, which the assertion checks:
    - at 1 msg/s a fetch never reaches `fetch.min.bytes`=5 MB, so the broker answers only when `fetch.max.wait`=500 ms expires;
    - each record therefore waits 0–500 ms (≈250 ms on average) before the service even sees it;
    - throughput vs latency is the trade-off this config makes.
  - If the measured p90 is under 200 ms, the hypothesis is wrong: investigate and rewrite the reveal, don't loosen the assertion.
  - JavaProps: `fetch.min.bytes=5242880`, `fetch.max.wait.ms=500`, `max.poll.records=500`.
  - CLI: `kafka-consumer-groups.sh --describe --group gap-lab` (run it repeatedly during the burst).
  - Confirm:
    1. "Nếu hạ fetch.min.bytes xuống 1 thì latency và số lần fetch thay đổi thế nào?"
    2. "Thêm instance thứ 3 có làm burst xong nhanh hơn không, khi DB mới là nút thắt?"
- [ ] **Step 3: Verify.** `go run ./cmd/lab -run 0.1` → exit 0; `go run ./cmd/lab -run 0.2` → exit 0, and note the latencies in `docs/design.md` "Measured".
- [ ] **Step 4: Commit** `feat(scenario): M0 basics`.

---

### Task 11: M3 scenarios — the service's failure handling

**Files:**
- Create: `internal/scenario/m3.go`
- Modify: `internal/scenario/scenario.go` (`All()` includes M3)

**Interfaces:**
- Consumes: as Task 10, plus `lab.Chaos`, `chaos.Docker.StopSvc/ComposeUp`, `payload.Poison/NoInterval/BrokenJSON/UnknownField/RenamedID`, `kafkax.Result` (partition and offset of the poison record).

- [ ] **Step 1: 3.1 "DB down"** — Baseline `{Instances: 2}`.
  - Steps:
    1. produce 2000; WaitUntil lag 0;
    2. Chaos `stop lab-timescaledb` and store the stop time;
    3. produce 2000;
    4. WaitUntil [45 s] some instance is PAUSED (observed: seconds since the stop);
    5. WaitUntil [60 s] the same instance went PAUSED → RECOVERING → PAUSED (track transitions in Vars);
    6. Chaos `start lab-timescaledb`;
    7. WaitUntil [120 s] all running instances are RUNNING, lag 0, and `DBRows == 4000`.
  - Expect: no loss (`DBRows == 4000`).
  - Reveal:
    - connection failures are retryable (`DefaultExceptionClassifier`);
    - 2 in-thread retries 1 s apart (`BatchRetryErrorHandler.retryLoop`), then seek back, keep the offset uncommitted, and pause (`PipelineRecoverer.recover`);
    - backoff lab 10/20/40/60 s, prod 30/60/120/240/300 s;
    - first PAUSED ≈ 3 × (connection timeout: lab 5 s / prod 30 s) + 2 × 1 s ≈ 17 s lab / 92 s prod. Print the measured value next to the formula. If they disagree (e.g. a fast DNS failure), fix the formula text;
    - the guard maxRetries × backoff < `max.poll.interval.ms` exists so retries never cost a rebalance.
  - JavaProps: `max.poll.interval.ms=300000`, `pipelines.error-handling.batch-retry.max-retries=2`, `backoff-ms=1000`.
  - CLI: `kafka-consumer-groups.sh --describe --group gap-lab` (lag grows, offsets frozen).
  - Confirm:
    1. "Vì sao service không commit offset khi pause mà vẫn không mất data dù pod restart?"
    2. "Nếu tăng max-retries lên 400 thì điều gì sẽ hỏng trước?"
- [ ] **Step 2: 3.2 "Poison pill"** — Baseline `{Instances: 2}`.
  - Steps:
    1. `StopSvc` both instances;
    2. produce 499 good records + 1 `Poison` keyed `lab-sensor-007`, and store its partition and offset;
    3. `ComposeUp gap-svc-1 gap-svc-2`;
    4. WaitUntil [90 s] the instance that owns the poison partition is PAUSED;
    5. Action: wait 40 s, recording the states seen.
  - Expect:
    - that instance was never RUNNING after the first PAUSED;
    - the poison partition's committed offset ≤ the poison offset;
    - `DBRows < 500`;
    - the other instance is RUNNING.
  - Reveal:
    - SQLState 22001 (value too long for `varchar(255)`) is non-retryable;
    - the whole batch is sought back and paused forever, so the blast radius is every partition of that consumer, not one record;
    - the DLQ is off by default (`pipelines.error-handling.dlq.enabled=false`);
    - malformed JSON is dropped instead (see 3.4), so the dangerous records are the ones that parse.
  - CLI: describe group (committed frozen).
  - Confirm:
    1. "Instance kia vẫn chạy — vậy những sensor nào bị kẹt, những sensor nào không?"
    2. "Restart pod có gỡ kẹt được không? Vì sao?"
- [ ] **Step 3: 3.3 "DLQ"** — Baseline `{Instances: 2, Env: {GroupID: "gap-lab", DLQ: true}}`.
  - Steps:
    1. the same production as 3.2;
    2. WaitUntil [90 s] main-topic lag 0 and all instances RUNNING;
    3. Expect `DLQEnd > 1` and `DBRows + DLQEnd == 500`;
    4. Action "replay minus poison": consume `dlq-lab.short-term` from the start with a group-less kgo consumer, drop values whose `permanentId` length > 255, and produce the rest to `lab.short-term` with the original keys;
    5. WaitUntil [60 s] `DBRows == 499`.
  - Reveal:
    - `DlqPublisher` diverts the whole failed batch (good records too), because a bulk COPY failure can't say which row;
    - headers `x-original-partition`/`x-original-offset` allow targeted replay;
    - retry-exhausted (DB down) batches are not diverted, otherwise an outage would empty healthy data into the DLQ;
    - replayed records arrive after newer ones, so per-key order is broken.
  - CLI: `kafka-console-consumer.sh --topic dlq-lab.short-term --from-beginning --property print.headers=true --property print.key=true`.
  - Confirm:
    1. "Nếu replay luôn cả record poison thì chuyện gì xảy ra?"
    2. "Thứ tự theo sensor sau khi replay còn đúng không, và điều đó quan trọng với bảng nào?"
- [ ] **Step 4: 3.4 "Malformed and schema drift"** — Baseline `{Instances: 1}`.
  - Steps:
    1. store the instance counters;
    2. produce 100 good + 1 `NoInterval` + 1 `BrokenJSON` + 1 `UnknownField` + 1 `RenamedID`;
    3. WaitUntil [60 s] lag 0.
  - Expect:
    - `DBRows == 101`;
    - `FilteredDrop` delta == 1;
    - `Failed["deserialization_error"]` delta == 1;
    - `Failed["missing_required_fields"]` delta == 1;
    - the state was never PAUSED (track during the wait).
  - Reveal:
    - without the `intervalLength` text, `IntervalFilter` drops the record before parsing;
    - invalid JSON → `deserialization_error`; a renamed id → `missing_required_fields`;
    - all are committed and gone (only a counter and a log line remain), while an unknown field is ignored (`FAIL_ON_UNKNOWN_PROPERTIES=false`, `JacksonConfiguration.java:44`);
    - contrast with 3.2: errors at parse time drop the record, errors at DB time stop the pipeline. Alert on drop counters.
  - Confirm:
    1. "Upstream đổi tên một field bắt buộc lúc 2h sáng — bạn phát hiện bằng cách nào, sau bao lâu?"
    2. "Vì sao thêm field mới thì an toàn còn đổi tên thì không?"
- [ ] **Step 5: Verify.** `go run ./cmd/lab -run 3.1`, `3.2`, `3.3`, `3.4` → each exits 0. Record the first-PAUSED seconds and the DLQ batch size under "Measured" in `docs/design.md`. Take a Playwright MCP screenshot of `http://localhost:7070` during 3.1 showing PAUSED in red.
- [ ] **Step 6: Commit** `feat(scenario): M3 failure handling — DB down, poison pill, DLQ, malformed data`.

---

## Final verification

- `go vet ./... && go test ./...` → PASS. `make itest` with the stack up → PASS.
- `for s in 0.1 0.2 3.1 3.2 3.3 3.4; do go run ./cmd/lab -run $s || echo FAIL $s; done` → no FAIL lines.
- `docker ps --format '{{.Names}}'` after a full run: the user's non-lab containers still running and untouched.
