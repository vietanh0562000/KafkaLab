# kafka-lab

A local lab for learning Kafka by breaking things: 3 brokers, 1 KRaft controller, TimescaleDB and the real
`gap-traffic-data-processing` service, driven by a Go app with a live dashboard and guided scenarios.
Design: `docs/design.md`.

## Run

```bash
make jar    # builds ../gap-traffic-data-processing and copies the jar to build/service.jar
make up     # starts the cluster, kafka-ui, TimescaleDB and one service instance
make run    # lab-control on http://localhost:7070
make down   # removes everything, including the DB volume
```

| Port | What |
|---|---|
| 7070 | lab-control (dashboard, scenarios) |
| 18080 | kafka-ui |
| 15432 | TimescaleDB (`postgres` / `postgres`) |
| 19092–19094 | brokers, for clients on the host |
| 18181–18184 | management port of service instances 1–4 |

Every lab container is named `lab-*` and belongs to compose project `kafkalab`; lab-control refuses to touch
anything else.
