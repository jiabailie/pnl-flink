# FIFO PnL Flink Platform

This project contains a local end-to-end FIFO PnL demo built around Kafka, Flink, PostgreSQL, and a lightweight dashboard.

Runtime flow:

```text
transaction-generator -> Kafka topic `transactions` -> pnl-calculator -> PostgreSQL -> dashboard
```

## Modules

- `common`
  Shared models, JSON serialization, and reporting window definitions.
- `transaction-generator`
  Java producer that generates random transactions and publishes them to Kafka.
- `pnl-calculator`
  Flink job that consumes transactions, applies FIFO matching, calculates PnL for multiple windows, and upserts results into PostgreSQL.
- `dashboard`
  Simple HTTP service that reads PostgreSQL and renders the latest snapshot rows.

## Reporting Windows

The calculator currently emits snapshots for:

- `1m`
- `5m`
- `30m`
- `1h`
- `6h`
- `12h`
- `1d`
- `1w`
- `1mo`
- `6mo`
- `1y`
- `5y`
- `10y`

## Prerequisites

You need:

- Java 17
- Maven 3.9+
- Docker and Docker Compose

## Build

From the repository root:

```bash
mvn clean package
```

Expected build outputs include:

- `transaction-generator/target/transaction-generator-1.0-SNAPSHOT.jar`
- `pnl-calculator/target/pnl-calculator-1.0-SNAPSHOT.jar`
- `dashboard/target/dashboard-1.0-SNAPSHOT.jar`

## Start The Stack

Start the infrastructure and long-running services:

```bash
docker compose up -d
```

This brings up:

- Kafka on `localhost:9092`
- Redpanda Console on `http://localhost:8080`
- PostgreSQL on `localhost:5432`
- Flink JobManager on `http://localhost:8081`
- Dashboard on `http://localhost:8088`

Check container status:

```bash
docker compose ps
```

## Submit The Flink Job

After the project is built and the containers are up, submit the calculator job into Flink:

```bash
docker compose exec jobmanager flink run -d /opt/flink/usrlib/pnl-calculator.jar
```

To verify the job is running:

```bash
docker compose exec jobmanager flink list
```

You can also inspect job status in the Flink UI:

```text
http://localhost:8081
```

## Start Generating Transactions

Run the generator as a one-off container:

```bash
docker compose run --rm transaction-generator
```

This will continuously print generated JSON transactions and publish them to Kafka topic `transactions`.

If you want to stop it, use `Ctrl+C`.

## Inspect Kafka Data

Open Redpanda Console:

```text
http://localhost:8080
```

Typical checks:

- confirm that topic `transactions` exists
- open the topic and inspect incoming messages
- verify that records are keyed by `accountId:symbol`

## View Calculated PnL

Open the dashboard:

```text
http://localhost:8088
```

The dashboard reads rows from PostgreSQL table `pnl_snapshots`.

You can filter by:

- account
- symbol
- window

## Query PostgreSQL Directly

The calculator writes latest snapshot rows into `pnl_snapshots`.

Example query:

```bash
psql postgresql://pnl:pnl@localhost:5432/pnl -c "select * from pnl_snapshots order by updated_at desc limit 50;"
```

## Recommended Startup Order

1. Build the project with `mvn clean package`
2. Start Docker Compose with `docker compose up -d`
3. Submit the Flink job with `docker compose exec jobmanager flink run -d /opt/flink/usrlib/pnl-calculator.jar`
4. Start the generator with `docker compose run --rm transaction-generator`
5. Inspect Kafka in Redpanda Console and results in the dashboard

## Stop The Stack

To stop all running services:

```bash
docker compose down
```

To stop and remove persisted PostgreSQL data as well:

```bash
docker compose down -v
```

## Main Environment Variables

### transaction-generator

- `KAFKA_BOOTSTRAP_SERVERS` default: `localhost:9092`
- `KAFKA_TOPIC` default: `transactions`
- `GENERATOR_ACCOUNT_COUNT` default: `5`
- `GENERATOR_SYMBOLS` default: `AAPL,MSFT,GOOG,TSLA,NVDA`
- `GENERATOR_INTERVAL_MS` default: `1000`

### pnl-calculator

- `KAFKA_BOOTSTRAP_SERVERS` default: `kafka:29092`
- `KAFKA_TOPIC` default: `transactions`
- `KAFKA_GROUP_ID` default: `pnl-calculator`
- `POSTGRES_URL` default: `jdbc:postgresql://postgres:5432/pnl`
- `POSTGRES_USER` default: `pnl`
- `POSTGRES_PASSWORD` default: `pnl`

### dashboard

- `POSTGRES_URL` default: `jdbc:postgresql://postgres:5432/pnl`
- `POSTGRES_USER` default: `pnl`
- `POSTGRES_PASSWORD` default: `pnl`
- `DASHBOARD_PORT` default: `8088`

## Notes

- The current implementation is designed as a demo, not a production deployment.
- FIFO PnL is recomputed per key from retained event history for each configured window.
- The first version is long-only and does not create short inventory.
- Results are stored as latest snapshots, not a full historical time series.

## Design Document

See [architecture.md](/Users/yangruiguo/Documents/pnl-flink/design/architecture.md).
