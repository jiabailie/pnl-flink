# FIFO PnL Platform Design

## Overview

This project will contain three runtime services coordinated by Docker Compose:

```text
transaction-generator -> Kafka topic -> pnl-calculator (Flink) -> PostgreSQL -> dashboard
```

The purpose is to simulate transactions, calculate FIFO profit and loss over multiple reporting windows, persist the results, and expose a simple dashboard for inspection.

## Modules

The repository will be organized as a multi-module Maven build:

- `common`
  Shared model classes, JSON serialization helpers, window definitions, and utility code.
- `transaction-generator`
  Java producer application that generates transaction events and publishes them to Kafka.
- `pnl-calculator`
  Flink streaming job that consumes transactions, maintains FIFO lot state, computes realized and unrealized PnL across configured windows, and writes snapshots to PostgreSQL.
- `dashboard`
  Lightweight Spring Boot web application that queries PostgreSQL and renders the current PnL results.

## Runtime Components

Docker Compose will provide:

- Kafka broker
- Redpanda Console for Kafka inspection
- Flink JobManager
- Flink TaskManager
- PostgreSQL
- dashboard

The transaction generator will be runnable both locally and via Docker Compose.

## Transaction Model

Each generated event will represent one executed trade:

- `tradeId`
- `accountId`
- `symbol`
- `side`
- `quantity`
- `price`
- `eventTime`

Kafka message key:

```text
accountId:symbol
```

This keeps related transactions partition-local for keyed Flink processing.

## FIFO PnL Logic

The calculator will maintain per `accountId + symbol` keyed state.

### Open lots

For buys, the calculator appends a new FIFO lot:

- buy quantity
- buy price
- buy timestamp
- remaining quantity

### Realized PnL

For sells, the calculator consumes open lots in FIFO order.

For each matched quantity:

```text
realized_pnl = matched_quantity * (sell_price - lot_buy_price)
```

If a sell quantity exceeds total available long quantity, the first version will clamp matching to available quantity and avoid creating short inventory.

### Unrealized PnL

Unrealized PnL is based on the remaining open FIFO lots marked against the latest observed price.

```text
unrealized_pnl = sum(remaining_qty * (last_price - lot_price))
```

## Reporting Windows

The project will compute PnL views for these windows:

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

Note:

- the user requested both `1m` and `1m`; to avoid ambiguity the design will use `1m` for one minute and `1mo` for one month

### Window semantics

Each window result will represent PnL as-of now for events whose `eventTime` falls within the window boundary.

The first implementation will support this by:

- keeping a rolling event ledger in keyed Flink state
- pruning events older than the largest required window when possible
- recomputing per-window FIFO views from the retained event history for the current key

This approach is simpler and correct for a demo, though not the most memory-efficient design.

## PostgreSQL Storage

The calculator will persist a latest snapshot table with one row per:

- `account_id`
- `symbol`
- `window_name`

Planned fields:

- `account_id`
- `symbol`
- `window_name`
- `position_qty`
- `avg_cost`
- `last_price`
- `realized_pnl`
- `unrealized_pnl`
- `total_pnl`
- `updated_at`

Primary key:

```text
(account_id, symbol, window_name)
```

## Dashboard

The dashboard will:

- connect to PostgreSQL
- query the snapshot table
- render a simple HTML table of latest results
- allow filtering by account, symbol, and window

The first version will optimize for usability and simplicity rather than advanced charting.

## Operational Flow

1. `transaction-generator` produces random trades into Kafka.
2. `pnl-calculator` reads from Kafka and keys by `accountId:symbol`.
3. For each key, the Flink job updates the FIFO lot ledger and calculates per-window PnL snapshots.
4. The job upserts results into PostgreSQL.
5. The dashboard reads PostgreSQL and displays current PnL rows.
6. Redpanda Console is used to inspect Kafka topic traffic.

## First-Version Constraints

The first implementation will intentionally keep scope bounded:

- long-only inventory
- latest snapshot storage, not full historical materialization
- simple HTML dashboard, no SPA frontend
- no authentication
- no schema registry
- minimal operational hardening

## Next Steps After Scaffold

After the first working version, the next useful improvements would be:

- deterministic test fixtures for FIFO matching
- support for shorts and position flips
- historical snapshot storage
- richer dashboard charts
- dead-letter handling for malformed Kafka records
- CI workflow for multi-module build
