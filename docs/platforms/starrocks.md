# StarRocks

Bruin supports StarRocks as a native SQL data platform through its MySQL-compatible FE query port. This lets you build and materialize StarRocks tables, run data quality checks, and use sensors — in addition to using StarRocks as an [ingestion source or destination](/ingestion/starrocks).

StarRocks needs specific DDL to create tables (a table model, a `DISTRIBUTED BY` clause, `PROPERTIES`, and optionally `PARTITION BY`), which the generic MySQL adapter does not emit — the `starrocks.*` asset types below generate it.

## Connection

Add a StarRocks entry under `connections` in `.bruin.yml`. The same connection is used for both native assets and ingestion.

```yaml
connections:
  starrocks:
    - name: "starrocks-default"
      username: "root"
      host: "starrocks.example.com"
      port: 9030                    # optional, defaults to 9030 (FE MySQL protocol port)
      password: "XXXXXXXXXX"        # optional
      database: "analytics"         # optional
      ssl: "true"                   # optional — "true" or "skip-verify"
```

## StarRocks Assets

### `starrocks.sql`

Executes a materialized StarRocks SQL asset.

```bruin-sql
/* @bruin
name: analytics.example
type: starrocks.sql
materialization:
    type: table
@bruin */

SELECT
    id,
    country,
    name
FROM staging.customers
```

Supported table materialization strategies:

- `create+replace`
- `append`
- `delete+insert`
- `merge`
- `truncate+insert`
- `time_interval`
- `ddl`

View materialization is also supported. For local and single-node StarRocks clusters, Bruin creates StarRocks-managed tables with `PROPERTIES ("replication_num" = "1")`. Atomic replacements (`create+replace`, `delete+insert`, `truncate+insert`, seeds) use StarRocks' `ALTER TABLE ... SWAP WITH ...`.

#### Table layout

Distribution and partitioning are taken from the standard materialization fields,
so they work the same way as on the other platforms:

- `materialization.cluster_by` → `DISTRIBUTED BY HASH(...)` (defaults to the key columns)
- `materialization.partition_by` → `PARTITION BY (...)` (a column or expression such as `date_trunc('day', event_date)`)
- `starrocks.order_by: [col, ...]` → `ORDER BY (...)` (the table's sort key)

`partition_by` is emitted verbatim so that expressions work, so it is **not** automatically backtick-quoted. If you partition on a single column whose name is a StarRocks reserved word (e.g. `date`, `value`, `key`), quote it yourself: `partition_by: "`date`"`.

StarRocks-specific layout that has no materialization equivalent is declared under `starrocks`:

```yaml
materialization:
  type: table
  strategy: create+replace
  cluster_by: [account_id]          # DISTRIBUTED BY HASH(account_id)
  partition_by: event_date          # PARTITION BY (event_date)

starrocks:
  table_model: primary_key          # duplicate_key | unique_key | primary_key
  buckets: 8                         # defaults to 1
  order_by: [event_date]             # ORDER BY (event_date)
  properties:
    replication_num: "1"
```

When any of these are set (or `columns` are declared), Bruin emits a typed `CREATE TABLE` with the key clause, `PARTITION BY`, `DISTRIBUTED BY HASH(...) BUCKETS`, `ORDER BY`, and `PROPERTIES`. Otherwise it falls back to `CREATE TABLE ... AS SELECT`. Setting `order_by` switches the asset onto the typed `CREATE TABLE` path (like `cluster_by`, `table_model`, or `buckets` do). Note the typed path requires `columns` to be declared.

#### Merge materialization

StarRocks has no `MERGE INTO` statement. Bruin implements `merge` with a StarRocks **PRIMARY KEY** table: matching rows are replaced and new rows inserted by a plain `INSERT`. Bruin infers `starrocks.table_model: primary_key` for merge assets, creates the table if it does not exist, and upserts on the primary key.

Merge assets must declare typed `columns` and at least one `primary_key` column. Per-column `merge_sql` expressions are not supported (StarRocks upserts whole rows on the primary key) — encode that logic in the asset query instead.

```bruin-sql
/* @bruin
name: analytics.accounts
type: starrocks.sql
materialization:
    type: table
    strategy: merge

columns:
  - name: account_id
    type: BIGINT
    primary_key: true
  - name: status
    type: VARCHAR(32)
    update_on_merge: true
@bruin */

SELECT account_id, status
FROM staging.accounts
```

#### Materialized views

Setting `starrocks.materialization.type: materialized_view` on a `starrocks.sql` asset materializes it as a StarRocks **materialized view** instead of a table. This is intentionally a StarRocks-local option: the shared `materialization.type` values remain `table` and `view`.

The `starrocks.materialization` block is atomic when pipeline defaults are applied. An asset inherits the complete pipeline-level block only when it omits the block entirely. If an asset declares the block, it replaces the inherited block; setting its `type` to `table` or `view` is therefore an explicit opt-out from an inherited materialized-view default.

Bruin supports both kinds of StarRocks materialized view: asynchronous (the default) and synchronous rollups.

##### Asynchronous materialized views

An async MV is a standalone object with its own distribution, partitioning, sort key, refresh schedule, and properties:

- `materialization.cluster_by` → `DISTRIBUTED BY HASH(...)`, with `starrocks.buckets` adding `BUCKETS n`
- `materialization.partition_by` → `PARTITION BY (...)`
- `starrocks.order_by` → `ORDER BY (...)`, the MV's sort key
- `starrocks.materialization.refresh` → the `REFRESH` clause:
  - `trigger: immediate | deferred` → `REFRESH IMMEDIATE` / `REFRESH DEFERRED` (omitted defaults to StarRocks' own default, `IMMEDIATE`)
  - `mode: async | manual` → `... ASYNC` / `... MANUAL`
  - `start` / `every` (async mode only) → `START('...') EVERY (INTERVAL n UNIT)`; `every` is `"<count> <unit>"`, e.g. `1 day` → `EVERY (INTERVAL 1 DAY)`
  - `refresh_on_run` — whether re-running the asset issues a `REFRESH MATERIALIZED VIEW` (see "Run behavior" below)
- `starrocks.properties` — rendered as `PROPERTIES (...)`. Unlike tables, Bruin does **not** inject a default `replication_num` for materialized views; only the properties you set are emitted.

An async materialized view must set `cluster_by` and/or `starrocks.materialization.refresh`. When `buckets` is set, `cluster_by` is required.

```bruin-sql
/* @bruin
name: analytics.daily_active_users
type: starrocks.sql
materialization:
  partition_by: date_trunc('day', event_date)   # -> PARTITION BY (date_trunc('day', event_date))
  cluster_by: [user_id]                          # -> DISTRIBUTED BY HASH(user_id)
starrocks:
  materialization:
    type: materialized_view
    mode: async              # async (default) | sync
    refresh:
      trigger: deferred      # immediate | deferred   -> REFRESH DEFERRED
      mode: async            # async | manual          -> ASYNC
      start: "2025-01-01 10:00:00"                # -> START('2025-01-01 10:00:00')
      every: 1 day           # DAY | HOUR | MINUTE | SECOND
      refresh_on_run: false  # (optional) override re-run REFRESH trigger
  buckets: 8                                       # -> BUCKETS 8
  order_by: [event_date, user_id]                 # -> ORDER BY (event_date, user_id)
  properties:
    partition_refresh_number: "4"
    replication_num: "1"
@bruin */

SELECT event_date, user_id, count(*) AS events
FROM analytics.raw_events
GROUP BY event_date, user_id
```

This renders as:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`daily_active_users`
DISTRIBUTED BY HASH(`user_id`) BUCKETS 8
REFRESH DEFERRED ASYNC START('2025-01-01 10:00:00') EVERY (INTERVAL 1 DAY)
PARTITION BY (date_trunc('day', event_date))
ORDER BY (`event_date`, `user_id`)
PROPERTIES ("partition_refresh_number" = "4", "replication_num" = "1")
AS
SELECT event_date, user_id, count(*) AS events
FROM analytics.raw_events
GROUP BY event_date, user_id;
```

##### Synchronous (rollup) materialized views

Setting `starrocks.materialization.mode: sync` creates a single-table StarRocks **rollup** materialized view instead of an async one. Sync MVs cannot set `cluster_by`, `partition_by`, `order_by`, `buckets`, or `refresh` — StarRocks manages those details for rollups automatically.

```bruin-sql
/* @bruin
name: analytics.sales_rollup
type: starrocks.sql
starrocks:
  materialization:
    type: materialized_view
    mode: sync
  properties:
    replication_num: "1"
@bruin */

SELECT store_id, sum(amount) AS total
FROM analytics.sales
GROUP BY store_id
```

This renders as:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`sales_rollup`
PROPERTIES ("replication_num" = "1")
AS
SELECT store_id, sum(amount) AS total
FROM analytics.sales
GROUP BY store_id;
```

##### Run behavior

- **First run / normal re-run:** Bruin emits `CREATE MATERIALIZED VIEW IF NOT EXISTS ...`, which is a no-op if the MV already exists. When `refresh_on_run` resolves to `true`, Bruin follows it with a blocking `REFRESH MATERIALIZED VIEW <name> WITH SYNC MODE;` so a completed `bruin run` means the MV's data is up to date.
- **`refresh_on_run` defaults:** `true` for `starrocks.materialization.refresh.mode: manual` (a manual-mode MV never refreshes itself, so Bruin's run is the only trigger), `false` for async refresh or when no refresh block is set. An explicit `refresh_on_run: true`/`false` always overrides the default.
- **`--full-refresh`:** Bruin issues `DROP MATERIALIZED VIEW IF EXISTS <name>;` followed by `CREATE MATERIALIZED VIEW <name> ...` (no `IF NOT EXISTS`), rebuilding the MV from scratch.

### `starrocks.seed`

Loads a local CSV file into StarRocks.

```yaml
name: analytics.seed_contacts
type: starrocks.seed

columns:
  - name: name
    type: STRING
  - name: channel
    type: STRING

parameters:
  path: seed.csv
  file_type: csv
```

### `starrocks.sensor.table`

Waits until a StarRocks table exists.

```yaml
name: analytics.wait_for_daily_summary
type: starrocks.sensor.table
parameters:
    table: analytics.daily_summary
    poke_interval: 30
```

### `starrocks.sensor.query`

Waits until a StarRocks query returns at least one row.

```yaml
name: analytics.wait_for_orders
type: starrocks.sensor.query
parameters:
    query: SELECT 1 FROM analytics.orders WHERE order_date = "{{ end_date }}" LIMIT 1
```

### `starrocks.source`

Defines an existing StarRocks table as a source asset for lineage and documentation.

```yaml
name: analytics.raw_orders
type: starrocks.source

columns:
  - name: order_id
    type: BIGINT
```
