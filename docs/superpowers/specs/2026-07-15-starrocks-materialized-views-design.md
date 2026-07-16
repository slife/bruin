# StarRocks Materialized Views & Order-By Support — Design

**Date:** 2026-07-15
**Status:** Approved (brainstorming), pending implementation plan
**Scope owner:** Bruin StarRocks platform (`pkg/starrocks`)

## 1. Goal

Extend Bruin's StarRocks support to cover StarRocks materialized views, closing
the gap with the [dbt-starrocks](https://github.com/StarRocks/dbt-starrocks)
plugin and the
[StarRocks `CREATE MATERIALIZED VIEW` reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW/).

Concretely, add:

1. **Asynchronous materialized views** — standalone objects with their own
   storage, distribution, partitioning, sort key, refresh scheme, and
   properties.
2. **Synchronous (rollup) materialized views** — single-table rollups with no
   distribution / partitioning / refresh / order-by.
3. **Refresh control** — `REFRESH {IMMEDIATE | DEFERRED} {ASYNC [START(...)
   EVERY(...)] | MANUAL}`.
4. **`ORDER BY` sort key** on both materialized views and regular tables (a
   feature StarRocks supports today that Bruin does not model at all).

This goes one step beyond dbt-starrocks, which only builds asynchronous MVs.

## 2. Background: what already exists

`pkg/starrocks/` is a complete native platform (not a reuse of `pkg/mysql`; it
shares only the `go-sql-driver/mysql` wire driver). Its nearest structural
sibling is `pkg/doris/` (Doris is a StarRocks fork).

Current materialization support (`pkg/starrocks/materialization.go`):

- Types: `table`, `view` (plain view only).
- Table strategies: `create+replace`, `append`, `delete+insert`,
  `truncate+insert`, `merge`, `time_interval`, `ddl`. SCD2 → `errorMaterializer`.
- The typed `CREATE TABLE` path already emits `DUPLICATE/UNIQUE/PRIMARY KEY`,
  `PARTITION BY`, `DISTRIBUTED BY HASH(...) BUCKETS n`, and `PROPERTIES(...)`.
- Config lives in `pipeline.StarRocksConfig{ TableModel, Buckets, Properties }`
  on the asset (`pkg/pipeline/pipeline.go:1195-1216`). Distribution is sourced
  from `materialization.cluster_by`; partitioning from
  `materialization.partition_by` — these are intentionally NOT duplicated into
  `StarRocksConfig`.

The single real gap versus the StarRocks/dbt feature set is **materialized
views** (Bruin has none), plus the absence of any `ORDER BY` sort-key modeling.

### Key relevant file references

| Concern | Location |
|---|---|
| Materialization struct + type/strategy enums | `pkg/pipeline/pipeline.go:578-651` |
| Central dispatch `Render` + full-refresh override | `pkg/pipeline/materializer.go:23-49` |
| `StarRocksConfig` struct (`MarshalJSON`/`IsZero`) | `pkg/pipeline/pipeline.go:1195-1216` |
| `mergeStarRocksDefaults` | `pkg/pipeline/pipeline.go:3280-3297` |
| Variant/Jinja rendering of StarRocks config | `pkg/pipeline/variant.go:308,347,458-462` |
| YAML materialization parse | `pkg/pipeline/yaml.go:212-220,514-523` |
| SQL/Python comment materialization parse | `pkg/pipeline/comment.go:341-371` |
| StarRocks matMap + builders | `pkg/starrocks/materialization.go:23-53,269-352` |
| StarRocks operator wiring | `pkg/starrocks/operator.go:34-140` |
| Runtime operator registration | `cmd/run.go:2579-2604` |
| render / render-ddl registration | `cmd/render.go:285-286`, `cmd/render_ddl.go:225-226` |
| Golden-SQL tests | `pkg/starrocks/materialization_test.go` |
| Integration test pipelines | `integration-tests/cloud-integration-tests/starrocks/` |
| Platform docs | `docs/platforms/starrocks.md` |

## 3. Design decisions (settled during brainstorming)

| Decision | Choice |
|---|---|
| Refresh API shape | **Structured, validated sub-fields** (not a verbatim dbt-style string). |
| Sync vs async | **Support both**, as distinct objects (`starrocks.sync: true` selects the rollup form). |
| Run behavior for existing MV | **CREATE IF NOT EXISTS + optional REFRESH**; `--full-refresh` does DROP + CREATE. |
| Re-run refresh trigger | **Configurable per asset** (`starrocks.refresh.refresh_on_run`), defaulting to `true` for `mode: manual` and `false` for `mode: async`. |
| Overall scope | **MVs (async + sync) + `ORDER BY` sort key on regular tables.** No AGGREGATE model, partition_type, bitmap indexes, or `on_table_exists` in this effort. |
| Refresh execution mode | `REFRESH MATERIALIZED VIEW ... WITH SYNC MODE` by default (blocking, so a completed run means data is ready); overridable. |

## 4. User-facing configuration

Materialized views require **no new asset type**. Like `table` and `view`, an MV
is a `starrocks.sql` (or `type: starrocks`) asset with a new
`materialization.type: materialized_view`.

### 4.1 Async MV (default)

```yaml
/* @bruin
name: analytics.daily_active_users
type: starrocks.sql
materialization:
  type: materialized_view
  partition_by: date_trunc('day', event_date)   # -> PARTITION BY (date_trunc('day', event_date))
  cluster_by: [user_id]                          # -> DISTRIBUTED BY HASH(user_id)
starrocks:
  buckets: 8                                      # -> BUCKETS 8
  order_by: [event_date, user_id]                 # -> ORDER BY (event_date, user_id)
  refresh:
    trigger: deferred        # immediate | deferred   -> REFRESH DEFERRED
    mode: async              # async | manual          -> ASYNC
    start: "2025-01-01 10:00:00"                  # -> START('2025-01-01 10:00:00')
    every: 1 day             # -> EVERY (INTERVAL 1 DAY)
    refresh_on_run: false    # (optional) override re-run REFRESH trigger
  properties:
    partition_refresh_number: "4"
    replication_num: "1"
@bruin */

SELECT event_date, user_id, count(*) AS events
FROM analytics.raw_events
GROUP BY event_date, user_id
```

Generated on first run:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`daily_active_users`
DISTRIBUTED BY HASH(`user_id`) BUCKETS 8
PARTITION BY (date_trunc('day', event_date))
ORDER BY (`event_date`, `user_id`)
REFRESH DEFERRED ASYNC START('2025-01-01 10:00:00') EVERY (INTERVAL 1 DAY)
PROPERTIES ("partition_refresh_number" = "4", "replication_num" = "1")
AS
SELECT event_date, user_id, count(*) AS events
FROM analytics.raw_events
GROUP BY event_date, user_id;
```

### 4.2 Sync (rollup) MV

```yaml
/* @bruin
name: analytics.sales_rollup
type: starrocks.sql
materialization:
  type: materialized_view
starrocks:
  sync: true          # single-table rollup; no refresh/distribution/partition/order_by allowed
  properties:
    replication_num: "1"
@bruin */

SELECT store_id, sum(amount) AS total
FROM analytics.sales
GROUP BY store_id
```

Generated:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`sales_rollup`
PROPERTIES ("replication_num" = "1")
AS
SELECT store_id, sum(amount) AS total
FROM analytics.sales
GROUP BY store_id;
```

### 4.3 `ORDER BY` on a regular table

```yaml
materialization:
  type: table
  cluster_by: [id]        # -> DISTRIBUTED BY HASH(id)
starrocks:
  buckets: 4
  order_by: [created_at]  # -> ORDER BY (created_at)   (NEW)
```

## 5. Implementation design

### 5.1 Generic pipeline layer

**New materialization type** (`pkg/pipeline/pipeline.go`):

```go
MaterializationTypeMaterializedView MaterializationType = "materialized_view"
```

**Full-refresh override** (`pkg/pipeline/materializer.go:31-37`): today the
`create+replace` override fires only for `MaterializationTypeTable`. Extend it to
also fire for `MaterializationTypeMaterializedView`, so `--full-refresh` routes an
MV to its DROP+CREATE builder. The override remains gated by the existing `DDL`
and `RefreshRestricted` exceptions.

```go
if m.FullRefresh &&
    (mat.Type == MaterializationTypeTable || mat.Type == MaterializationTypeMaterializedView) {
    if mat.Strategy != MaterializationStrategyDDL &&
        (asset.RefreshRestricted == nil || !*asset.RefreshRestricted) {
        strategy = MaterializationStrategyCreateReplace
    }
}
```

This is generic; only StarRocks registers `materialized_view` entries in its
`matMap`, so no other platform's behavior changes. Any platform asked to
materialize `materialized_view` without a registered builder gets the existing
"unsupported materialization type - strategy combination" error.

**Parsing.** `materialization.type` is already parsed generically for both the
YAML path (`pkg/pipeline/yaml.go:514`) and the SQL/Python comment path
(`pkg/pipeline/comment.go:346`), lower-cased. `materialized_view` flows through
with no parser change. Only the StarRocks-specific config block (§5.2) needs new
parsing.

### 5.2 StarRocks config extensions

Extend `pipeline.StarRocksConfig` (`pkg/pipeline/pipeline.go:1199-1216`):

```go
type StarRocksConfig struct {
    TableModel string            `json:"table_model,omitempty" yaml:"table_model,omitempty" mapstructure:"table_model"`
    Buckets    int               `json:"buckets,omitempty" yaml:"buckets,omitempty" mapstructure:"buckets"`
    Properties map[string]string `json:"properties,omitempty" yaml:"properties,omitempty" mapstructure:"properties"`

    // NEW
    OrderBy []string          `json:"order_by,omitempty" yaml:"order_by,omitempty" mapstructure:"order_by"`
    Sync    bool              `json:"sync,omitempty" yaml:"sync,omitempty" mapstructure:"sync"`
    Refresh *StarRocksRefresh `json:"refresh,omitempty" yaml:"refresh,omitempty" mapstructure:"refresh"`
}

type StarRocksRefresh struct {
    Trigger      string `json:"trigger,omitempty" yaml:"trigger,omitempty" mapstructure:"trigger"`   // immediate | deferred
    Mode         string `json:"mode,omitempty" yaml:"mode,omitempty" mapstructure:"mode"`             // async | manual
    Start        string `json:"start,omitempty" yaml:"start,omitempty" mapstructure:"start"`
    Every        string `json:"every,omitempty" yaml:"every,omitempty" mapstructure:"every"`
    RefreshOnRun *bool  `json:"refresh_on_run,omitempty" yaml:"refresh_on_run,omitempty" mapstructure:"refresh_on_run"`
}
```

Also update:

- `StarRocksConfig.MarshalJSON` / `IsZero` to account for the new fields.
- `mergeStarRocksDefaults` (`pkg/pipeline/pipeline.go:3280-3297`) so
  `pipeline.yml` `defaults.starrocks` can supply `order_by` / `sync` / `refresh`.
- `pkg/pipeline/variant.go` (`renderAssetStrings`, around 458-462) so
  `order_by`, `refresh.start`, `refresh.every`, and `properties` values are
  Jinja-rendered (they may contain templated dates).

`refresh` is a pointer so its absence is distinguishable from a zero value —
needed for the sync-MV validation (a sync MV must not carry `refresh`) and for
resolving the `refresh_on_run` default.

### 5.3 StarRocks matMap

Add a `materialized_view` type to `matMap`
(`pkg/starrocks/materialization.go:30-53`):

```go
pipeline.MaterializationTypeMaterializedView: {
    pipeline.MaterializationStrategyNone:          buildMaterializedView,
    pipeline.MaterializationStrategyCreateReplace: buildMaterializedViewFullRefresh,
    // all other strategies -> errorMaterializer
},
```

- `None` → normal run path (`CREATE ... IF NOT EXISTS` + optional refresh).
- `CreateReplace` → the strategy the full-refresh override rewrites to; emits
  `DROP MATERIALIZED VIEW IF EXISTS ...; CREATE MATERIALIZED VIEW ...`.
- Every other strategy (append, merge, delete+insert, etc.) →
  `errorMaterializer`, mirroring how the `view` type already restricts strategies.

### 5.4 MV SQL generation

A shared internal builder assembles the `CREATE MATERIALIZED VIEW` body; the two
matMap entries differ only in the `IF NOT EXISTS` / `DROP ... ;` prefix.

**Async MV** — clause order is fixed by StarRocks grammar and MUST be:

```
CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name>
DISTRIBUTED BY HASH(<cluster_by>) [BUCKETS <buckets>]
PARTITION BY (<partition_by>)
ORDER BY (<order_by>)
REFRESH [IMMEDIATE|DEFERRED] [ASYNC [START('...') EVERY (INTERVAL n unit)] | MANUAL]
PROPERTIES (...)
AS <query>
```

Rules:

- **Distribution:** from `materialization.cluster_by`. If empty, omit
  `DISTRIBUTED BY` entirely (StarRocks defaults to random bucketing). `BUCKETS`
  emitted only when `starrocks.buckets > 0`.
- **Partition:** from `materialization.partition_by`, wrapped as
  `PARTITION BY (<expr>)`. Free-form expression (supports `date_trunc(...)`
  etc.), consistent with the existing table path.
- **Order by:** from `starrocks.order_by`, backtick-quoted, `ORDER BY (a, b)`.
- **Refresh:** assembled from the structured `refresh` block (§5.5).
- **Properties:** `buildStarRocksProperties`-style rendering. Unlike tables,
  **do not inject a default `replication_num`** for MVs (StarRocks MV defaults
  differ; only emit what the user supplies). Emit `PROPERTIES(...)` only when
  non-empty.
- **StarRocks constraint:** an async MV must have `DISTRIBUTED BY` **or**
  `REFRESH` present, else StarRocks parses it as a *sync* MV. Validation (§5.6)
  requires that a non-sync MV specify at least one of `cluster_by` or `refresh`.

**Sync MV** (`starrocks.sync: true`): emit only

```
CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name>
[PROPERTIES (...)]
AS <query>
```

No distribution / partition / order-by / refresh clauses (StarRocks forbids them
on sync MVs; validation enforces their absence).

Reuse existing helpers: `quoteIdentifier`, `quoteColumnNames`,
`buildStarRocksProperties` (or an MV variant that skips the default
`replication_num`).

### 5.5 Refresh clause assembly

From `StarRocksRefresh`:

| Field | Value | Emitted |
|---|---|---|
| `trigger` | `immediate` | `IMMEDIATE` |
| `trigger` | `deferred` | `DEFERRED` |
| `trigger` | (unset) | (omitted — StarRocks defaults to IMMEDIATE) |
| `mode` | `manual` | `MANUAL` |
| `mode` | `async` (no start/every) | `ASYNC` |
| `mode` | `async` + `start`/`every` | `ASYNC START('<start>') EVERY (INTERVAL <every>)` |

`every: "1 day"` → `EVERY (INTERVAL 1 DAY)`. The value is parsed as
`<n> <unit>` and upper-cased for the unit; a single bare token is treated as an
error (needs both count and unit). Full `REFRESH` clause = `REFRESH` +
`[trigger]` + `[mode/schedule]`. If the entire `refresh` block is absent on an
async MV, omit the `REFRESH` clause and rely on `DISTRIBUTED BY` to keep it an
async MV (validation guarantees one of the two is present).

### 5.6 Run behavior & refresh-on-run

The StarRocks `BasicOperator` (`pkg/starrocks/operator.go`) runs the rendered
statements via `conn.RunQueryWithoutResult`. The materializer output already
encodes the DDL; the **re-run refresh trigger** is the one behavior that depends
on runtime state (does the MV exist?) and refresh config.

Approach — keep the materializer stateless and push the refresh trigger into the
rendered statement list:

- **Normal run (`None`):** emit `CREATE MATERIALIZED VIEW IF NOT EXISTS ...`.
  Then, when `refresh_on_run` resolves true, append
  `REFRESH MATERIALIZED VIEW <name> WITH SYNC MODE;`.
  - `CREATE IF NOT EXISTS` is a no-op when the MV already exists, so first-run
    and re-run share one code path and PCT/incremental refresh state is
    preserved.
  - `WITH SYNC MODE` makes the refresh blocking, so a completed `bruin run`
    means the MV data is materialized before downstream assets read it.
- **`--full-refresh` (`CreateReplace`):** emit
  `DROP MATERIALIZED VIEW IF EXISTS <name>;` then `CREATE MATERIALIZED VIEW
  <name> ...`. No separate refresh needed — `trigger: immediate` (the StarRocks
  default) refreshes on create; if `trigger: deferred`, the user opted out of an
  immediate refresh and we honor that.

**`refresh_on_run` default resolution:**

| `refresh.mode` | `refresh_on_run` unset → default | Rationale |
|---|---|---|
| `manual` | `true` | The MV never self-refreshes; Bruin's pipeline schedule is the intended driver. |
| `async` | `false` | The MV self-refreshes on its own schedule; re-run is a no-op. |
| (no refresh block / sync MV) | `false` | Nothing to trigger. |

An explicit `refresh_on_run` always wins over the default.

> **Note on `WITH SYNC MODE`:** default is synchronous (blocking) so downstream
> correctness holds. This can be long for large MVs. It is acceptable for this
> iteration; a future `refresh.sync_mode: false` (→ `WITH ASYNC MODE`) can be
> added without breaking changes if needed. Not in scope now.

### 5.7 `ORDER BY` on regular tables

Add an `ORDER BY (<order_by>)` clause, sourced from `StarRocksConfig.OrderBy`, to:

- the typed `buildCreateTableStatement` (`pkg/starrocks/materialization.go:290-352`),
  positioned after `DISTRIBUTED BY ... BUCKETS n` and before `PROPERTIES`
  (StarRocks CREATE TABLE clause order: `... distribution_desc → ORDER BY →
  PROPERTIES`).
- `buildDDLQuery`, in the same position.

When `OrderBy` is empty, the clause is omitted (unchanged output for existing
assets). Emitting a non-empty `order_by` will flip a plain `create+replace`
into the typed-create path — extend `requiresTypedCreateTable`
(`pkg/starrocks/materialization.go:269-275`) to include
`len(asset.StarRocks.OrderBy) > 0`.

### 5.8 Validation

Add StarRocks-specific validation (lint rule in `pkg/lint`, and/or fail-fast in
the materializer) covering:

- `materialization.type: materialized_view` is only valid for StarRocks asset
  types (other platforms already error via the generic dispatch, but a clear
  lint message is better UX).
- `refresh.trigger ∈ {immediate, deferred}` (or empty).
- `refresh.mode ∈ {async, manual}` (or empty).
- `refresh.start` / `refresh.every` require `mode: async` (illegal with
  `manual`); `every` must parse as `<count> <unit>`.
- **Sync MV** (`sync: true`) must NOT set `refresh`, `cluster_by`,
  `partition_by`, or `order_by`.
- **Async MV** must specify at least one of `cluster_by` (distribution) or
  `refresh` — otherwise StarRocks silently reinterprets it as a sync MV.
- `order_by` may be combined with `table` and `materialized_view` types only.

### 5.9 Command wiring

No new operators or asset types. The existing `starrocks.sql` operator
(`cmd/run.go:2584`), `render` (`cmd/render.go:285-286`), and `render-ddl`
(`cmd/render_ddl.go:225-226`) registrations already route through
`starrocks.NewMaterializer`, so MV output flows through them unchanged once the
matMap has the new entries.

## 6. Testing strategy

### 6.1 Unit tests (`pkg/starrocks/materialization_test.go`)

Table-driven golden-SQL assertions (existing pattern; deterministic temp-table
names via `flag.Lookup("test.v")` are already handled). Cases:

- Async MV: minimal (distribution only); with partition_by; with order_by; with
  each refresh form (`manual`; `async`; `async` + start/every;
  `immediate`/`deferred` combinations); with properties.
- Async MV `--full-refresh`: `DROP ... ; CREATE ...`.
- Refresh-on-run: `manual` default appends `REFRESH ... WITH SYNC MODE`;
  `async` default does not; explicit `refresh_on_run` overrides both.
- Sync MV: minimal; with properties; error when refresh/cluster_by/partition_by/
  order_by set.
- Regular table + `order_by`: create+replace and DDL paths.
- Validation errors: bad trigger, bad mode, start/every with manual, async MV
  missing both distribution and refresh.

Also extend `pkg/pipeline` tests for `StarRocksConfig` MarshalJSON/IsZero and
`mergeStarRocksDefaults` with the new fields.

### 6.2 Integration tests

New pipelines under `integration-tests/cloud-integration-tests/starrocks/`:

- `materialized-view-async/` — async MV with partition + distribution + refresh.
- `materialized-view-sync/` — sync rollup MV.
- (optionally) extend an existing table pipeline with `order_by`.

Run via the StarRocks cloud integration module; refresh expectations with
`make refresh-integration-expectations` and inspect the diff.

### 6.3 Required checks (per CLAUDE.md)

`make format` and `make test` must pass before completion. Also update
`integration-tests/expectations/expected_connections_schema.json` only if the
connection schema changes (it should not — no `StarRocksConnection` changes).

## 7. Documentation

Update `docs/platforms/starrocks.md`:

- New "Materialized views" section: async vs sync, the `materialization.type:
  materialized_view` flag, the `starrocks.refresh` block, `order_by`, and MV
  `properties`.
- Document run behavior (create-if-not-exists + refresh_on_run, `--full-refresh`
  = drop+recreate) and the `WITH SYNC MODE` default.
- Document `order_by` on regular tables.

## 8. Out of scope (explicitly)

- `ALTER MATERIALIZED VIEW` (StarRocks and dbt-starrocks both drop-and-recreate;
  no in-place alter).
- Table extras: `AGGREGATE` key model, `partition_type` RANGE/LIST/Expr,
  bitmap/`indexes`, `on_table_exists`.
- `WITH ASYNC MODE` refresh execution (SYNC only for now; documented seam).
- Sync-MV aggregate-function rewrite validation (StarRocks enforces this
  server-side; Bruin passes the query through).

## 9. File-change summary

| File | Change |
|---|---|
| `pkg/pipeline/pipeline.go` | Add `MaterializationTypeMaterializedView`; extend `StarRocksConfig` (+ `StarRocksRefresh`); update `MarshalJSON`/`IsZero`; extend `mergeStarRocksDefaults`. |
| `pkg/pipeline/materializer.go` | Extend full-refresh override to `materialized_view`. |
| `pkg/pipeline/variant.go` | Jinja-render new StarRocks config fields. |
| `pkg/starrocks/materialization.go` | Add MV builders + matMap entries; `ORDER BY` on table/DDL paths; extend `requiresTypedCreateTable`; refresh-clause assembly. |
| `pkg/starrocks/operator.go` | (If needed) refresh-on-run statement handling. |
| `pkg/lint/rules.go` (or new rule) | MV/refresh/sync/order_by validation. |
| `pkg/starrocks/materialization_test.go` | Golden-SQL cases. |
| `pkg/pipeline/*_test.go` | Config marshaling + defaults-merge cases. |
| `integration-tests/cloud-integration-tests/starrocks/` | New MV test pipelines. |
| `docs/platforms/starrocks.md` | MV + order_by documentation. |
