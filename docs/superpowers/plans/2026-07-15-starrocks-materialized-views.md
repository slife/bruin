# StarRocks Materialized Views & Order-By Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add StarRocks asynchronous + synchronous materialized views (with structured REFRESH control) and `ORDER BY` sort-key support to Bruin, closing the gap with dbt-starrocks and the full StarRocks `CREATE MATERIALIZED VIEW` feature set.

**Architecture:** Add a generic `materialized_view` materialization type to `pkg/pipeline`, then implement StarRocks-specific SQL generation in `pkg/starrocks/materialization.go`. Materialized views reuse the existing `starrocks.sql` asset type and operator — only the materializer's dispatch map and the pipeline config surface change. Distribution/partitioning reuse `materialization.cluster_by`/`partition_by` (existing StarRocks convention); MV-only options (`sync`, `refresh`, `order_by`) live on the per-asset `StarRocksConfig` struct.

**Tech Stack:** Go 1.x, `urfave/cli`, testify (`assert`/`require`), table-driven golden-SQL tests. Build tag `no_duckdb_arrow` for tests.

## Global Constraints

- **Format + test before done:** `make format` then `make test` must both pass (per `CLAUDE.md`). While iterating, target packages: `go test -tags="no_duckdb_arrow" ./pkg/starrocks/... ./pkg/pipeline/... ./pkg/lint/...`.
- **Backtick identifier quoting:** all identifiers/columns use existing `quoteIdentifier` / `quoteColumnName` helpers (MySQL-style backticks).
- **StarRocks clause order (async MV):** `CREATE MATERIALIZED VIEW [IF NOT EXISTS] name → DISTRIBUTED BY → PARTITION BY → ORDER BY → REFRESH → PROPERTIES → AS query`. This order is mandatory; StarRocks rejects other orderings.
- **StarRocks async-vs-sync rule:** an async MV must have `DISTRIBUTED BY` **or** `REFRESH`; a sync (rollup) MV must have none of distribution/partition/order-by/refresh.
- **Deterministic temp names in tests:** `temporaryTableRunID()` already returns `"abcefghi"` under `go test`; MV code needs no temp tables, but reuse the pattern if any arise.
- **No default `replication_num` for MVs:** unlike tables, MV `PROPERTIES` must emit only user-supplied keys (StarRocks MV defaults differ).
- **Enum/string values (verbatim):** materialization type string = `materialized_view`; refresh trigger ∈ {`immediate`, `deferred`}; refresh mode ∈ {`async`, `manual`}.
- **Refresh execution:** re-run refresh trigger uses `REFRESH MATERIALIZED VIEW <name> WITH SYNC MODE`.
- **`refresh_on_run` defaults:** unset → `true` when `mode: manual`, `false` when `mode: async` or no refresh block. Explicit value always wins.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `pkg/pipeline/pipeline.go` | Materialization type enum; `StarRocksConfig` + new `StarRocksRefresh`; `MarshalJSON`/`IsZero`; `mergeStarRocksDefaults` | Modify |
| `pkg/pipeline/materializer.go` | Full-refresh override — include `materialized_view` | Modify |
| `pkg/pipeline/yaml.go` | YAML `starrocks:` block struct + conversion to `StarRocksConfig` | Modify |
| `pkg/pipeline/variant.go` | Jinja rendering of new StarRocks config fields | Modify |
| `pkg/starrocks/materialization.go` | MV builders, matMap entries, refresh assembly, `ORDER BY` on tables, `requiresTypedCreateTable` | Modify |
| `pkg/lint/rules.go` | Register `materialized_view` type + MV/refresh/sync validation | Modify |
| `pkg/starrocks/materialization_test.go` | Golden-SQL MV + order_by cases | Modify |
| `pkg/pipeline/pipeline_test.go` (or existing) | `StarRocksConfig` marshal/IsZero/merge cases | Modify |
| `integration-tests/cloud-integration-tests/starrocks/test-pipelines/` | New MV pipelines | Create |
| `docs/platforms/starrocks.md` | MV + order_by docs | Modify |

---

## Task 1: Add `materialized_view` materialization type + full-refresh routing

**Files:**
- Modify: `pkg/pipeline/pipeline.go:582-586` (type enum)
- Modify: `pkg/pipeline/materializer.go:29-37` (full-refresh override)
- Test: `pkg/pipeline/materializer_test.go`

**Interfaces:**
- Produces: `pipeline.MaterializationTypeMaterializedView MaterializationType = "materialized_view"`. Later tasks reference this constant.

- [ ] **Step 1: Write the failing test**

Add to `pkg/pipeline/materializer_test.go`:

```go
func TestMaterializer_MaterializedViewFullRefreshOverride(t *testing.T) {
	t.Parallel()

	calls := map[MaterializationStrategy]bool{}
	record := func(s MaterializationStrategy) MaterializerFunc {
		return func(_ *Asset, _ string) (string, error) {
			calls[s] = true
			return "", nil
		}
	}
	m := &Materializer{
		FullRefresh: true,
		MaterializationMap: AssetMaterializationMap{
			MaterializationTypeMaterializedView: {
				MaterializationStrategyNone:          record(MaterializationStrategyNone),
				MaterializationStrategyCreateReplace: record(MaterializationStrategyCreateReplace),
			},
		},
	}
	asset := &Asset{Materialization: Materialization{Type: MaterializationTypeMaterializedView}}

	_, err := m.Render(asset, "SELECT 1")
	require.NoError(t, err)
	assert.True(t, calls[MaterializationStrategyCreateReplace], "full refresh should route MV to create+replace")
	assert.False(t, calls[MaterializationStrategyNone], "full refresh must not use the None builder")
}
```

Ensure the file imports `github.com/stretchr/testify/assert` and `github.com/stretchr/testify/require` (check existing imports; add if missing).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags="no_duckdb_arrow" ./pkg/pipeline/ -run TestMaterializer_MaterializedViewFullRefreshOverride -v`
Expected: FAIL — `MaterializationTypeMaterializedView` undefined (compile error).

- [ ] **Step 3: Add the enum constant**

In `pkg/pipeline/pipeline.go`, change the const block at lines 582-586 to:

```go
const (
	MaterializationTypeNone             MaterializationType = ""
	MaterializationTypeView             MaterializationType = "view"
	MaterializationTypeTable            MaterializationType = "table"
	MaterializationTypeMaterializedView MaterializationType = "materialized_view"
)
```

- [ ] **Step 4: Extend the full-refresh override**

In `pkg/pipeline/materializer.go`, change lines 29-37 to:

```go
	strategy := mat.Strategy
	if m.FullRefresh && (mat.Type == MaterializationTypeTable || mat.Type == MaterializationTypeMaterializedView) {
		// Only override to CreateReplace if strategy is not explicitly set to DDL
		// This strategy should never be overridden, even with full refresh
		// Also respect full refresh restriction - if true, don't drop/recreate the table
		if mat.Strategy != MaterializationStrategyDDL && (asset.RefreshRestricted == nil || !*asset.RefreshRestricted) {
			strategy = MaterializationStrategyCreateReplace
		}
	}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -tags="no_duckdb_arrow" ./pkg/pipeline/ -run TestMaterializer_MaterializedViewFullRefreshOverride -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/pipeline/pipeline.go pkg/pipeline/materializer.go pkg/pipeline/materializer_test.go
git commit -m "feat(pipeline): add materialized_view materialization type"
```

---

## Task 2: Extend `StarRocksConfig` with `OrderBy`, `Sync`, `Refresh`

**Files:**
- Modify: `pkg/pipeline/pipeline.go:1199-1216` (`StarRocksConfig`, `MarshalJSON`, `IsZero`)
- Modify: `pkg/pipeline/pipeline.go:3280-3297` (`mergeStarRocksDefaults`)
- Test: `pkg/pipeline/pipeline_test.go`

**Interfaces:**
- Produces:
  ```go
  type StarRocksRefresh struct {
      Trigger      string // immediate | deferred
      Mode         string // async | manual
      Start        string
      Every        string
      RefreshOnRun *bool
  }
  ```
  and new fields on `StarRocksConfig`: `OrderBy []string`, `Sync bool`, `Refresh *StarRocksRefresh`.
- Consumes: nothing.

- [ ] **Step 1: Write the failing test**

Add to `pkg/pipeline/pipeline_test.go`:

```go
func TestStarRocksConfig_IsZeroAndMerge(t *testing.T) {
	t.Parallel()

	assert.True(t, pipeline.StarRocksConfig{}.IsZero())
	assert.False(t, pipeline.StarRocksConfig{Sync: true}.IsZero())
	assert.False(t, pipeline.StarRocksConfig{OrderBy: []string{"a"}}.IsZero())
	assert.False(t, pipeline.StarRocksConfig{Refresh: &pipeline.StarRocksRefresh{Mode: "async"}}.IsZero())
}
```

(Use the existing `pipeline_test.go` package alias — check whether it is `package pipeline` or `package pipeline_test`; the snippet assumes `pipeline_test` with import. If the file is `package pipeline`, drop the `pipeline.` qualifier.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags="no_duckdb_arrow" ./pkg/pipeline/ -run TestStarRocksConfig_IsZeroAndMerge -v`
Expected: FAIL — unknown fields `Sync`, `OrderBy`, `Refresh` (compile error).

- [ ] **Step 3: Extend the struct + IsZero + MarshalJSON**

In `pkg/pipeline/pipeline.go`, replace the `StarRocksConfig` block (lines 1199-1216) with:

```go
type StarRocksConfig struct {
	TableModel string            `json:"table_model,omitempty" yaml:"table_model,omitempty" mapstructure:"table_model"`
	Buckets    int               `json:"buckets,omitempty" yaml:"buckets,omitempty" mapstructure:"buckets"`
	Properties map[string]string `json:"properties,omitempty" yaml:"properties,omitempty" mapstructure:"properties"`
	OrderBy    []string          `json:"order_by,omitempty" yaml:"order_by,omitempty" mapstructure:"order_by"`
	Sync       bool              `json:"sync,omitempty" yaml:"sync,omitempty" mapstructure:"sync"`
	Refresh    *StarRocksRefresh `json:"refresh,omitempty" yaml:"refresh,omitempty" mapstructure:"refresh"`
}

// StarRocksRefresh models a StarRocks asynchronous materialized view REFRESH
// clause. Trigger maps to REFRESH IMMEDIATE|DEFERRED, Mode maps to ASYNC|MANUAL,
// and Start/Every build the scheduled `ASYNC START(...) EVERY (INTERVAL ...)`
// form. RefreshOnRun overrides whether `bruin run` issues REFRESH MATERIALIZED
// VIEW on a re-run of an already-existing MV.
type StarRocksRefresh struct {
	Trigger      string `json:"trigger,omitempty" yaml:"trigger,omitempty" mapstructure:"trigger"`
	Mode         string `json:"mode,omitempty" yaml:"mode,omitempty" mapstructure:"mode"`
	Start        string `json:"start,omitempty" yaml:"start,omitempty" mapstructure:"start"`
	Every        string `json:"every,omitempty" yaml:"every,omitempty" mapstructure:"every"`
	RefreshOnRun *bool  `json:"refresh_on_run,omitempty" yaml:"refresh_on_run,omitempty" mapstructure:"refresh_on_run"`
}

func (s StarRocksConfig) MarshalJSON() ([]byte, error) {
	if s.IsZero() {
		return []byte("null"), nil
	}

	type Alias StarRocksConfig
	return json.Marshal(Alias(s))
}

func (s StarRocksConfig) IsZero() bool {
	return s.TableModel == "" &&
		s.Buckets == 0 &&
		len(s.Properties) == 0 &&
		len(s.OrderBy) == 0 &&
		!s.Sync &&
		s.Refresh == nil
}
```

- [ ] **Step 4: Extend `mergeStarRocksDefaults`**

In `pkg/pipeline/pipeline.go`, replace `mergeStarRocksDefaults` (lines 3280-3297) with:

```go
func mergeStarRocksDefaults(target *StarRocksConfig, defaults StarRocksConfig) {
	if target.TableModel == "" {
		target.TableModel = defaults.TableModel
	}
	if target.Buckets == 0 {
		target.Buckets = defaults.Buckets
	}
	if len(target.OrderBy) == 0 {
		target.OrderBy = defaults.OrderBy
	}
	if !target.Sync {
		target.Sync = defaults.Sync
	}
	if target.Refresh == nil {
		target.Refresh = defaults.Refresh
	}
	if len(defaults.Properties) > 0 {
		if target.Properties == nil {
			target.Properties = make(map[string]string, len(defaults.Properties))
		}
		for key, value := range defaults.Properties {
			if _, exists := target.Properties[key]; !exists {
				target.Properties[key] = value
			}
		}
	}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -tags="no_duckdb_arrow" ./pkg/pipeline/ -run TestStarRocksConfig_IsZeroAndMerge -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/pipeline/pipeline.go pkg/pipeline/pipeline_test.go
git commit -m "feat(pipeline): add order_by/sync/refresh to StarRocksConfig"
```

---

## Task 3: Parse new StarRocks fields from YAML/SQL frontmatter + render templated fields

**Files:**
- Modify: `pkg/pipeline/yaml.go:390-394` (YAML `starrocks` struct)
- Modify: `pkg/pipeline/yaml.go:654-658` (conversion to `StarRocksConfig`)
- Modify: `pkg/pipeline/variant.go:458-465` (Jinja render of new fields)
- Test: `pkg/pipeline/variant_test.go` (extend existing StarRocks case)

**Interfaces:**
- Consumes: `StarRocksConfig`, `StarRocksRefresh` from Task 2.
- Produces: parsed `asset.StarRocks.{OrderBy,Sync,Refresh}` from both `.yml` and `.sql` (`/* @bruin */`) assets — both flow through `taskDefinition` (`pkg/pipeline/comment.go:490`).

- [ ] **Step 1: Write the failing test**

Add to `pkg/pipeline/variant_test.go`:

```go
func TestRenderAssetStrings_StarRocksRefreshTemplated(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "analytics.mv",
		StarRocks: pipeline.StarRocksConfig{
			OrderBy: []string{"{{ order_col }}"},
			Refresh: &pipeline.StarRocksRefresh{
				Mode:  "async",
				Start: "{{ start_ts }}",
				Every: "1 day",
			},
		},
	}
	render := func(_ string, tmpl string) (string, error) {
		switch tmpl {
		case "{{ order_col }}":
			return "event_date", nil
		case "{{ start_ts }}":
			return "2025-01-01 10:00:00", nil
		default:
			return tmpl, nil
		}
	}
	require.NoError(t, pipeline.RenderAssetStringsForTest(render, asset))
	assert.Equal(t, []string{"event_date"}, asset.StarRocks.OrderBy)
	assert.Equal(t, "2025-01-01 10:00:00", asset.StarRocks.Refresh.Start)
}
```

NOTE: `renderAssetStrings` is unexported. Before writing this test, check `variant_test.go` for how existing tests invoke it (they are in `package pipeline` internal tests, calling `renderAssetStrings` directly, OR via a wrapper). If the test file is `package pipeline`, call `renderAssetStrings(render, asset)` directly and delete the `pipeline.` qualifiers + the `RenderAssetStringsForTest` reference. Match the existing `RenderFunc` signature exactly (inspect `maybeRender`'s first arg — it is `RenderFunc`; confirm its parameters).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags="no_duckdb_arrow" ./pkg/pipeline/ -run TestRenderAssetStrings_StarRocksRefreshTemplated -v`
Expected: FAIL — new fields not rendered (Start still `{{ start_ts }}`), or compile error if signature mismatch (fix signature per Step 1 note).

- [ ] **Step 3: Extend the YAML struct**

In `pkg/pipeline/yaml.go`, replace the `starrocks` struct (lines 390-394) with:

```go
type starrocksRefresh struct {
	Trigger      string `yaml:"trigger"`
	Mode         string `yaml:"mode"`
	Start        string `yaml:"start"`
	Every        string `yaml:"every"`
	RefreshOnRun *bool  `yaml:"refresh_on_run"`
}

type starrocks struct {
	TableModel string            `yaml:"table_model"`
	Buckets    int               `yaml:"buckets"`
	Properties map[string]string `yaml:"properties"`
	OrderBy    []string          `yaml:"order_by"`
	Sync       bool              `yaml:"sync"`
	Refresh    *starrocksRefresh `yaml:"refresh"`
}
```

- [ ] **Step 4: Extend the conversion**

In `pkg/pipeline/yaml.go`, replace the `StarRocks: StarRocksConfig{...}` block (lines 654-658) with:

```go
		StarRocks: func() StarRocksConfig {
			cfg := StarRocksConfig{
				TableModel: definition.StarRocks.TableModel,
				Buckets:    definition.StarRocks.Buckets,
				Properties: definition.StarRocks.Properties,
				OrderBy:    definition.StarRocks.OrderBy,
				Sync:       definition.StarRocks.Sync,
			}
			if definition.StarRocks.Refresh != nil {
				cfg.Refresh = &StarRocksRefresh{
					Trigger:      definition.StarRocks.Refresh.Trigger,
					Mode:         definition.StarRocks.Refresh.Mode,
					Start:        definition.StarRocks.Refresh.Start,
					Every:        definition.StarRocks.Refresh.Every,
					RefreshOnRun: definition.StarRocks.Refresh.RefreshOnRun,
				}
			}
			return cfg
		}(),
```

- [ ] **Step 5: Render new templated fields**

In `pkg/pipeline/variant.go`, immediately after the StarRocks `Properties` render loop (ends at line 465, before the `renderRoutingConfig` call), insert:

```go
	for i, col := range a.StarRocks.OrderBy {
		if a.StarRocks.OrderBy[i], err = maybeRender(render, fmt.Sprintf("asset[%s].starrocks.order_by[%d]", originalName, i), col); err != nil {
			return err
		}
	}
	if a.StarRocks.Refresh != nil {
		if a.StarRocks.Refresh.Start, err = maybeRender(render, fmt.Sprintf("asset[%s].starrocks.refresh.start", originalName), a.StarRocks.Refresh.Start); err != nil {
			return err
		}
		if a.StarRocks.Refresh.Every, err = maybeRender(render, fmt.Sprintf("asset[%s].starrocks.refresh.every", originalName), a.StarRocks.Refresh.Every); err != nil {
			return err
		}
	}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test -tags="no_duckdb_arrow" ./pkg/pipeline/ -run TestRenderAssetStrings_StarRocksRefreshTemplated -v`
Expected: PASS. Also run the full pipeline package to catch regressions: `go test -tags="no_duckdb_arrow" ./pkg/pipeline/`.

- [ ] **Step 7: Commit**

```bash
git add pkg/pipeline/yaml.go pkg/pipeline/variant.go pkg/pipeline/variant_test.go
git commit -m "feat(pipeline): parse and render StarRocks MV config fields"
```

---

## Task 4: Refresh-clause assembly helper (StarRocks)

**Files:**
- Modify: `pkg/starrocks/materialization.go` (add helpers near the other builders)
- Test: `pkg/starrocks/materialization_test.go` (new dedicated test)

**Interfaces:**
- Produces: `func buildRefreshClause(refresh *pipeline.StarRocksRefresh) (string, error)` returning e.g. `"REFRESH DEFERRED ASYNC START('2025-01-01 10:00:00') EVERY (INTERVAL 1 DAY)"`, `""` when `refresh == nil`.
- Produces: `func formatEveryInterval(every string) (string, error)` returning `"INTERVAL 1 DAY"` from `"1 day"`.
- Consumes: `pipeline.StarRocksRefresh` (Task 2).

- [ ] **Step 1: Write the failing test**

Add to `pkg/starrocks/materialization_test.go`:

```go
func TestBuildRefreshClause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		refresh *pipeline.StarRocksRefresh
		want    string
		wantErr string
	}{
		{name: "nil refresh yields empty", refresh: nil, want: ""},
		{name: "manual", refresh: &pipeline.StarRocksRefresh{Mode: "manual"}, want: "REFRESH MANUAL"},
		{name: "async bare", refresh: &pipeline.StarRocksRefresh{Mode: "async"}, want: "REFRESH ASYNC"},
		{
			name:    "deferred async scheduled",
			refresh: &pipeline.StarRocksRefresh{Trigger: "deferred", Mode: "async", Start: "2025-01-01 10:00:00", Every: "1 day"},
			want:    "REFRESH DEFERRED ASYNC START('2025-01-01 10:00:00') EVERY (INTERVAL 1 DAY)",
		},
		{
			name:    "immediate async every only",
			refresh: &pipeline.StarRocksRefresh{Trigger: "immediate", Mode: "async", Every: "30 minute"},
			want:    "REFRESH IMMEDIATE ASYNC EVERY (INTERVAL 30 MINUTE)",
		},
		{name: "bad trigger", refresh: &pipeline.StarRocksRefresh{Trigger: "eventually", Mode: "async"}, wantErr: "refresh.trigger"},
		{name: "bad mode", refresh: &pipeline.StarRocksRefresh{Mode: "sometimes"}, wantErr: "refresh.mode"},
		{name: "start with manual", refresh: &pipeline.StarRocksRefresh{Mode: "manual", Start: "2025-01-01 10:00:00"}, wantErr: "start"},
		{name: "malformed every", refresh: &pipeline.StarRocksRefresh{Mode: "async", Every: "soon"}, wantErr: "every"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildRefreshClause(tt.refresh)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags="no_duckdb_arrow" ./pkg/starrocks/ -run TestBuildRefreshClause -v`
Expected: FAIL — `buildRefreshClause` undefined.

- [ ] **Step 3: Implement the helpers**

Add to `pkg/starrocks/materialization.go` (after `buildDDLQuery`, before `requiresTypedCreateTable`):

```go
// buildRefreshClause assembles a StarRocks async materialized-view REFRESH
// clause from the structured refresh config. Returns "" when refresh is nil.
func buildRefreshClause(refresh *pipeline.StarRocksRefresh) (string, error) {
	if refresh == nil {
		return "", nil
	}

	parts := []string{"REFRESH"}

	switch strings.ToLower(strings.TrimSpace(refresh.Trigger)) {
	case "":
		// omit; StarRocks defaults to IMMEDIATE
	case "immediate":
		parts = append(parts, "IMMEDIATE")
	case "deferred":
		parts = append(parts, "DEFERRED")
	default:
		return "", fmt.Errorf("invalid refresh.trigger %q: expected \"immediate\" or \"deferred\"", refresh.Trigger)
	}

	mode := strings.ToLower(strings.TrimSpace(refresh.Mode))
	start := strings.TrimSpace(refresh.Start)
	every := strings.TrimSpace(refresh.Every)

	switch mode {
	case "manual":
		if start != "" || every != "" {
			return "", errors.New("refresh.start and refresh.every require refresh.mode \"async\"")
		}
		parts = append(parts, "MANUAL")
	case "async", "":
		asyncParts := []string{"ASYNC"}
		if start != "" {
			asyncParts = append(asyncParts, fmt.Sprintf("START('%s')", start))
		}
		if every != "" {
			interval, err := formatEveryInterval(every)
			if err != nil {
				return "", err
			}
			asyncParts = append(asyncParts, fmt.Sprintf("EVERY (%s)", interval))
		}
		parts = append(parts, strings.Join(asyncParts, " "))
	default:
		return "", fmt.Errorf("invalid refresh.mode %q: expected \"async\" or \"manual\"", refresh.Mode)
	}

	return strings.Join(parts, " "), nil
}

// formatEveryInterval turns "1 day" into "INTERVAL 1 DAY".
func formatEveryInterval(every string) (string, error) {
	fields := strings.Fields(every)
	if len(fields) != 2 {
		return "", fmt.Errorf("invalid refresh.every %q: expected \"<count> <unit>\" such as \"1 day\"", every)
	}
	return fmt.Sprintf("INTERVAL %s %s", fields[0], strings.ToUpper(fields[1])), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags="no_duckdb_arrow" ./pkg/starrocks/ -run TestBuildRefreshClause -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/starrocks/materialization.go pkg/starrocks/materialization_test.go
git commit -m "feat(starrocks): add MV refresh-clause assembly"
```

---

## Task 5: Materialized-view SQL builders + matMap wiring + refresh-on-run

**Files:**
- Modify: `pkg/starrocks/materialization.go` (add MV builders, matMap entry)
- Test: `pkg/starrocks/materialization_test.go` (golden cases via `TestMaterializer_Render`)

**Interfaces:**
- Consumes: `buildRefreshClause` (Task 4), `MaterializationTypeMaterializedView` (Task 1), `StarRocksConfig.{Sync,Refresh,OrderBy}` (Task 2), existing `quoteIdentifier`, `quoteColumnNames`, `escapeStarRocksProperty`.
- Produces: matMap `MaterializationTypeMaterializedView` entry with `buildMaterializedView` (None) and `buildMaterializedViewFullRefresh` (CreateReplace).

- [ ] **Step 1: Write the failing tests**

Add these cases to the `tests` slice in `TestMaterializer_Render` in `pkg/starrocks/materialization_test.go`:

```go
		{
			name: "async MV minimal with distribution",
			asset: &pipeline.Asset{
				Name:            "analytics.mv_users",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeMaterializedView, ClusterBy: []string{"user_id"}},
				StarRocks:       pipeline.StarRocksConfig{Buckets: 8},
			},
			query: "SELECT user_id FROM analytics.events",
			want: "CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`mv_users`\n" +
				"DISTRIBUTED BY HASH(`user_id`) BUCKETS 8\n" +
				"AS\n" +
				"SELECT user_id FROM analytics.events;",
		},
		{
			name: "async MV full clauses",
			asset: &pipeline.Asset{
				Name: "analytics.dau",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeMaterializedView,
					PartitionBy: "date_trunc('day', event_date)",
					ClusterBy:   []string{"user_id"},
				},
				StarRocks: pipeline.StarRocksConfig{
					Buckets: 4,
					OrderBy: []string{"event_date", "user_id"},
					Refresh: &pipeline.StarRocksRefresh{Trigger: "deferred", Mode: "async", Start: "2025-01-01 10:00:00", Every: "1 day"},
					Properties: map[string]string{"partition_refresh_number": "4"},
				},
			},
			query: "SELECT event_date, user_id FROM analytics.events",
			want: "CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`dau`\n" +
				"DISTRIBUTED BY HASH(`user_id`) BUCKETS 4\n" +
				"PARTITION BY (date_trunc('day', event_date))\n" +
				"ORDER BY (`event_date`, `user_id`)\n" +
				"REFRESH DEFERRED ASYNC START('2025-01-01 10:00:00') EVERY (INTERVAL 1 DAY)\n" +
				"PROPERTIES (\"partition_refresh_number\" = \"4\")\n" +
				"AS\n" +
				"SELECT event_date, user_id FROM analytics.events;",
		},
		{
			name: "async MV manual mode triggers refresh on run",
			asset: &pipeline.Asset{
				Name: "analytics.mv_manual",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeMaterializedView, ClusterBy: []string{"id"}},
				StarRocks: pipeline.StarRocksConfig{
					Buckets: 2,
					Refresh: &pipeline.StarRocksRefresh{Mode: "manual"},
				},
			},
			query: "SELECT id FROM analytics.src",
			want: "CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`mv_manual`\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 2\n" +
				"REFRESH MANUAL\n" +
				"AS\n" +
				"SELECT id FROM analytics.src;\n" +
				"REFRESH MATERIALIZED VIEW `analytics`.`mv_manual` WITH SYNC MODE;",
		},
		{
			name: "async MV manual refresh_on_run false suppresses refresh",
			asset: &pipeline.Asset{
				Name: "analytics.mv_manual2",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeMaterializedView, ClusterBy: []string{"id"}},
				StarRocks: pipeline.StarRocksConfig{
					Buckets: 2,
					Refresh: &pipeline.StarRocksRefresh{Mode: "manual", RefreshOnRun: boolPtr(false)},
				},
			},
			query: "SELECT id FROM analytics.src",
			want: "CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`mv_manual2`\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 2\n" +
				"REFRESH MANUAL\n" +
				"AS\n" +
				"SELECT id FROM analytics.src;",
		},
		{
			name: "sync rollup MV",
			asset: &pipeline.Asset{
				Name:            "analytics.sales_rollup",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeMaterializedView},
				StarRocks:       pipeline.StarRocksConfig{Sync: true, Properties: map[string]string{"replication_num": "1"}},
			},
			query: "SELECT store_id, sum(amount) AS total FROM analytics.sales GROUP BY store_id",
			want: "CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`sales_rollup`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT store_id, sum(amount) AS total FROM analytics.sales GROUP BY store_id;",
		},
		{
			name: "async MV full refresh drops and recreates",
			asset: &pipeline.Asset{
				Name:            "analytics.mv_fr",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeMaterializedView, ClusterBy: []string{"id"}},
				StarRocks:       pipeline.StarRocksConfig{Buckets: 2, Refresh: &pipeline.StarRocksRefresh{Mode: "async"}},
			},
			query:       "SELECT id FROM analytics.src",
			fullRefresh: true,
			want: "DROP MATERIALIZED VIEW IF EXISTS `analytics`.`mv_fr`;\n" +
				"CREATE MATERIALIZED VIEW `analytics`.`mv_fr`\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 2\n" +
				"REFRESH ASYNC\n" +
				"AS\n" +
				"SELECT id FROM analytics.src;",
		},
		{
			name: "sync MV rejects refresh",
			asset: &pipeline.Asset{
				Name:            "analytics.bad_sync",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeMaterializedView},
				StarRocks:       pipeline.StarRocksConfig{Sync: true, Refresh: &pipeline.StarRocksRefresh{Mode: "async"}},
			},
			query:   "SELECT 1",
			wantErr: "sync",
		},
		{
			name: "async MV requires distribution or refresh",
			asset: &pipeline.Asset{
				Name:            "analytics.bad_async",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeMaterializedView},
			},
			query:   "SELECT 1",
			wantErr: "distribution",
		},
```

Add this helper at the top of the test file (after the `falsePtr` block inside `TestMaterializer_Render`, or as a package-level func):

```go
func boolPtr(b bool) *bool { return &b }
```

(If `boolPtr` or similar already exists in the package's test files, reuse it instead of redefining.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags="no_duckdb_arrow" ./pkg/starrocks/ -run TestMaterializer_Render -v`
Expected: FAIL — the MV cases produce the "unsupported materialization type - strategy combination" error (no matMap entry yet).

- [ ] **Step 3: Implement the MV builders**

Add to `pkg/starrocks/materialization.go` (near the other builders, e.g. after `viewMaterializer`):

```go
func buildMaterializedView(asset *pipeline.Asset, query string) (string, error) {
	return renderMaterializedView(asset, query, false)
}

func buildMaterializedViewFullRefresh(asset *pipeline.Asset, query string) (string, error) {
	return renderMaterializedView(asset, query, true)
}

// renderMaterializedView builds a StarRocks CREATE MATERIALIZED VIEW statement.
// When fullRefresh is true it is prefixed with DROP MATERIALIZED VIEW IF EXISTS
// and omits IF NOT EXISTS; otherwise it uses IF NOT EXISTS and, for manual-mode
// refresh (or an explicit refresh_on_run), appends REFRESH MATERIALIZED VIEW.
func renderMaterializedView(asset *pipeline.Asset, query string, fullRefresh bool) (string, error) {
	trimmedQuery := strings.TrimSuffix(strings.TrimSpace(query), ";")
	sync := asset.StarRocks.Sync

	if sync {
		if asset.StarRocks.Refresh != nil {
			return "", errors.New("StarRocks sync (rollup) materialized views (`starrocks.sync: true`) do not support `refresh`")
		}
		if len(asset.Materialization.ClusterBy) > 0 || strings.TrimSpace(asset.Materialization.PartitionBy) != "" || len(asset.StarRocks.OrderBy) > 0 {
			return "", errors.New("StarRocks sync (rollup) materialized views (`starrocks.sync: true`) do not support `cluster_by`, `partition_by`, or `order_by`")
		}
	}

	var b strings.Builder
	if fullRefresh {
		b.WriteString("DROP MATERIALIZED VIEW IF EXISTS " + quoteIdentifier(asset.Name) + ";\n")
		b.WriteString("CREATE MATERIALIZED VIEW " + quoteIdentifier(asset.Name))
	} else {
		b.WriteString("CREATE MATERIALIZED VIEW IF NOT EXISTS " + quoteIdentifier(asset.Name))
	}

	refreshClause, err := buildRefreshClause(asset.StarRocks.Refresh)
	if err != nil {
		return "", err
	}

	if !sync {
		hasDistribution := len(asset.Materialization.ClusterBy) > 0
		if !hasDistribution && refreshClause == "" {
			return "", errors.New("StarRocks async materialized view requires distribution (`cluster_by`) or a `refresh` block; set `starrocks.sync: true` for a rollup view")
		}

		if hasDistribution {
			for _, column := range asset.Materialization.ClusterBy {
				if strings.TrimSpace(column) == "" {
					return "", errors.New("starrocks cluster_by columns cannot be empty")
				}
			}
			b.WriteString("\nDISTRIBUTED BY HASH(" + strings.Join(quoteColumnNames(asset.Materialization.ClusterBy), ", ") + ")")
			if asset.StarRocks.Buckets < 0 {
				return "", errors.New("starrocks buckets must be greater than zero")
			}
			if asset.StarRocks.Buckets > 0 {
				b.WriteString(fmt.Sprintf(" BUCKETS %d", asset.StarRocks.Buckets))
			}
		}

		if partitionBy := strings.TrimSpace(asset.Materialization.PartitionBy); partitionBy != "" {
			b.WriteString("\nPARTITION BY (" + partitionBy + ")")
		}

		if len(asset.StarRocks.OrderBy) > 0 {
			b.WriteString("\nORDER BY (" + strings.Join(quoteColumnNames(asset.StarRocks.OrderBy), ", ") + ")")
		}

		if refreshClause != "" {
			b.WriteString("\n" + refreshClause)
		}
	}

	if props := buildMaterializedViewProperties(asset); props != "" {
		b.WriteString("\nPROPERTIES (" + props + ")")
	}

	b.WriteString("\nAS\n" + trimmedQuery + ";")

	// On a normal (non full-refresh) run, CREATE ... IF NOT EXISTS is a no-op for
	// an existing MV, so trigger REFRESH when the asset opts in.
	if !fullRefresh && shouldRefreshOnRun(asset) {
		b.WriteString("\nREFRESH MATERIALIZED VIEW " + quoteIdentifier(asset.Name) + " WITH SYNC MODE;")
	}

	return b.String(), nil
}

// shouldRefreshOnRun resolves whether a re-run should issue REFRESH MATERIALIZED
// VIEW. Default: true for manual-mode refresh, false otherwise. An explicit
// refresh_on_run always wins.
func shouldRefreshOnRun(asset *pipeline.Asset) bool {
	refresh := asset.StarRocks.Refresh
	if refresh == nil {
		return false
	}
	if refresh.RefreshOnRun != nil {
		return *refresh.RefreshOnRun
	}
	return strings.EqualFold(strings.TrimSpace(refresh.Mode), "manual")
}

// buildMaterializedViewProperties renders PROPERTIES for a materialized view.
// Unlike tables, it does NOT inject a default replication_num.
func buildMaterializedViewProperties(asset *pipeline.Asset) string {
	if len(asset.StarRocks.Properties) == 0 {
		return ""
	}
	keys := make([]string, 0, len(asset.StarRocks.Properties))
	for key := range asset.StarRocks.Properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	clauses := make([]string, 0, len(keys))
	for _, key := range keys {
		clauses = append(clauses, fmt.Sprintf("\"%s\" = \"%s\"", escapeStarRocksProperty(key), escapeStarRocksProperty(asset.StarRocks.Properties[key])))
	}
	return strings.Join(clauses, ", ")
}
```

- [ ] **Step 4: Wire the matMap entry**

In `pkg/starrocks/materialization.go`, add to `matMap` (after the `MaterializationTypeTable` block, before the closing `}` at line 53):

```go
	pipeline.MaterializationTypeMaterializedView: {
		pipeline.MaterializationStrategyNone:           buildMaterializedView,
		pipeline.MaterializationStrategyCreateReplace:  buildMaterializedViewFullRefresh,
		pipeline.MaterializationStrategyAppend:         errorMaterializer,
		pipeline.MaterializationStrategyDeleteInsert:   errorMaterializer,
		pipeline.MaterializationStrategyTruncateInsert: errorMaterializer,
		pipeline.MaterializationStrategyMerge:          errorMaterializer,
		pipeline.MaterializationStrategyTimeInterval:   errorMaterializer,
		pipeline.MaterializationStrategyDDL:            errorMaterializer,
		pipeline.MaterializationStrategySCD2ByColumn:   errorMaterializer,
		pipeline.MaterializationStrategySCD2ByTime:     errorMaterializer,
	},
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -tags="no_duckdb_arrow" ./pkg/starrocks/ -run TestMaterializer_Render -v`
Expected: PASS (all cases including the new MV ones).

- [ ] **Step 6: Commit**

```bash
git add pkg/starrocks/materialization.go pkg/starrocks/materialization_test.go
git commit -m "feat(starrocks): implement async and sync materialized views"
```

---

## Task 6: `ORDER BY` on regular tables

**Files:**
- Modify: `pkg/starrocks/materialization.go:269-275` (`requiresTypedCreateTable`), `:335-352` (`buildCreateTableStatement`)
- Test: `pkg/starrocks/materialization_test.go`

**Interfaces:**
- Consumes: `StarRocksConfig.OrderBy` (Task 2).
- Produces: `ORDER BY (...)` clause between `DISTRIBUTED BY ... BUCKETS n` and `PROPERTIES` in typed CREATE TABLE and DDL.

- [ ] **Step 1: Write the failing test**

Add to the `tests` slice in `TestMaterializer_Render`:

```go
		{
			name: "table with order_by",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					ClusterBy: []string{"id"},
				},
				StarRocks: pipeline.StarRocksConfig{Buckets: 4, OrderBy: []string{"created_at"}},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT"},
					{Name: "created_at", Type: "DATETIME"},
				},
			},
			query: "SELECT id, created_at FROM src",
			want: "DROP TABLE IF EXISTS `analytics`.`orders`;\n" +
				"CREATE TABLE `analytics`.`orders` (\n" +
				"`id` INT,\n" +
				"`created_at` DATETIME\n" +
				")\n" +
				"DUPLICATE KEY(`id`)\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 4\n" +
				"ORDER BY (`created_at`)\n" +
				"PROPERTIES (\"replication_num\" = \"1\");\n" +
				"INSERT INTO `analytics`.`orders` (`id`, `created_at`)\n" +
				"SELECT id, created_at FROM src;",
		},
```

IMPORTANT: this `want` string is the plan author's best reconstruction of the existing `buildCreateTableStatement` format plus the new `ORDER BY` line. Before finalizing, the implementer MUST run the test once after Step 3 and, if the only differences are pre-existing formatting (whitespace/newlines) unrelated to `ORDER BY`, adjust the `want` string to match actual output — the assertion is exact-match. Do NOT change the ORDER BY placement to make it pass.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags="no_duckdb_arrow" ./pkg/starrocks/ -run TestMaterializer_Render/table_with_order_by -v`
Expected: FAIL — no `ORDER BY` in output.

- [ ] **Step 3: Add `OrderBy` to `requiresTypedCreateTable`**

In `pkg/starrocks/materialization.go`, change `requiresTypedCreateTable` (lines 269-275) to add a clause:

```go
func requiresTypedCreateTable(asset *pipeline.Asset) bool {
	return asset.Materialization.Strategy == pipeline.MaterializationStrategyMerge ||
		strings.TrimSpace(asset.StarRocks.TableModel) != "" ||
		len(asset.Materialization.ClusterBy) > 0 ||
		strings.TrimSpace(asset.Materialization.PartitionBy) != "" ||
		asset.StarRocks.Buckets != 0 ||
		len(asset.StarRocks.OrderBy) > 0
}
```

- [ ] **Step 4: Emit `ORDER BY` in `buildCreateTableStatement`**

In `pkg/starrocks/materialization.go`, modify `buildCreateTableStatement`. Before the final `return fmt.Sprintf(...)` (line 335), add:

```go
	orderByClause := ""
	if len(asset.StarRocks.OrderBy) > 0 {
		orderByClause = "ORDER BY (" + strings.Join(quoteColumnNames(asset.StarRocks.OrderBy), ", ") + ")\n"
	}
```

Then change the format string and args so the `ORDER BY` line sits between the `DISTRIBUTED BY ... BUCKETS %d` line and `PROPERTIES`. Replace the `return fmt.Sprintf(...)` block (lines 335-351) with:

```go
	return fmt.Sprintf(
		`%s%s (
%s
)
%s(%s)
%sDISTRIBUTED BY HASH(%s) BUCKETS %d
%sPROPERTIES (%s)`,
		createPrefix,
		quoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
		starRocksKeyClause(model),
		strings.Join(quoteColumnNames(keyColumns), ", "),
		partitionClause,
		strings.Join(quoteColumnNames(distributedBy), ", "),
		buckets,
		orderByClause,
		buildStarRocksProperties(asset),
	), nil
```

(Note: `%s` for `orderByClause` is inserted immediately before `PROPERTIES`; the clause itself carries its trailing `\n` so it disappears cleanly when empty — matching how `partitionClause` works.)

- [ ] **Step 5: Run test; reconcile golden string if needed**

Run: `go test -tags="no_duckdb_arrow" ./pkg/starrocks/ -run TestMaterializer_Render/table_with_order_by -v`
Expected: PASS. If it fails ONLY on pre-existing formatting (not the `ORDER BY` line/position), update the test's `want` to the actual output and re-run. Then run the whole file: `go test -tags="no_duckdb_arrow" ./pkg/starrocks/ -run TestMaterializer_Render` — the DDL golden case (`ddl emits partition by clause`, if present) must still pass; if `ORDER BY` now appears there unexpectedly, it means an existing test asset sets `OrderBy` (it should not) — investigate rather than editing that golden blindly.

- [ ] **Step 6: Commit**

```bash
git add pkg/starrocks/materialization.go pkg/starrocks/materialization_test.go
git commit -m "feat(starrocks): support ORDER BY sort key on tables"
```

---

## Task 7: Lint validation for `materialized_view` + refresh/sync/order_by

**Files:**
- Modify: `pkg/lint/rules.go:1359-1540` (`EnsureMaterializationValuesAreValidForSingleAsset`)
- Test: `pkg/lint/rules_test.go` (find existing tests for this function; follow their pattern)

**Interfaces:**
- Consumes: `MaterializationTypeMaterializedView` (Task 1), `StarRocksConfig.{Sync,Refresh}` (Task 2).
- Produces: validation issues; adds `materialized_view` to the supported-types list so non-MV platforms still error clearly but StarRocks MVs pass.

- [ ] **Step 1: Locate the existing test**

Run: `grep -rn "EnsureMaterializationValuesAreValidForSingleAsset" pkg/lint/`
Read the existing test cases (likely in `pkg/lint/rules_test.go`) to match the harness (how issues/descriptions are asserted).

- [ ] **Step 2: Write the failing test**

Add a test following the existing pattern for `EnsureMaterializationValuesAreValidForSingleAsset`. Minimal shape (adapt names to the existing suite):

```go
func TestEnsureMaterializationValuesAreValidForSingleAsset_MaterializedView(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	valid := &pipeline.Asset{
		Type:            pipeline.AssetTypeStarRocksQuery,
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeMaterializedView, ClusterBy: []string{"id"}},
	}
	issues, err := lint.EnsureMaterializationValuesAreValidForSingleAsset(ctx, nil, valid)
	require.NoError(t, err)
	assert.Empty(t, issues)

	badRefresh := &pipeline.Asset{
		Type: pipeline.AssetTypeStarRocksQuery,
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeMaterializedView},
		StarRocks: pipeline.StarRocksConfig{Refresh: &pipeline.StarRocksRefresh{Trigger: "eventually", Mode: "async"}},
	}
	issues, err = lint.EnsureMaterializationValuesAreValidForSingleAsset(ctx, nil, badRefresh)
	require.NoError(t, err)
	assert.NotEmpty(t, issues)
}
```

Check the actual exported name / package (`lint` vs internal) and `AssetTypeStarRocksQuery` availability; adjust import qualifiers to match the existing test file's package.

- [ ] **Step 3: Run test to verify it fails**

Run: `go test -tags="no_duckdb_arrow" ./pkg/lint/ -run TestEnsureMaterializationValuesAreValidForSingleAsset_MaterializedView -v`
Expected: FAIL — the valid MV asset currently produces a "Materialization type 'materialized_view' is not supported" issue (hits the `default` case at line 1525).

- [ ] **Step 4: Add the `materialized_view` case**

In `pkg/lint/rules.go`, add a new `case` inside the `switch asset.Materialization.Type` block (after the `MaterializationTypeTable` case ends at line 1524, before the `default:` at line 1525):

```go
	case pipeline.MaterializationTypeMaterializedView:
		refresh := asset.StarRocks.Refresh
		if asset.StarRocks.Sync {
			if refresh != nil {
				issues = append(issues, &Issue{Task: asset, Description: "StarRocks sync materialized views (starrocks.sync: true) do not support 'refresh'"})
			}
			if len(asset.Materialization.ClusterBy) > 0 || asset.Materialization.PartitionBy != "" || len(asset.StarRocks.OrderBy) > 0 {
				issues = append(issues, &Issue{Task: asset, Description: "StarRocks sync materialized views (starrocks.sync: true) do not support cluster_by, partition_by, or order_by"})
			}
		} else if len(asset.Materialization.ClusterBy) == 0 && refresh == nil {
			issues = append(issues, &Issue{Task: asset, Description: "StarRocks async materialized view requires distribution (cluster_by) or a refresh block; set starrocks.sync: true for a rollup view"})
		}
		if refresh != nil {
			switch strings.ToLower(strings.TrimSpace(refresh.Trigger)) {
			case "", "immediate", "deferred":
			default:
				issues = append(issues, &Issue{Task: asset, Description: "materialization refresh.trigger must be 'immediate' or 'deferred'"})
			}
			switch strings.ToLower(strings.TrimSpace(refresh.Mode)) {
			case "", "async", "manual":
			default:
				issues = append(issues, &Issue{Task: asset, Description: "materialization refresh.mode must be 'async' or 'manual'"})
			}
			if strings.EqualFold(strings.TrimSpace(refresh.Mode), "manual") && (strings.TrimSpace(refresh.Start) != "" || strings.TrimSpace(refresh.Every) != "") {
				issues = append(issues, &Issue{Task: asset, Description: "materialization refresh.start/refresh.every require refresh.mode 'async'"})
			}
		}
```

- [ ] **Step 5: Also list the type in the `default` error**

In the `default:` case (lines 1525-1536), extend the supported-types slice so the error message is accurate:

```go
				[]pipeline.MaterializationType{
					pipeline.MaterializationTypeView,
					pipeline.MaterializationTypeTable,
					pipeline.MaterializationTypeMaterializedView,
				},
```

Confirm `strings` is imported in `pkg/lint/rules.go` (it is — used elsewhere in the file).

- [ ] **Step 6: Run test to verify it passes**

Run: `go test -tags="no_duckdb_arrow" ./pkg/lint/ -run TestEnsureMaterializationValuesAreValidForSingleAsset_MaterializedView -v`
Expected: PASS. Then run the whole lint package: `go test -tags="no_duckdb_arrow" ./pkg/lint/`.

- [ ] **Step 7: Commit**

```bash
git add pkg/lint/rules.go pkg/lint/rules_test.go
git commit -m "feat(lint): validate StarRocks materialized_view assets"
```

---

## Task 8: Integration test pipelines

**Files:**
- Create: `integration-tests/cloud-integration-tests/starrocks/test-pipelines/materialized-view-async/pipeline.yml`
- Create: `integration-tests/cloud-integration-tests/starrocks/test-pipelines/materialized-view-async/assets/mv_async.sql`
- Create: `integration-tests/cloud-integration-tests/starrocks/test-pipelines/materialized-view-sync/pipeline.yml`
- Create: `integration-tests/cloud-integration-tests/starrocks/test-pipelines/materialized-view-sync/assets/mv_sync.sql`

**Interfaces:**
- Consumes: nothing new (uses the shipped binary + existing StarRocks cloud test module).

- [ ] **Step 1: Inspect an existing StarRocks test-pipeline for exact structure**

Run: `ls integration-tests/cloud-integration-tests/starrocks/test-pipelines/` and read one existing pipeline's `pipeline.yml` + an asset `.sql` (e.g. the `view` pipeline). Match its `pipeline.yml` keys (name, connection defaults) and how `starrocks_test.go` discovers/asserts pipelines. If `starrocks_test.go` enumerates pipelines explicitly, add the two new pipeline names there following the existing pattern.

- [ ] **Step 2: Create the async MV pipeline**

`integration-tests/cloud-integration-tests/starrocks/test-pipelines/materialized-view-async/pipeline.yml` — mirror the existing pipelines' header exactly. Minimal template (adapt `default_connections`/`name` to match siblings):

```yaml
name: materialized-view-async
```

`.../materialized-view-async/assets/mv_async.sql`:

```sql
/* @bruin
name: mv_async_test.daily_counts
type: starrocks.sql
materialization:
  type: materialized_view
  cluster_by: [id]
starrocks:
  buckets: 2
  refresh:
    mode: async
@bruin */

SELECT 1 AS id, 10 AS cnt
```

- [ ] **Step 3: Create the sync MV pipeline**

`.../materialized-view-sync/pipeline.yml`:

```yaml
name: materialized-view-sync
```

`.../materialized-view-sync/assets/mv_sync.sql`:

```sql
/* @bruin
name: mv_sync_test.rollup
type: starrocks.sql
materialization:
  type: materialized_view
starrocks:
  sync: true
@bruin */

SELECT id, sum(cnt) AS total FROM mv_sync_test.base GROUP BY id
```

(Adjust schema/base-table references to whatever the existing StarRocks integration pipelines assume; the sync MV needs a real base table — model it on the existing `view`/`append` pipelines which already create source tables.)

- [ ] **Step 4: Validate parsing locally (no cloud needed)**

Build and parse-check the pipelines (parsing/materialization runs without a live StarRocks):

```bash
make build
./bin/bruin internal parse-pipeline integration-tests/cloud-integration-tests/starrocks/test-pipelines/materialized-view-async
./bin/bruin render integration-tests/cloud-integration-tests/starrocks/test-pipelines/materialized-view-async/assets/mv_async.sql
```

Expected: parse succeeds; `render` prints the `CREATE MATERIALIZED VIEW IF NOT EXISTS ... DISTRIBUTED BY HASH(\`id\`) BUCKETS 2 ... REFRESH ASYNC ... AS SELECT ...`.

- [ ] **Step 5: Commit**

```bash
git add integration-tests/cloud-integration-tests/starrocks/test-pipelines/materialized-view-async integration-tests/cloud-integration-tests/starrocks/test-pipelines/materialized-view-sync
git commit -m "test(starrocks): add materialized view integration pipelines"
```

Note: full cloud execution (`make integration-test-cloud`) requires live StarRocks credentials and may be skipped in this environment; the parse/render check in Step 4 is the required local verification.

---

## Task 9: Documentation

**Files:**
- Modify: `docs/platforms/starrocks.md`

**Interfaces:**
- Consumes: nothing.

- [ ] **Step 1: Read the current docs**

Read `docs/platforms/starrocks.md` in full to match its heading style, code-fence conventions, and existing materialization section.

- [ ] **Step 2: Add a "Materialized Views" section**

Append a section documenting:
- `materialization.type: materialized_view` on a `starrocks.sql` asset.
- Async MV: `cluster_by` → `DISTRIBUTED BY HASH`, `starrocks.buckets`, `partition_by`, `starrocks.order_by`, and the `starrocks.refresh` block (`trigger: immediate|deferred`, `mode: async|manual`, `start`, `every`, `refresh_on_run`), plus MV `properties` (note: no default `replication_num` is injected for MVs).
- Sync (rollup) MV: `starrocks.sync: true`; no distribution/partition/order-by/refresh allowed.
- Run behavior: first run `CREATE ... IF NOT EXISTS`; re-run issues `REFRESH MATERIALIZED VIEW ... WITH SYNC MODE` when `refresh_on_run` resolves true (default true for `manual`, false for `async`); `--full-refresh` does `DROP` + `CREATE`.

Use the two examples from the design spec (`docs/superpowers/specs/2026-07-15-starrocks-materialized-views-design.md` §4.1, §4.2) verbatim as the doc examples.

- [ ] **Step 3: Add an `order_by` note to the tables section**

Document `starrocks.order_by: [col, ...]` → `ORDER BY (...)` for regular `table` materialization, with the example from spec §4.3.

- [ ] **Step 4: Verify markdown**

Run: `git diff docs/platforms/starrocks.md` and read it for correctness. (No build/test needed for docs-only changes.)

- [ ] **Step 5: Commit**

```bash
git add docs/platforms/starrocks.md
git commit -m "docs(starrocks): document materialized views and order_by"
```

---

## Task 10: Full verification

**Files:** none (verification only).

- [ ] **Step 1: Format**

Run: `make format`
Then: `git diff` — if formatting changed files, stage and commit them:
```bash
git add -A && git commit -m "chore: gofmt/gci formatting"
```
(Skip the commit if `git diff` is empty.)

- [ ] **Step 2: Full unit tests**

Run: `make test`
Expected: PASS. If failures appear in `pkg/starrocks`, `pkg/pipeline`, or `pkg/lint`, fix them (most likely an exact-match golden string needing reconciliation — verify the SQL is correct StarRocks, then align the expectation).

- [ ] **Step 3: Light integration (parsing/workflow) sanity**

Run: `make integration-test-light`
Expected: PASS (this exercises parsing and local pipeline workflows; MV parsing is covered). If it references expectation JSON that changed intentionally, inspect the diff carefully before including it.

- [ ] **Step 4: Final commit if anything pending**

```bash
git status
# commit any residual intentional changes
```

---

## Self-Review Notes (for the executor)

- **Exact-match golden strings** (Tasks 5, 6): several `want` strings reproduce the existing `buildCreateTableStatement` formatting. Task 6 explicitly instructs reconciling the table `want` against actual output — do this rather than force-fitting the code. For MV `want` strings (Task 5), the builder in Task 5 Step 3 is the source of truth; if a mismatch appears, confirm the SQL is valid StarRocks (clause order per Global Constraints) and align the expectation to the builder's output.
- **Test package (internal vs external):** Tasks 2/3/7 test snippets qualify types with `pipeline.` / `lint.`. Check each target `_test.go`'s `package` line first and drop qualifiers for internal (`package pipeline`) test files.
- **`renderAssetStrings` is unexported** (Task 3): match how existing `variant_test.go` cases call it; do not invent an exported wrapper unless one already exists.
- **SQL and YAML assets share one parser** (`taskDefinition` at `comment.go:490`), so Task 3 covers both `.sql` frontmatter and `.asset.yml` with a single struct change — no separate comment.go edit needed.
