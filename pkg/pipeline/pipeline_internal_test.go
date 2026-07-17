package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const starRocksMaterializationModeAsync = "async"

// TestMergeStarRocksDefaults covers mergeStarRocksDefaults directly. It is
// unexported, so this lives in the internal `package pipeline` test file
// rather than pipeline_test.go (package pipeline_test).
func TestMergeStarRocksDefaults(t *testing.T) {
	t.Parallel()

	t.Run("inherits all defaults onto an empty target", func(t *testing.T) {
		t.Parallel()

		defaults := StarRocksConfig{
			OrderBy: []string{"created_at"},
			Materialization: &StarRocksMaterializationConfig{
				Type: "materialized_view",
				Mode: starRocksMaterializationModeAsync,
				Refresh: &StarRocksRefresh{
					Mode:  "async",
					Start: "2024-01-01",
					Every: "1 hour",
				},
			},
		}
		target := StarRocksConfig{}

		mergeStarRocksDefaults(&target, defaults)

		assert.Equal(t, []string{"created_at"}, target.OrderBy)
		assert.Equal(t, defaults.Materialization, target.Materialization)
	})

	t.Run("keeps target's own values instead of the defaults", func(t *testing.T) {
		t.Parallel()

		defaults := StarRocksConfig{
			OrderBy: []string{"created_at"},
			Materialization: &StarRocksMaterializationConfig{
				Type:    "materialized_view",
				Mode:    "async",
				Refresh: &StarRocksRefresh{Mode: starRocksMaterializationModeAsync},
			},
		}
		target := StarRocksConfig{
			OrderBy: []string{"id"},
			Materialization: &StarRocksMaterializationConfig{
				Type: "table",
			},
		}

		mergeStarRocksDefaults(&target, defaults)

		assert.Equal(t, []string{"id"}, target.OrderBy)
		assert.Equal(t, &StarRocksMaterializationConfig{Type: "table"}, target.Materialization)
	})

	t.Run("does not alias defaults across sibling assets", func(t *testing.T) {
		t.Parallel()

		refreshOnRun := true
		defaults := StarRocksConfig{
			OrderBy: []string{"created_at"},
			Materialization: &StarRocksMaterializationConfig{
				Type: "materialized_view",
				Mode: starRocksMaterializationModeAsync,
				Refresh: &StarRocksRefresh{
					Mode:         "async",
					Start:        "2024-01-01",
					RefreshOnRun: &refreshOnRun,
				},
			},
		}

		target1 := StarRocksConfig{}
		target2 := StarRocksConfig{}
		mergeStarRocksDefaults(&target1, defaults)
		mergeStarRocksDefaults(&target2, defaults)

		// Simulate what a later rendering stage does: mutate the merged
		// value in place (e.g. Jinja-rendering OrderBy[i] and Refresh.Start).
		target1.OrderBy[0] = "rendered_column"
		target1.Materialization.Refresh.Start = "rendered_start"
		*target1.Materialization.Refresh.RefreshOnRun = false

		// The shared pipeline-level defaults must be untouched.
		assert.Equal(t, "created_at", defaults.OrderBy[0])
		assert.Equal(t, "2024-01-01", defaults.Materialization.Refresh.Start)
		assert.True(t, *defaults.Materialization.Refresh.RefreshOnRun)

		// And target2, which merged from the same defaults, must also be
		// untouched by target1's in-place mutation.
		assert.Equal(t, "created_at", target2.OrderBy[0])
		assert.Equal(t, "2024-01-01", target2.Materialization.Refresh.Start)
		assert.True(t, *target2.Materialization.Refresh.RefreshOnRun)

		// Sanity: target1 really did get its own, independently mutable copy.
		assert.Equal(t, "rendered_column", target1.OrderBy[0])
		assert.Equal(t, "rendered_start", target1.Materialization.Refresh.Start)
		assert.False(t, *target1.Materialization.Refresh.RefreshOnRun)
	})
}

func TestConvertYamlToTaskParsesStarRocksMaterialization(t *testing.T) {
	t.Parallel()

	asset, err := ConvertYamlToTask([]byte(`
name: analytics.daily_users
type: starrocks.sql
materialization:
  cluster_by: [user_id]
starrocks:
  materialization:
    type: materialized_view
    mode: async
    refresh:
      trigger: deferred
      mode: async
      start: "2025-01-01 10:00:00"
      every: 1 day
      refresh_on_run: false
  buckets: 8
  order_by: [event_date, user_id]
`))

	require.NoError(t, err)
	assert.Equal(t, []string{"user_id"}, asset.Materialization.ClusterBy)
	assert.Equal(t, 8, asset.StarRocks.Buckets)
	assert.Equal(t, []string{"event_date", "user_id"}, asset.StarRocks.OrderBy)
	assert.Equal(t, &StarRocksMaterializationConfig{
		Type: "materialized_view",
		Mode: starRocksMaterializationModeAsync,
		Refresh: &StarRocksRefresh{
			Trigger:      "deferred",
			Mode:         "async",
			Start:        "2025-01-01 10:00:00",
			Every:        "1 day",
			RefreshOnRun: boolPointer(false),
		},
	}, asset.StarRocks.Materialization)
}

func boolPointer(value bool) *bool {
	return &value
}
