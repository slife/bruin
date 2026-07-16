package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMergeStarRocksDefaults covers mergeStarRocksDefaults directly. It is
// unexported, so this lives in the internal `package pipeline` test file
// rather than pipeline_test.go (package pipeline_test).
func TestMergeStarRocksDefaults(t *testing.T) {
	t.Parallel()

	t.Run("inherits all defaults onto an empty target", func(t *testing.T) {
		t.Parallel()

		defaults := StarRocksConfig{
			OrderBy: []string{"created_at"},
			Sync:    true,
			Refresh: &StarRocksRefresh{Mode: "async", Start: "2024-01-01", Every: "1 hour"},
		}
		target := StarRocksConfig{}

		mergeStarRocksDefaults(&target, defaults)

		assert.Equal(t, []string{"created_at"}, target.OrderBy)
		assert.True(t, target.Sync)
		assert.Equal(t, &StarRocksRefresh{Mode: "async", Start: "2024-01-01", Every: "1 hour"}, target.Refresh)
	})

	t.Run("keeps target's own values instead of the defaults", func(t *testing.T) {
		t.Parallel()

		// Sync is a plain bool (not *bool), so its zero value (false) is
		// indistinguishable from "unset" by design; the only value a target
		// can meaningfully assert ownership of is `true`.
		defaults := StarRocksConfig{
			OrderBy: []string{"created_at"},
			Sync:    false,
			Refresh: &StarRocksRefresh{Mode: "async"},
		}
		target := StarRocksConfig{
			OrderBy: []string{"id"},
			Sync:    true,
			Refresh: &StarRocksRefresh{Mode: "manual"},
		}

		mergeStarRocksDefaults(&target, defaults)

		assert.Equal(t, []string{"id"}, target.OrderBy)
		assert.True(t, target.Sync)
		assert.Equal(t, &StarRocksRefresh{Mode: "manual"}, target.Refresh)
	})

	t.Run("does not alias defaults across sibling assets", func(t *testing.T) {
		t.Parallel()

		refreshOnRun := true
		defaults := StarRocksConfig{
			OrderBy: []string{"created_at"},
			Refresh: &StarRocksRefresh{Mode: "async", Start: "2024-01-01", RefreshOnRun: &refreshOnRun},
		}

		target1 := StarRocksConfig{}
		target2 := StarRocksConfig{}
		mergeStarRocksDefaults(&target1, defaults)
		mergeStarRocksDefaults(&target2, defaults)

		// Simulate what a later rendering stage does: mutate the merged
		// value in place (e.g. Jinja-rendering OrderBy[i] and Refresh.Start).
		target1.OrderBy[0] = "rendered_column"
		target1.Refresh.Start = "rendered_start"
		*target1.Refresh.RefreshOnRun = false

		// The shared pipeline-level defaults must be untouched.
		assert.Equal(t, "created_at", defaults.OrderBy[0])
		assert.Equal(t, "2024-01-01", defaults.Refresh.Start)
		assert.True(t, *defaults.Refresh.RefreshOnRun)

		// And target2, which merged from the same defaults, must also be
		// untouched by target1's in-place mutation.
		assert.Equal(t, "created_at", target2.OrderBy[0])
		assert.Equal(t, "2024-01-01", target2.Refresh.Start)
		assert.True(t, *target2.Refresh.RefreshOnRun)

		// Sanity: target1 really did get its own, independently mutable copy.
		assert.Equal(t, "rendered_column", target1.OrderBy[0])
		assert.Equal(t, "rendered_start", target1.Refresh.Start)
		assert.False(t, *target1.Refresh.RefreshOnRun)
	})
}
