package lint

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureStarRocksMaterializationValuesAreValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		asset *pipeline.Asset
		want  []string
	}{
		{
			name: "valid async materialized view",
			asset: starRocksMaterializedViewAsset(
				pipeline.Materialization{ClusterBy: []string{"id"}},
				&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeAsync},
			),
		},
		{
			name: "valid sync materialized view",
			asset: starRocksMaterializedViewAsset(
				pipeline.Materialization{},
				&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeSync},
			),
		},
		{
			name: "sync materialized view rejects create replace strategy",
			asset: starRocksMaterializedViewAsset(
				pipeline.Materialization{Strategy: pipeline.MaterializationStrategyCreateReplace},
				&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeSync},
			),
			want: []string{"materialization.strategy 'create+replace' is not supported for StarRocks sync materialized views; available strategy is: none"},
		},
		{
			name: "configuration is rejected on another datasource",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeBigqueryQuery,
				StarRocks: pipeline.StarRocksConfig{Materialization: &pipeline.StarRocksMaterializationConfig{
					Type: starRocksMaterializationTypeMaterializedView,
				}},
			},
			want: []string{"starrocks configuration is only supported for StarRocks SQL assets"},
		},
		{
			name: "shared materialization registry does not accept materialized view",
			asset: &pipeline.Asset{
				Type:            pipeline.AssetTypeBigqueryQuery,
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationType("materialized_view")},
			},
			want: []string{"Materialization type 'materialized_view' is not supported, available types are: [view table]"},
		},
		{
			name: "invalid local type",
			asset: starRocksMaterializedViewAsset(
				pipeline.Materialization{},
				&pipeline.StarRocksMaterializationConfig{Type: "index"},
			),
			want: []string{"starrocks.materialization.type must be 'materialized_view', 'table', or 'view'"},
		},
		{
			name: "invalid local mode",
			asset: starRocksMaterializedViewAsset(
				pipeline.Materialization{ClusterBy: []string{"id"}},
				&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: "eventual"},
			),
			want: []string{"starrocks.materialization.mode must be 'async' or 'sync'"},
		},
		{
			name: "unsupported materialized view strategy",
			asset: starRocksMaterializedViewAsset(
				pipeline.Materialization{Strategy: pipeline.MaterializationStrategyAppend, ClusterBy: []string{"id"}},
				&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeAsync},
			),
			want: []string{"materialization.strategy 'append' is not supported for StarRocks materialized views; available strategies are: [none create+replace]"},
		},
		{
			name: "table model is rejected",
			asset: func() *pipeline.Asset {
				asset := starRocksMaterializedViewAsset(
					pipeline.Materialization{ClusterBy: []string{"id"}},
					&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeAsync},
				)
				asset.StarRocks.TableModel = "primary_key"
				return asset
			}(),
			want: []string{"starrocks.table_model is not supported for materialized views"},
		},
		{
			name: "buckets require cluster by",
			asset: func() *pipeline.Asset {
				asset := starRocksMaterializedViewAsset(
					pipeline.Materialization{},
					&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeAsync, Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeManual}},
				)
				asset.StarRocks.Buckets = 4
				return asset
			}(),
			want: []string{"starrocks.buckets requires materialization.cluster_by"},
		},
		{
			name: "sync rejects layout and refresh settings",
			asset: func() *pipeline.Asset {
				asset := starRocksMaterializedViewAsset(
					pipeline.Materialization{ClusterBy: []string{"id"}, PartitionBy: "dt"},
					&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeSync, Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync}},
				)
				asset.StarRocks.Buckets = 4
				asset.StarRocks.OrderBy = []string{"dt"}
				return asset
			}(),
			want: []string{
				"starrocks.materialization.mode 'sync' does not support materialization.cluster_by",
				"starrocks.materialization.mode 'sync' does not support materialization.partition_by",
				"starrocks.materialization.mode 'sync' does not support starrocks.order_by",
				"starrocks.materialization.mode 'sync' does not support starrocks.buckets",
				"starrocks.materialization.mode 'sync' does not support starrocks.materialization.refresh",
			},
		},
		{
			name: "empty layout keys are rejected",
			asset: func() *pipeline.Asset {
				asset := starRocksMaterializedViewAsset(
					pipeline.Materialization{ClusterBy: []string{""}},
					&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeAsync},
				)
				asset.StarRocks.OrderBy = []string{" "}
				return asset
			}(),
			want: []string{
				"materialization.cluster_by entries cannot be empty",
				"starrocks.order_by entries cannot be empty",
			},
		},
		{
			name: "refresh interval requires a positive integer",
			asset: starRocksMaterializedViewAsset(
				pipeline.Materialization{},
				&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeAsync, Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync, Every: "0 day"}},
			),
			want: []string{"starrocks.materialization.refresh.every count must be a positive integer"},
		},
		{
			name: "refresh interval rejects unsupported units",
			asset: starRocksMaterializedViewAsset(
				pipeline.Materialization{},
				&pipeline.StarRocksMaterializationConfig{Type: starRocksMaterializationTypeMaterializedView, Mode: starRocksMaterializationModeAsync, Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync, Every: "1 week"}},
			),
			want: []string{"starrocks.materialization.refresh.every unit must be DAY, HOUR, MINUTE, or SECOND"},
		},
		{
			name: "table override uses generic validation",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeStarRocksQuery,
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
				StarRocks: pipeline.StarRocksConfig{Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(pipeline.MaterializationTypeTable)}},
			},
			want: []string{"Materialization strategy 'delete+insert' requires the 'incremental_key' field to be set"},
		},
		{
			name: "view override rejects MV-only mode",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeStarRocksQuery,
				StarRocks: pipeline.StarRocksConfig{Materialization: &pipeline.StarRocksMaterializationConfig{
					Type: string(pipeline.MaterializationTypeView),
					Mode: starRocksMaterializationModeSync,
				}},
			},
			want: []string{"starrocks.materialization.mode is only supported when type is 'materialized_view'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issues, err := EnsureMaterializationValuesAreValidForSingleAsset(t.Context(), nil, tt.asset)
			require.NoError(t, err)
			descriptions := make([]string, len(issues))
			for i, issue := range issues {
				descriptions[i] = issue.Description
			}
			if tt.want == nil {
				assert.Empty(t, descriptions)
				return
			}
			assert.Equal(t, tt.want, descriptions)
		})
	}
}

func starRocksMaterializedViewAsset(materialization pipeline.Materialization, config *pipeline.StarRocksMaterializationConfig) *pipeline.Asset {
	return &pipeline.Asset{
		Name:            "analytics.mv",
		Type:            pipeline.AssetTypeStarRocksQuery,
		Materialization: materialization,
		StarRocks: pipeline.StarRocksConfig{
			Materialization: config,
		},
	}
}
