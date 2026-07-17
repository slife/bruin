package lint

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

const (
	starRocksMaterializationTypeMaterializedView = "materialized_view"
	starRocksMaterializationModeAsync            = "async"
	starRocksMaterializationModeSync             = "sync"
	starRocksRefreshModeAsync                    = "async"
	starRocksRefreshModeManual                   = "manual"
)

func ensureStarRocksMaterializationValuesAreValid(asset *pipeline.Asset) ([]*Issue, error) {
	if asset.Type != pipeline.AssetTypeStarRocksQuery {
		return []*Issue{{Task: asset, Description: "starrocks configuration is only supported for StarRocks SQL assets"}}, nil
	}

	config := asset.StarRocks.Materialization
	if config == nil {
		issues := ensureGenericMaterializationValuesAreValid(asset)
		return append(issues, validateStarRocksLayout(asset, asset.Materialization.Type, "")...), nil
	}

	localType := strings.ToLower(strings.TrimSpace(config.Type))
	switch localType {
	case string(pipeline.MaterializationTypeTable), string(pipeline.MaterializationTypeView):
		issues := make([]*Issue, 0)
		if strings.TrimSpace(config.Mode) != "" {
			issues = append(issues, starRocksIssue(asset, "starrocks.materialization.mode is only supported when type is 'materialized_view'"))
		}
		if config.Refresh != nil {
			issues = append(issues, starRocksIssue(asset, "starrocks.materialization.refresh is only supported when type is 'materialized_view'"))
		}

		assetCopy := *asset
		assetCopy.Materialization = asset.Materialization
		assetCopy.Materialization.Type = pipeline.MaterializationType(localType)
		issues = append(issues, ensureGenericMaterializationValuesAreValid(&assetCopy)...)
		issues = append(issues, validateStarRocksLayout(asset, pipeline.MaterializationType(localType), "")...)
		return issues, nil
	case starRocksMaterializationTypeMaterializedView:
		return validateStarRocksMaterializedView(asset, config), nil
	default:
		return []*Issue{starRocksIssue(asset, "starrocks.materialization.type must be 'materialized_view', 'table', or 'view'")}, nil
	}
}

func validateStarRocksMaterializedView(asset *pipeline.Asset, config *pipeline.StarRocksMaterializationConfig) []*Issue {
	issues := make([]*Issue, 0)
	mode := strings.ToLower(strings.TrimSpace(config.Mode))
	if mode == "" {
		mode = starRocksMaterializationModeAsync
	}
	if mode != starRocksMaterializationModeAsync && mode != starRocksMaterializationModeSync {
		issues = append(issues, starRocksIssue(asset, "starrocks.materialization.mode must be 'async' or 'sync'"))
	}

	switch asset.Materialization.Strategy {
	case pipeline.MaterializationStrategyNone:
	case pipeline.MaterializationStrategyCreateReplace:
		if mode == starRocksMaterializationModeSync {
			issues = append(issues, starRocksIssue(asset, "materialization.strategy 'create+replace' is not supported for StarRocks sync materialized views; available strategy is: none"))
		}
	default:
		issues = append(issues, starRocksIssue(asset, fmt.Sprintf(
			"materialization.strategy '%s' is not supported for StarRocks materialized views; available strategies are: [none create+replace]",
			asset.Materialization.Strategy,
		)))
	}

	if strings.TrimSpace(asset.StarRocks.TableModel) != "" {
		issues = append(issues, starRocksIssue(asset, "starrocks.table_model is not supported for materialized views"))
	}

	issues = append(issues, validateStarRocksLayout(asset, pipeline.MaterializationType(starRocksMaterializationTypeMaterializedView), mode)...)
	if mode == starRocksMaterializationModeAsync && len(asset.Materialization.ClusterBy) == 0 && config.Refresh == nil {
		issues = append(issues, starRocksIssue(asset, "StarRocks async materialized view requires materialization.cluster_by or starrocks.materialization.refresh"))
	}
	if mode == starRocksMaterializationModeSync {
		if len(asset.Materialization.ClusterBy) > 0 {
			issues = append(issues, starRocksIssue(asset, "starrocks.materialization.mode 'sync' does not support materialization.cluster_by"))
		}
		if strings.TrimSpace(asset.Materialization.PartitionBy) != "" {
			issues = append(issues, starRocksIssue(asset, "starrocks.materialization.mode 'sync' does not support materialization.partition_by"))
		}
		if len(asset.StarRocks.OrderBy) > 0 {
			issues = append(issues, starRocksIssue(asset, "starrocks.materialization.mode 'sync' does not support starrocks.order_by"))
		}
		if asset.StarRocks.Buckets != 0 {
			issues = append(issues, starRocksIssue(asset, "starrocks.materialization.mode 'sync' does not support starrocks.buckets"))
		}
		if config.Refresh != nil {
			issues = append(issues, starRocksIssue(asset, "starrocks.materialization.mode 'sync' does not support starrocks.materialization.refresh"))
		}
	}
	if config.Refresh != nil {
		issues = append(issues, validateStarRocksRefresh(asset, config.Refresh)...)
	}
	return issues
}

func validateStarRocksLayout(asset *pipeline.Asset, materializationType pipeline.MaterializationType, mode string) []*Issue {
	issues := make([]*Issue, 0)
	for _, column := range asset.Materialization.ClusterBy {
		if strings.TrimSpace(column) == "" {
			issues = append(issues, starRocksIssue(asset, "materialization.cluster_by entries cannot be empty"))
		}
	}
	for _, column := range asset.StarRocks.OrderBy {
		if strings.TrimSpace(column) == "" {
			issues = append(issues, starRocksIssue(asset, "starrocks.order_by entries cannot be empty"))
		}
	}

	if len(asset.StarRocks.OrderBy) > 0 && materializationType != pipeline.MaterializationTypeTable && materializationType != pipeline.MaterializationType(starRocksMaterializationTypeMaterializedView) {
		issues = append(issues, starRocksIssue(asset, "starrocks.order_by is only supported for tables and materialized views"))
	}
	if asset.StarRocks.Buckets < 0 {
		issues = append(issues, starRocksIssue(asset, "starrocks.buckets must be greater than zero"))
	} else if asset.StarRocks.Buckets > 0 &&
		materializationType == pipeline.MaterializationType(starRocksMaterializationTypeMaterializedView) &&
		len(asset.Materialization.ClusterBy) == 0 && mode != starRocksMaterializationModeSync {
		issues = append(issues, starRocksIssue(asset, "starrocks.buckets requires materialization.cluster_by"))
	}
	return issues
}

func validateStarRocksRefresh(asset *pipeline.Asset, refresh *pipeline.StarRocksRefresh) []*Issue {
	issues := make([]*Issue, 0)
	trigger := strings.ToLower(strings.TrimSpace(refresh.Trigger))
	if trigger != "" && trigger != "immediate" && trigger != "deferred" {
		issues = append(issues, starRocksIssue(asset, "starrocks.materialization.refresh.trigger must be 'immediate' or 'deferred'"))
	}

	mode := strings.ToLower(strings.TrimSpace(refresh.Mode))
	if mode != "" && mode != starRocksRefreshModeAsync && mode != starRocksRefreshModeManual {
		issues = append(issues, starRocksIssue(asset, "starrocks.materialization.refresh.mode must be 'async' or 'manual'"))
	}
	start := strings.TrimSpace(refresh.Start)
	every := strings.TrimSpace(refresh.Every)
	if mode == starRocksRefreshModeManual && (start != "" || every != "") {
		issues = append(issues, starRocksIssue(asset, "starrocks.materialization.refresh.start and every require refresh.mode 'async'"))
	}
	if start != "" && every == "" {
		issues = append(issues, starRocksIssue(asset, "starrocks.materialization.refresh.start requires starrocks.materialization.refresh.every"))
	}
	if every != "" {
		issues = append(issues, validateStarRocksRefreshInterval(asset, every)...)
	}
	return issues
}

func validateStarRocksRefreshInterval(asset *pipeline.Asset, every string) []*Issue {
	parts := strings.Fields(every)
	if len(parts) != 2 {
		return []*Issue{starRocksIssue(asset, "starrocks.materialization.refresh.every must be in the form '<count> <unit>', e.g. '1 day'")}
	}
	count, err := strconv.Atoi(parts[0])
	if err != nil || count <= 0 {
		return []*Issue{starRocksIssue(asset, "starrocks.materialization.refresh.every count must be a positive integer")}
	}
	switch strings.ToUpper(parts[1]) {
	case "DAY", "HOUR", "MINUTE", "SECOND":
		return nil
	default:
		return []*Issue{starRocksIssue(asset, "starrocks.materialization.refresh.every unit must be DAY, HOUR, MINUTE, or SECOND")}
	}
}

func starRocksIssue(asset *pipeline.Asset, description string) *Issue {
	return &Issue{Task: asset, Description: description}
}
