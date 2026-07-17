package starrocks

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const selectIDFromAnalyticsSource = "SELECT id FROM analytics.src"

func TestMaterializer_Render(t *testing.T) {
	t.Parallel()

	falsePtr := func() *bool {
		v := false
		return &v
	}()

	tests := []struct {
		name        string
		asset       *pipeline.Asset
		query       string
		fullRefresh bool
		wantErr     string
		want        string
	}{
		{
			name: "returns raw query when materialization disabled",
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeNone},
			},
			query: "SELECT 1",
			want:  "SELECT 1",
		},
		{
			name: "renders view with drop and create",
			asset: &pipeline.Asset{
				Name:            "analytics.daily_orders",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeView},
			},
			query: "SELECT 1",
			want:  "DROP VIEW IF EXISTS `analytics`.`daily_orders`;\nCREATE VIEW `analytics`.`daily_orders` AS\nSELECT 1;",
		},
		{
			name: "table defaults to create replace with single replica",
			asset: &pipeline.Asset{
				Name:            "analytics.orders",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
			},
			query: "SELECT * FROM source",
			want: "DROP TABLE IF EXISTS `analytics`.`orders`;\n" +
				"CREATE TABLE `analytics`.`orders`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT * FROM source;",
		},
		{
			name: "StarRocks table override wins over shared view type",
			asset: &pipeline.Asset{
				Name:            "analytics.orders",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeView},
				StarRocks: pipeline.StarRocksConfig{Materialization: &pipeline.StarRocksMaterializationConfig{
					Type: "table",
				}},
			},
			query: "SELECT * FROM source",
			want: "DROP TABLE IF EXISTS `analytics`.`orders`;\n" +
				"CREATE TABLE `analytics`.`orders`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT * FROM source;",
		},
		{
			name: "StarRocks view override wins over shared table type",
			asset: &pipeline.Asset{
				Name:            "analytics.daily_orders",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
				StarRocks: pipeline.StarRocksConfig{Materialization: &pipeline.StarRocksMaterializationConfig{
					Type: "view",
				}},
			},
			query: "SELECT 1",
			want:  "DROP VIEW IF EXISTS `analytics`.`daily_orders`;\nCREATE VIEW `analytics`.`daily_orders` AS\nSELECT 1;",
		},
		{
			name: "append emits insert",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query: "SELECT * FROM staging",
			want:  "INSERT INTO `analytics`.`orders` SELECT * FROM staging;",
		},
		{
			name: "truncate insert swaps replacement table",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyTruncateInsert,
				},
			},
			query: "SELECT * FROM staging",
			want: "DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"CREATE TABLE `analytics`.`__bruin_tmp_orders_abcefghi_replacement`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT * FROM staging;\n" +
				"CREATE TABLE IF NOT EXISTS `analytics`.`orders` LIKE `analytics`.`__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"ALTER TABLE `analytics`.`orders` SWAP WITH `__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_replacement`;",
		},
		{
			name: "incremental requires key",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:   "SELECT id FROM source",
			wantErr: "requires the `incremental_key` field to be set",
		},
		{
			name: "incremental builds delete insert with swap",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "id",
				},
			},
			query: "SELECT id, value FROM source",
			want: "DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_new`;\n" +
				"CREATE TABLE `analytics`.`__bruin_tmp_orders_abcefghi_new`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT id, value FROM source;\n" +
				"DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"CREATE TABLE `analytics`.`__bruin_tmp_orders_abcefghi_replacement`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT `target`.*\n" +
				"FROM `analytics`.`orders` AS `target`\n" +
				"WHERE NOT EXISTS (\n" +
				"  SELECT 1\n" +
				"  FROM `analytics`.`__bruin_tmp_orders_abcefghi_new` AS `new_rows`\n" +
				"  WHERE (`target`.`id` = `new_rows`.`id` OR (`target`.`id` IS NULL AND `new_rows`.`id` IS NULL))\n" +
				")\n" +
				"UNION ALL\n" +
				"SELECT * FROM `analytics`.`__bruin_tmp_orders_abcefghi_new`;\n" +
				"ALTER TABLE `analytics`.`orders` SWAP WITH `__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_replacement`;\n" +
				"DROP TABLE IF EXISTS `analytics`.`__bruin_tmp_orders_abcefghi_new`;",
		},
		{
			name: "time interval",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "event_time",
					TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
				},
			},
			query: "SELECT * FROM staging",
			want: "DELETE FROM `analytics`.`orders` WHERE `event_time` BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}';\n" +
				"INSERT INTO `analytics`.`orders` SELECT * FROM staging;",
		},
		{
			name: "ddl builds duplicate key table",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", Nullable: pipeline.DefaultTrueBool{Value: falsePtr}},
					{Name: "description", Type: "VARCHAR(255)", Description: "product info"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS `analytics`.`orders` (\n" +
				"`id` INT NOT NULL,\n" +
				"`description` VARCHAR(255) COMMENT 'product info'\n" +
				")\n" +
				"DUPLICATE KEY(`id`)\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
				"PROPERTIES (\"replication_num\" = \"1\");",
		},
		{
			name: "ddl emits partition by clause",
			asset: &pipeline.Asset{
				Name: "analytics.events",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyDDL,
					ClusterBy:   []string{"id"},
					PartitionBy: "event_date",
				},
				StarRocks: pipeline.StarRocksConfig{
					Buckets: 4,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT"},
					{Name: "event_date", Type: "DATE"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS `analytics`.`events` (\n" +
				"`id` INT,\n" +
				"`event_date` DATE\n" +
				")\n" +
				"DUPLICATE KEY(`id`)\n" +
				"PARTITION BY (event_date)\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 4\n" +
				"PROPERTIES (\"replication_num\" = \"1\");",
		},
		{
			name: "ddl emits expression partition by clause",
			asset: &pipeline.Asset{
				Name: "analytics.events",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyDDL,
					PartitionBy: "date_trunc('day', event_ts)",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT"},
					{Name: "event_ts", Type: "DATETIME"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS `analytics`.`events` (\n" +
				"`id` INT,\n" +
				"`event_ts` DATETIME\n" +
				")\n" +
				"DUPLICATE KEY(`id`)\n" +
				"PARTITION BY (date_trunc('day', event_ts))\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
				"PROPERTIES (\"replication_num\" = \"1\");",
		},
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
		{
			name: "table with order_by alone triggers typed create table",
			asset: &pipeline.Asset{
				Name: "analytics.sorted_orders",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
				StarRocks: pipeline.StarRocksConfig{OrderBy: []string{"created_at"}},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT"},
					{Name: "created_at", Type: "DATETIME"},
				},
			},
			query: "SELECT id, created_at FROM src",
			want: "DROP TABLE IF EXISTS `analytics`.`sorted_orders`;\n" +
				"CREATE TABLE `analytics`.`sorted_orders` (\n" +
				"`id` INT,\n" +
				"`created_at` DATETIME\n" +
				")\n" +
				"DUPLICATE KEY(`id`)\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
				"ORDER BY (`created_at`)\n" +
				"PROPERTIES (\"replication_num\" = \"1\");\n" +
				"INSERT INTO `analytics`.`sorted_orders` (`id`, `created_at`)\n" +
				"SELECT id, created_at FROM src;",
		},
		{
			name: "merge creates primary key table and upserts",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "status", Type: "VARCHAR(20)", UpdateOnMerge: true},
					{Name: "amount", Type: "INT"},
				},
			},
			query: "SELECT id, status, amount FROM staging",
			want: "CREATE TABLE IF NOT EXISTS `analytics`.`accounts` (\n" +
				"`id` INT NOT NULL,\n" +
				"`status` VARCHAR(20),\n" +
				"`amount` INT\n" +
				")\n" +
				"PRIMARY KEY(`id`)\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
				"PROPERTIES (\"replication_num\" = \"1\");\n" +
				"INSERT INTO `analytics`.`accounts` (`id`, `status`, `amount`)\n" +
				"SELECT id, status, amount FROM staging;",
		},
		{
			name: "merge rejects per-column merge expressions",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "balance", Type: "INT", MergeSQL: "target.`balance` + source.`balance`"},
				},
			},
			query:   "SELECT id, balance FROM staging",
			wantErr: "does not support per-column merge expressions",
		},
		{
			name: "merge requires primary key",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT"},
				},
			},
			query:   "SELECT id FROM staging",
			wantErr: "requires the `primary_key` field to be set",
		},
		{
			name: "merge rejects non primary key table model",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				StarRocks: pipeline.StarRocksConfig{TableModel: "duplicate_key"},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
				},
			},
			query:   "SELECT id FROM staging",
			wantErr: "requires StarRocks table_model \"primary_key\"",
		},
		{
			name: "full refresh merge creates primary key table before insert",
			asset: &pipeline.Asset{
				Name: "analytics.accounts",
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					Strategy:  pipeline.MaterializationStrategyMerge,
					ClusterBy: []string{"account_id"},
				},
				StarRocks: pipeline.StarRocksConfig{
					Buckets:    2,
					Properties: map[string]string{"compression": "zstd"},
				},
				Columns: []pipeline.Column{
					{Name: "status", Type: "VARCHAR(20)", UpdateOnMerge: true},
					{Name: "account_id", Type: "STRING", PrimaryKey: true},
					{Name: "balance", Type: "INT"},
				},
			},
			query:       "SELECT status, account_id, balance FROM staging",
			fullRefresh: true,
			want: "DROP TABLE IF EXISTS `analytics`.`accounts`;\n" +
				"CREATE TABLE `analytics`.`accounts` (\n" +
				"`account_id` VARCHAR(65533) NOT NULL,\n" +
				"`status` VARCHAR(20),\n" +
				"`balance` INT\n" +
				")\n" +
				"PRIMARY KEY(`account_id`)\n" +
				"DISTRIBUTED BY HASH(`account_id`) BUCKETS 2\n" +
				"PROPERTIES (\"compression\" = \"zstd\", \"replication_num\" = \"1\");\n" +
				"INSERT INTO `analytics`.`accounts` (`status`, `account_id`, `balance`)\n" +
				"SELECT status, account_id, balance FROM staging;",
		},
		{
			name: "scd2 by column is not supported",
			asset: &pipeline.Asset{
				Name: "analytics.orders",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
			},
			query:   "SELECT id FROM source",
			wantErr: "materialization strategy scd2_by_column is not supported",
		},
		{
			name: "async MV minimal with distribution",
			asset: &pipeline.Asset{
				Name:            "analytics.mv_users",
				Materialization: pipeline.Materialization{ClusterBy: []string{"user_id"}},
				StarRocks: pipeline.StarRocksConfig{
					Buckets:         8,
					Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(materializationTypeMaterializedView), Mode: starRocksMaterializationModeAsync},
				},
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
					PartitionBy: "date_trunc('day', event_date)",
					ClusterBy:   []string{"user_id"},
				},
				StarRocks: pipeline.StarRocksConfig{
					Buckets:    4,
					OrderBy:    []string{"event_date", "user_id"},
					Properties: map[string]string{"partition_refresh_number": "4"},
					Materialization: &pipeline.StarRocksMaterializationConfig{
						Type:    string(materializationTypeMaterializedView),
						Mode:    starRocksMaterializationModeAsync,
						Refresh: &pipeline.StarRocksRefresh{Trigger: "deferred", Mode: starRocksRefreshModeAsync, Start: "2025-01-01 10:00:00", Every: "1 day"},
					},
				},
			},
			query: "SELECT event_date, user_id FROM analytics.events",
			want: "CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`dau`\n" +
				"DISTRIBUTED BY HASH(`user_id`) BUCKETS 4\n" +
				"REFRESH DEFERRED ASYNC START('2025-01-01 10:00:00') EVERY (INTERVAL 1 DAY)\n" +
				"PARTITION BY (date_trunc('day', event_date))\n" +
				"ORDER BY (`event_date`, `user_id`)\n" +
				"PROPERTIES (\"partition_refresh_number\" = \"4\")\n" +
				"AS\n" +
				"SELECT event_date, user_id FROM analytics.events;",
		},
		{
			name: "async MV manual mode triggers refresh on run",
			asset: &pipeline.Asset{
				Name:            "analytics.mv_manual",
				Materialization: pipeline.Materialization{ClusterBy: []string{"id"}},
				StarRocks: pipeline.StarRocksConfig{
					Buckets: 2,
					Materialization: &pipeline.StarRocksMaterializationConfig{
						Type:    string(materializationTypeMaterializedView),
						Mode:    starRocksMaterializationModeAsync,
						Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeManual},
					},
				},
			},
			query: selectIDFromAnalyticsSource,
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
				Name:            "analytics.mv_manual2",
				Materialization: pipeline.Materialization{ClusterBy: []string{"id"}},
				StarRocks: pipeline.StarRocksConfig{
					Buckets: 2,
					Materialization: &pipeline.StarRocksMaterializationConfig{
						Type:    string(materializationTypeMaterializedView),
						Mode:    starRocksMaterializationModeAsync,
						Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeManual, RefreshOnRun: boolPtr(false)},
					},
				},
			},
			query: selectIDFromAnalyticsSource,
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
				Materialization: pipeline.Materialization{},
				StarRocks: pipeline.StarRocksConfig{
					Properties:      map[string]string{"replication_num": "1"},
					Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(materializationTypeMaterializedView), Mode: starRocksMaterializationModeSync},
				},
			},
			query: "SELECT store_id, sum(amount) AS total FROM analytics.sales GROUP BY store_id",
			want: "CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`sales_rollup`\n" +
				"PROPERTIES (\"replication_num\" = \"1\")\n" +
				"AS\n" +
				"SELECT store_id, sum(amount) AS total FROM analytics.sales GROUP BY store_id;",
		},
		{
			name: "sync MV rejects full refresh",
			asset: &pipeline.Asset{
				Name: "analytics.sales_rollup",
				StarRocks: pipeline.StarRocksConfig{
					Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(materializationTypeMaterializedView), Mode: starRocksMaterializationModeSync},
				},
			},
			query:       "SELECT store_id, sum(amount) FROM analytics.sales GROUP BY store_id",
			fullRefresh: true,
			wantErr:     "full refresh is not supported for StarRocks sync materialized views",
		},
		{
			name: "sync MV rejects create replace strategy",
			asset: &pipeline.Asset{
				Name:            "analytics.sales_rollup",
				Materialization: pipeline.Materialization{Strategy: pipeline.MaterializationStrategyCreateReplace},
				StarRocks: pipeline.StarRocksConfig{
					Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(materializationTypeMaterializedView), Mode: starRocksMaterializationModeSync},
				},
			},
			query:   "SELECT store_id, sum(amount) FROM analytics.sales GROUP BY store_id",
			wantErr: "create+replace is not supported for StarRocks sync materialized views",
		},
		{
			name: "MV rejects unknown outer mode",
			asset: &pipeline.Asset{
				Name:            "analytics.bad_mode",
				Materialization: pipeline.Materialization{ClusterBy: []string{"id"}},
				StarRocks: pipeline.StarRocksConfig{
					Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(materializationTypeMaterializedView), Mode: "eventual"},
				},
			},
			query:   "SELECT id FROM analytics.sales",
			wantErr: "starrocks.materialization.mode",
		},
		{
			name: "async MV full refresh drops and recreates",
			asset: &pipeline.Asset{
				Name:            "analytics.mv_fr",
				Materialization: pipeline.Materialization{ClusterBy: []string{"id"}},
				StarRocks: pipeline.StarRocksConfig{
					Buckets: 2,
					Materialization: &pipeline.StarRocksMaterializationConfig{
						Type:    string(materializationTypeMaterializedView),
						Mode:    starRocksMaterializationModeAsync,
						Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync},
					},
				},
			},
			query:       selectIDFromAnalyticsSource,
			fullRefresh: true,
			want: "DROP MATERIALIZED VIEW IF EXISTS `analytics`.`mv_fr`;\n" +
				"CREATE MATERIALIZED VIEW `analytics`.`mv_fr`\n" +
				"DISTRIBUTED BY HASH(`id`) BUCKETS 2\n" +
				"REFRESH ASYNC\n" +
				"AS\n" +
				"SELECT id FROM analytics.src;",
		},
		{
			name: "async MV full refresh respects refresh restriction",
			asset: &pipeline.Asset{
				Name:              "analytics.mv_restricted",
				Materialization:   pipeline.Materialization{ClusterBy: []string{"id"}},
				RefreshRestricted: boolPtr(true),
				StarRocks: pipeline.StarRocksConfig{
					Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(materializationTypeMaterializedView), Mode: starRocksMaterializationModeAsync},
				},
			},
			query:       selectIDFromAnalyticsSource,
			fullRefresh: true,
			want: "CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`mv_restricted`\n" +
				"DISTRIBUTED BY HASH(`id`)\n" +
				"AS\n" +
				"SELECT id FROM analytics.src;",
		},
		{
			name: "sync MV rejects refresh",
			asset: &pipeline.Asset{
				Name:            "analytics.bad_sync",
				Materialization: pipeline.Materialization{},
				StarRocks: pipeline.StarRocksConfig{Materialization: &pipeline.StarRocksMaterializationConfig{
					Type:    string(materializationTypeMaterializedView),
					Mode:    "sync",
					Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync},
				}},
			},
			query:   "SELECT 1",
			wantErr: "sync",
		},
		{
			name: "async MV requires distribution or refresh",
			asset: &pipeline.Asset{
				Name:            "analytics.bad_async",
				Materialization: pipeline.Materialization{},
				StarRocks: pipeline.StarRocksConfig{Materialization: &pipeline.StarRocksMaterializationConfig{
					Type: string(materializationTypeMaterializedView),
					Mode: starRocksMaterializationModeAsync,
				}},
			},
			query:   "SELECT 1",
			wantErr: "distribution",
		},
		{
			name: "async MV rejects negative buckets without distribution",
			asset: &pipeline.Asset{
				Name:            "analytics.bad_buckets",
				Materialization: pipeline.Materialization{},
				StarRocks: pipeline.StarRocksConfig{
					Buckets: -1,
					Materialization: &pipeline.StarRocksMaterializationConfig{
						Type:    string(materializationTypeMaterializedView),
						Mode:    starRocksMaterializationModeAsync,
						Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync},
					},
				},
			},
			query:   "SELECT 1",
			wantErr: "buckets",
		},
		{
			name: "async MV rejects buckets without distribution",
			asset: &pipeline.Asset{
				Name:            "analytics.bad_buckets_no_distribution",
				Materialization: pipeline.Materialization{},
				StarRocks: pipeline.StarRocksConfig{
					Buckets: 4,
					Materialization: &pipeline.StarRocksMaterializationConfig{
						Type:    string(materializationTypeMaterializedView),
						Mode:    starRocksMaterializationModeAsync,
						Refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync},
					},
				},
			},
			query:   "SELECT 1",
			wantErr: "buckets",
		},
		{
			name: "async MV rejects empty order by keys",
			asset: &pipeline.Asset{
				Name:            "analytics.bad_order",
				Materialization: pipeline.Materialization{ClusterBy: []string{"id"}},
				StarRocks: pipeline.StarRocksConfig{
					OrderBy:         []string{" "},
					Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(materializationTypeMaterializedView), Mode: starRocksMaterializationModeAsync},
				},
			},
			query:   selectIDFromAnalyticsSource,
			wantErr: "order_by",
		},
		{
			name: "async MV rejects negative buckets with distribution",
			asset: &pipeline.Asset{
				Name:            "analytics.bad_buckets_dist",
				Materialization: pipeline.Materialization{ClusterBy: []string{"id"}},
				StarRocks: pipeline.StarRocksConfig{
					Buckets:         -1,
					Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(materializationTypeMaterializedView), Mode: starRocksMaterializationModeAsync},
				},
			},
			query:   selectIDFromAnalyticsSource,
			wantErr: "buckets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := NewMaterializer(tt.fullRefresh).Render(tt.asset, tt.query)
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

func boolPtr(b bool) *bool { return &b }

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "`analytics`.`orders`", quoteIdentifier("analytics.orders"))
	assert.Equal(t, "`odd``name`", quoteIdentifier("odd`name"))
}

func TestBuildRefreshClause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		refresh *pipeline.StarRocksRefresh
		want    string
		wantErr string
	}{
		{name: "nil refresh yields empty", refresh: nil, want: ""},
		{name: "manual", refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeManual}, want: "REFRESH MANUAL"},
		{name: "async bare", refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync}, want: "REFRESH ASYNC"},
		{
			name:    "deferred async scheduled",
			refresh: &pipeline.StarRocksRefresh{Trigger: "deferred", Mode: starRocksRefreshModeAsync, Start: "2025-01-01 10:00:00", Every: "1 day"},
			want:    "REFRESH DEFERRED ASYNC START('2025-01-01 10:00:00') EVERY (INTERVAL 1 DAY)",
		},
		{
			name:    "immediate async every only",
			refresh: &pipeline.StarRocksRefresh{Trigger: "immediate", Mode: starRocksRefreshModeAsync, Every: "30 minute"},
			want:    "REFRESH IMMEDIATE ASYNC EVERY (INTERVAL 30 MINUTE)",
		},
		{name: "bad trigger", refresh: &pipeline.StarRocksRefresh{Trigger: "eventually", Mode: starRocksRefreshModeAsync}, wantErr: "refresh.trigger"},
		{name: "bad mode", refresh: &pipeline.StarRocksRefresh{Mode: "sometimes"}, wantErr: "refresh.mode"},
		{name: "start with manual", refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeManual, Start: "2025-01-01 10:00:00"}, wantErr: "start"},
		{name: "async start without every", refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync, Start: "2025-01-01 10:00:00"}, wantErr: "refresh.start requires refresh.every"},
		{name: "malformed every", refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync, Every: "soon"}, wantErr: "every"},
		{name: "zero every count", refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync, Every: "0 day"}, wantErr: "positive integer"},
		{name: "non-numeric every count", refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync, Every: "many days"}, wantErr: "positive integer"},
		{name: "unsupported every unit", refresh: &pipeline.StarRocksRefresh{Mode: starRocksRefreshModeAsync, Every: "1 week"}, wantErr: "DAY, HOUR, MINUTE, or SECOND"},
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

func TestRenderMaterializedViewRejectsUnknownMode(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name:            "analytics.bad_mode",
		Materialization: pipeline.Materialization{ClusterBy: []string{"id"}},
		StarRocks: pipeline.StarRocksConfig{
			Materialization: &pipeline.StarRocksMaterializationConfig{Type: string(materializationTypeMaterializedView), Mode: "eventual"},
		},
	}

	_, err := renderMaterializedView(asset, "SELECT id FROM analytics.sales", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "starrocks.materialization.mode")
}
