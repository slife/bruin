/* @bruin
name: bruin_test.mv_async
type: starrocks.sql

materialization:
  partition_by: date_trunc('day', event_date)
  cluster_by: [id]

starrocks:
  materialization:
    type: materialized_view
    mode: async
    refresh:
      mode: async
      start: "2024-01-01 10:00:00"
      every: 1 day
      refresh_on_run: true
  buckets: 2
  order_by: [id]

depends:
  - bruin_test.mv_async_src
@bruin */

SELECT id, event_date, value
FROM `bruin_test`.`mv_async_src`
