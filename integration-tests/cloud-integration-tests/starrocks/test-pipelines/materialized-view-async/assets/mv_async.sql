/* @bruin
name: bruin_test.mv_async
type: starrocks.sql

materialization:
  type: materialized_view
  cluster_by: [id]

starrocks:
  buckets: 2
  refresh:
    mode: async

depends:
  - bruin_test.mv_async_src
@bruin */

SELECT id, event_date, value
FROM `bruin_test`.`mv_async_src`
