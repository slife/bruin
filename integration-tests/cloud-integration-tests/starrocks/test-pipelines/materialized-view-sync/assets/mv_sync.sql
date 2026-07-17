/* @bruin
name: bruin_test.mv_sync
type: starrocks.sql

starrocks:
  materialization:
    type: materialized_view
    mode: sync

depends:
  - bruin_test.mv_sync_src
@bruin */

SELECT id, sum(amount) AS total FROM bruin_test.mv_sync_src GROUP BY id
