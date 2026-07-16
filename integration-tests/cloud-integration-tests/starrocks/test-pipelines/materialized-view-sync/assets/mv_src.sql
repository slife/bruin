/* @bruin
name: bruin_test.mv_sync_src
type: starrocks.sql

materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS id, 10 AS amount
UNION ALL
SELECT 1 AS id, 5 AS amount
UNION ALL
SELECT 2 AS id, 20 AS amount
