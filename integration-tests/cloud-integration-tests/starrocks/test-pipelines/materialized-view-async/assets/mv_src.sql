/* @bruin
name: bruin_test.mv_async_src
type: starrocks.sql

materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS id, CAST('2024-01-01' AS DATE) AS event_date, 10 AS value
UNION ALL
SELECT 2 AS id, CAST('2024-01-02' AS DATE) AS event_date, 5 AS value
