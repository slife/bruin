/* @bruin
name: bruin_test.mv_async_src
type: starrocks.sql

materialization:
  type: table
  strategy: create+replace
  partition_by: event_date
  cluster_by: [id]

starrocks:
  buckets: 2

columns:
  - name: id
    type: INT
  - name: event_date
    type: DATE
  - name: value
    type: INT
@bruin */

SELECT 1 AS id, CAST('2024-01-01' AS DATE) AS event_date, 10 AS value
UNION ALL
SELECT 2 AS id, CAST('2024-01-02' AS DATE) AS event_date, 5 AS value
