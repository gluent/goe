CREATE TABLE gl_part_range_ts3_pi_id
( id BIGINT
, dt DATE
, ts TIMESTAMP(3)
, ts_year INTEGER
, ts_month INTEGER
, cat INTEGER
, flag CHAR(1)
, data VARCHAR(30))
PRIMARY INDEX (id)
PARTITION BY(
    RANGE_N(ts BETWEEN TIMESTAMP '2018-01-01 00:00:00.000+00:00' AND TIMESTAMP '2025-12-31 23:59:59.999+00:00' EACH INTERVAL '1' MONTH)
);


INSERT INTO gl_part_range_ts3_pi_id
WITH v AS (
    SELECT id,  TIMESTAMP '2020-01-01 00:00:00' + NUMTODSINTERVAL(id, 'HOUR') AS ts
    FROM   generated_ids
    WHERE  id <= 24*365*2)
SELECT id
,      CAST(ts AS DATE) dt
,      ts
,      EXTRACT(YEAR FROM ts) ts_year
,      EXTRACT(MONTH FROM ts) ts_month
,      MOD(id,4) AS cat
,      SUBSTR('ABCDE',Random(1,5),1) flag
,      'blah' AS data
FROM v;

