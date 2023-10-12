CREATE TABLE gl_part_cp_range_year_month_nopi
( id BIGINT
, dt DATE
, ts TIMESTAMP(0)
, ts_year INTEGER
, ts_month INTEGER
, cat INTEGER
, flag CHAR(1)
, data VARCHAR(30))
NO PRIMARY INDEX
PARTITION BY(
    COLUMN
,   RANGE_N(ts_year BETWEEN 2018 AND 2025 EACH 1)
,   RANGE_N(ts_month BETWEEN 1 AND 12 EACH 1)
);


INSERT INTO gl_part_cp_range_year_month_nopi
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

