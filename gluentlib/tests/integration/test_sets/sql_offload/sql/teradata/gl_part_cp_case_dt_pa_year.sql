CREATE TABLE gl_part_cp_case_dt_pa_year
( id BIGINT
, dt DATE
, ts TIMESTAMP(0)
, ts_year INTEGER
, ts_month INTEGER
, cat INTEGER
, flag CHAR(1)
, data VARCHAR(30))
PRIMARY AMP (ts_year)
PARTITION BY(
    COLUMN
,   CASE_N(dt BETWEEN DATE '2020-01-01' AND DATE '2020-12-31',
           dt BETWEEN DATE '2021-01-01' AND DATE '2021-12-31',
           dt BETWEEN DATE '2022-01-01' AND DATE '2022-12-31',
           dt BETWEEN DATE '2023-01-01' AND DATE '2023-12-31',
           dt BETWEEN DATE '2024-01-01' AND DATE '2024-12-31',
           UNKNOWN)
);


INSERT INTO gl_part_cp_case_dt_pa_year
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

