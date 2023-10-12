CREATE TABLE gl_part_case_dt_pi_id
( id BIGINT
, dt DATE
, ts TIMESTAMP(0)
, ts_year INTEGER
, ts_month INTEGER
, cat INTEGER
, flag CHAR(1)
, data VARCHAR(30))
PRIMARY INDEX (id)
PARTITION BY(
    CASE_N(dt BETWEEN DATE '2020-01-01' AND DATE '2020-12-31',
           dt BETWEEN DATE '2021-01-01' AND DATE '2021-12-31',
           dt BETWEEN DATE '2022-01-01' AND DATE '2022-12-31',
           dt BETWEEN DATE '2023-01-01' AND DATE '2023-12-31',
           dt BETWEEN DATE '2024-01-01' AND DATE '2024-12-31',
           UNKNOWN)
);


INSERT INTO gl_part_case_dt_pi_id
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

