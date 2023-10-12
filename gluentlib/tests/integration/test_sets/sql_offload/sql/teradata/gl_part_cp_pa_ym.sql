CREATE TABLE gl_part_columnar_pa_ym
( id BIGINT
, dt DATE
, ts TIMESTAMP(0)
, ts_year INTEGER
, ts_month INTEGER
, cat INTEGER
, flag CHAR(1)
, data VARCHAR(30))
PRIMARY AMP (ts_year, ts_month)
PARTITION BY(COLUMN);

INSERT INTO gl_part_columnar_pa_ym
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

