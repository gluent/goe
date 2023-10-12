create table gl_null_in_string
( id number
, ch_col char(7)
, vc_col varchar(7)
);

INSERT INTO gl_null_in_string
SELECT id, n, n
FROM (
    SELECT id
    , CASE id
      WHEN 1 THEN 'normal'
      WHEN 2 THEN 'null'
      WHEN 3 THEN '"null"'
      WHEN 4 THEN '''null'''
      ELSE NULL
      END n
    FROM generated_ids
    WHERE id <= 5
) v
;
