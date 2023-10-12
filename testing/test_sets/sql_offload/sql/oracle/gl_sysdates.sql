CREATE TABLE gl_sysdates
( id  NUMBER(10)
, dt  DATE
)
;

INSERT INTO gl_sysdates
SELECT ROWNUM, SYSDATE + DBMS_RANDOM.VALUE(-1000,1000)
FROM   dual
CONNECT BY ROWNUM <= 10000
;
