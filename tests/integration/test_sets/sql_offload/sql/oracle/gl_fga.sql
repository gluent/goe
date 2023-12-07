CREATE TABLE gl_fga
( id      NUMBER
, id_desc VARCHAR2(10)
);

INSERT INTO gl_fga
SELECT  ROWNUM       AS id
,       'some text'  AS id_desc
FROM    dual
CONNECT BY ROWNUM <= 5;

exec DBMS_FGA.ADD_POLICY(SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),'GL_FGA','GL_FGA_POL','id > 2','ID' );
