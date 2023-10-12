create table gl_null_in_string
( id number
, ch_col char(7)
, vc_col varchar2(7)
, nc_col nchar(7)
, nvc_col nvarchar2(7)
);

insert into gl_null_in_string
select 1, 'normal', 'normal', 'normal', 'normal' from dual
union all
select 2, 'null', 'null', 'null', 'null' from dual
union all
select 3, '"null"', '"null"', '"null"', '"null"' from dual
union all
select 4, '''null''', '''null''', '''null''', '''null''' from dual
union all
select 5, NULL, NULL, NULL, NULL from dual
;
