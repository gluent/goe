
define _app_schema = &1
define _hybrid_schema = &_app_schema._H

prompt *********************************************************************************
prompt
prompt Creating test objects in the following schemas:
prompt
prompt * Application Schema = &_app_schema
prompt * Hybrid Schema      = &_hybrid_schema
prompt
prompt Enter to Continue, Ctrl-C to Cancel
prompt
prompt *********************************************************************************
prompt

pause

alter session set current_schema = &_app_schema;

declare
   procedure do_drop ( p_type in varchar2, p_name in varchar2 ) is
      x_no_plsql exception;
      pragma exception_init(x_no_plsql, -04043);
   begin
      execute immediate 'drop ' || p_type || ' ' || p_name || case when upper(p_type) = 'TYPE' then ' force' end;
   exception
      when x_no_plsql then
         null;
   end do_drop;
begin
   do_drop('TYPE', 'SALES_OT');
   do_drop('TYPE', 'SALES_NTT');
   do_drop('FUNCTION', 'SALES_PIPED_STATIC');
   do_drop('FUNCTION', 'SALES_PIPED_CURSOR');
   do_drop('FUNCTION', 'SALES_FN');
   do_drop('FUNCTION', 'CLOB_TO_BLOB');
end;
/


create or replace type sales_ot as object
( prod_id       number
, cust_id       number
, time_id       date
, channel_id    number
, promo_id      number
, quantity_sold number(10,2)
, amount_sold   number(10,2)
);
/

create or replace type sales_ntt as table of sales_ot;
/

create or replace function sales_piped_static ( p_prod_id in integer default 20 )
    return sales_ntt pipelined authid current_user as
begin
    for r in (select * from sales where prod_id = p_prod_id) loop
        pipe row (sales_ot(r.prod_id, r.cust_id, r.time_id, r.channel_id,
                           r.promo_id, r.quantity_sold, r.amount_sold));
    end loop;
    return;
end sales_piped_static;
/

create or replace function sales_piped_cursor ( p_cursor in sys_refcursor )
    return sales_ntt pipelined parallel_enable (partition p_cursor by any)
    authid current_user as
    r sales%rowtype;
begin
    loop
        fetch p_cursor into r;
        exit when p_cursor%notfound;
        pipe row (sales_ot(r.prod_id, r.cust_id, r.time_id, r.channel_id,
                           r.promo_id, r.quantity_sold, r.amount_sold));
    end loop;
    close p_cursor;
    return;
end sales_piped_cursor;
/

create or replace function sales_fn ( p_prod_id in integer default 20 )
    return number authid current_user as
    n pls_integer := 0;
begin
    for r in (select *
              from   table(sales_piped_cursor(cursor(select * from sales where prod_id = p_prod_id))))
    loop
        n := n + 1;
    end loop;
    return n;
end;
/

create or replace function clob_to_blob( p_clob in clob )
    return blob
as
    l_blob        blob;
    l_dest_offset integer := 1;
    l_src_offset  integer := 1;
    l_warn        integer;
    l_ctx         integer := dbms_lob.default_lang_ctx;
begin
    dbms_lob.createtemporary(l_blob, false );
    dbms_lob.converttoblob(l_blob, p_clob, dbms_lob.lobmaxsize, l_dest_offset, l_src_offset, dbms_lob.default_csid, l_ctx, l_warn);
    return l_blob;
end;
/

--grant execute on &_app_schema..sales_ot to &_hybrid_schema;
--grant execute on &_app_schema..sales_ntt to &_hybrid_schema;
--grant execute on &_app_schema..sales_piped_static to &_hybrid_schema;
--grant execute on &_app_schema..sales_piped_cursor to &_hybrid_schema;
--grant execute on &_app_schema..sales_fn to &_hybrid_schema;
--grant execute on &_app_schema..clob_to_blob to &_hybrid_schema;

--create or replace synonym &_hybrid_schema..sales_ot for &_app_schema..sales_ot;
--create or replace synonym &_hybrid_schema..sales_ntt for &_app_schema..sales_ntt;
--create or replace synonym &_hybrid_schema..sales_piped_static for &_app_schema..sales_piped_static;
--create or replace synonym &_hybrid_schema..sales_piped_cursor for &_app_schema..sales_piped_cursor;
--create or replace synonym &_hybrid_schema..sales_fn for &_app_schema..sales_fn;
--create or replace synonym &_hybrid_schema..clob_to_blob for &_app_schema..clob_to_blob;

--@@create_jfpd_test_objects.sql &_app_schema

undefine 1
undefine _app_schema
undefine _hybrid_schema
