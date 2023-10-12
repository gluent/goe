
set serveroutput on
whenever sqlerror exit

prompt
prompt Generating additional COSTS data based on SALES facts...

select to_char(sysdate, 'dd/mm/yyyy hh24:mi:ss') as start_time 
from   dual;

prompt
prompt Enabling PDML...
alter session force parallel dml parallel 8;

prompt
prompt Managing constraints and indexes...
alter table costs disable constraint costs_channel_fk;
alter table costs disable constraint costs_product_fk;
alter table costs disable constraint costs_promo_fk;
alter table costs disable constraint costs_time_fk;
alter index costs_prod_bix unusable;
alter index costs_time_bix unusable;

prompt
prompt Creating temporary table...
declare
    x exception;
    pragma exception_init(x, -942);
begin
    execute immediate 'truncate table prices_gtt';
    execute immediate 'drop table prices_gtt';
exception
    when x then
        null;
end;
/
create global temporary table prices_gtt
( time_id     date
, prod_id     number
, unit_cost   number
, unit_price  number
)
on commit preserve rows;
create unique index prices_gtt_pk on prices_gtt(time_id, prod_id);

prompt
prompt Generating data...
declare
    v_min_date  date;
    v_max_date  date;
    v_time_id   date;
    v_num_dates pls_integer;
    v_date_num  pls_integer := 1;
    v_rows      pls_integer := 0;
begin
    dbms_application_info.set_module('costs_generator.sql','Fetching initialisation data...');
    select min(time_id)
    ,      max(time_id)
    ,     (max(time_id)-min(time_id))+1
    into   v_min_date
    ,      v_max_date
    ,      v_num_dates
    from   sales;
    v_time_id := v_min_date;
    dbms_application_info.set_action('Caching prices data...');
    insert into prices_gtt ( time_id
                           , prod_id
                           , unit_cost
                           , unit_price
                           )
    select time_id
    ,      prod_id
    ,      coalesce(min(unit_cost), round(max(prod_list_price)/dbms_random.value(1,1.75), 2)) as unit_cost
    ,      coalesce(max(unit_price), max(prod_list_price))                                    as unit_price
    from   products
           left outer join 
           costs 
           partition by (time_id)
           using (prod_id)
    group  by 
           time_id 
    ,      prod_id;
    commit;
    dbms_application_info.set_action('Loading COSTS');
    while v_time_id <= v_max_date loop
        insert /*+ append */ into costs ( prod_id
                                        , time_id
                                        , promo_id
                                        , channel_id
                                        , unit_cost
                                        , unit_price
                                        )
        with missing_costs as (
                select prod_id, time_id, promo_id, channel_id 
                from   sales 
                where  time_id = v_time_id
                minus
                select prod_id, time_id, promo_id, channel_id 
                from   costs 
                where  time_id = v_time_id
                )
        select c.prod_id
        ,      c.time_id
        ,      c.promo_id
        ,      c.channel_id
        ,      p.unit_cost
        ,      p.unit_price
        from   missing_costs c
               inner join
               prices_gtt    p
               on (    p.time_id = c.time_id
                   and p.prod_id = c.prod_id);
        v_rows := v_rows + sql%rowcount;
        commit;
        dbms_application_info.set_client_info('Loaded ' || to_char(v_date_num) || ' dates of ' || to_char(v_num_dates));
        v_time_id  := v_time_id + 1;
        v_date_num := v_date_num + 1;
    end loop;
    dbms_output.put_line(to_char(v_rows) || ' additional COSTS rows generated.');
end;
/

prompt 
prompt Managing constraints and indexes...
alter session force parallel ddl parallel 8;
alter table costs parallel 8;
declare
    procedure enable_constraint ( p_table_name in varchar2, p_constraint_name in varchar2, p_step in number, p_total in number ) is
    begin
        dbms_application_info.set_action('Enabling constraint ' || p_constraint_name);
        dbms_application_info.set_client_info('Constraint ' || to_char(p_step) || ' of ' || to_char(p_total));
        execute immediate 'alter table ' || p_table_name || ' enable novalidate constraint ' || p_constraint_name;
        execute immediate 'alter table ' || p_table_name || ' enable constraint ' || p_constraint_name;
    end enable_constraint;
begin
   enable_constraint('COSTS', 'COSTS_CHANNEL_FK',  1, 4);
   enable_constraint('COSTS', 'COSTS_PRODUCT_FK',  2, 4);
   enable_constraint('COSTS', 'COSTS_PROMO_FK',    3, 4);
   enable_constraint('COSTS', 'COSTS_TIME_FK',     4, 4);
end;
/
alter table costs noparallel;
declare
    procedure rebuild ( p_index_name in varchar2, p_step in number, p_total in number ) is
    begin
        dbms_application_info.set_action('Rebuilding ' || p_index_name);
        dbms_application_info.set_client_info('Index ' || to_char(p_step) || ' of ' || to_char(p_total));
        for r in (select partition_name from user_ind_partitions where index_name = p_index_name and status = 'UNUSABLE' order by partition_position) loop
            execute immediate 'alter index ' || p_index_name || ' rebuild partition ' || r.partition_name || ' parallel 8 nologging';
        end loop;
        execute immediate 'alter index ' || p_index_name || ' noparallel';
    end rebuild;
begin
    rebuild('COSTS_PROD_BIX', 1, 2);
    rebuild('COSTS_TIME_BIX', 2, 2);
end ;
/

prompt
prompt Removing temporary table...
begin
    dbms_application_info.set_action('Dropping temporary table');
    dbms_application_info.set_client_info(null);
    execute immediate 'truncate table prices_gtt';
    execute immediate 'drop table prices_gtt';
end;
/

prompt
prompt Gathering stats...
begin
    dbms_application_info.set_action('Gathering stats');
    dbms_application_info.set_client_info(null);
    dbms_stats.gather_table_stats(user, 'COSTS', degree=>8, method_opt=>'for all columns size 1');
end;
/

prompt
prompt Resetting PDML and PDDL...
alter session disable parallel dml;
alter session disable parallel ddl;

select to_char(sysdate, 'dd/mm/yyyy hh24:mi:ss') as end_time 
from   dual;

begin
    dbms_application_info.set_action('Completed');
    dbms_application_info.set_client_info(null);
end;
/

prompt
prompt COSTS rows successfully generated.
prompt
