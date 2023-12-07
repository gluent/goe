
set serveroutput on
whenever sqlerror exit

prompt Target number of sales facts...
define _gen_rows = &1

prompt
prompt Generating SALES rows up to a target of &_gen_rows....

select to_char(sysdate, 'dd/mm/yyyy hh24:mi:ss') as start_time 
from   dual;

prompt
prompt Enabling PDML...
alter session force parallel dml parallel 8;

prompt
prompt Managing constraints and indexes...
alter table sales disable constraint sales_customer_fk;
alter table sales disable constraint sales_channel_fk;
alter table sales disable constraint sales_product_fk;
alter table sales disable constraint sales_promo_fk;
alter table sales disable constraint sales_time_fk;
alter index sales_channel_bix unusable;
alter index sales_cust_bix unusable;
alter index sales_prod_bix unusable;
alter index sales_promo_bix unusable;
alter index sales_time_bix unusable;

prompt
prompt Creating temporary table...
declare
    x exception;
    pragma exception_init(x, -942);
begin
    execute immediate 'truncate table dims_gtt';
    execute immediate 'drop table dims_gtt';
exception
    when x then
        null;
end;
/
create global temporary table dims_gtt
( dim_id        number
, prod_id       number
, prod_price    number
, channel_id    number
, promo_id      number
)
on commit preserve rows;
create unique index dims_gtt_pk on dims_gtt(dim_id);

prompt
prompt Generating data...
declare
    v_min_date          date;
    v_max_date          date;
    v_time_id           date;
    v_num_rows          pls_integer;
    v_num_dates         pls_integer;
    v_cust_rows         pls_integer;
    v_dim_rows          pls_integer;
    v_rows              pls_integer := 0;
    v_target_rows       pls_integer := &_gen_rows;
    v_part_rows         pls_integer;
    v_batch_size        pls_integer;
    v_sample_size       number(8,6);
begin
    dbms_application_info.set_module('sales_generator.sql','Fetching initialisation data...');
    select min(time_id)
    ,      max(time_id)
    ,      count(*)
    ,     (max(time_id)-min(time_id))+1
    into   v_min_date
    ,      v_max_date
    ,      v_num_rows
    ,      v_num_dates
    from   sales;
    v_time_id     := v_min_date;
    v_target_rows := v_target_rows - v_num_rows;
    select count(*)
    into   v_cust_rows
    from   customers;
    dbms_application_info.set_action('Caching dimension keys...');
    insert into dims_gtt ( dim_id
                         , prod_id
                         , prod_price
                         , channel_id
                         , promo_id
                         )
    select row_number() over (order by dbms_random.value) as dim_id
    ,      prod_id
    ,      prod_list_price
    ,      channel_id
    ,      promo_id
    from   products
           cross join
           channels
           cross join
           promotions;
    v_dim_rows := sql%rowcount;
    commit;
    dbms_application_info.set_action('Loading SALES');
    while v_time_id <= v_max_date loop
        v_part_rows := trunc(v_target_rows/v_num_dates);
        v_batch_size := case
                            when v_time_id < v_max_date
                            then v_part_rows
                            else v_target_rows - ((v_num_dates-1)*v_part_rows)
                        end;
        v_sample_size := least(greatest((v_batch_size/v_cust_rows)*150, 0.000001),99.999999);
        execute immediate '
            declare
                v_time_id    date         := :v_time_id;
                v_dim_rows   pls_integer  := :v_dim_rows;
                v_batch_size pls_integer  := :v_batch_size;
            begin
                insert /*+ append */ into sales ( prod_id
                                                , cust_id
                                                , time_id
                                                , channel_id
                                                , promo_id
                                                , quantity_sold
                                                , amount_sold
                                                )
                with custs as (
                        select cust_id
                        ,      trunc(dbms_random.value(1, v_dim_rows+1)) as dim_id1
                        ,      trunc(dbms_random.value(1, v_dim_rows+1)) as dim_id2
                        from   customers sample (' || to_char(v_sample_size) || ')
                        where  v_time_id >= cust_eff_from and v_time_id < nvl(cust_eff_to, v_time_id+1)
                        and    cust_id not in (select s.cust_id from sales s where s.time_id = v_time_id)
                        )
                ,    facts as (
                        select c.cust_id
                        ,      c.dim_id1
                        ,      c.dim_id2
                        ,      d.prod_id
                        ,      d.prod_price
                        ,      d.channel_id
                        ,      d.promo_id
                        from   custs    c
                               inner join
                               dims_gtt d 
                               on (d.dim_id = c.dim_id1)
                        where  rownum <= trunc(v_batch_size*0.80)
                        union all
                        select c.cust_id
                        ,      c.dim_id1
                        ,      c.dim_id2
                        ,      d.prod_id
                        ,      d.prod_price
                        ,      d.channel_id
                        ,      d.promo_id
                        from   custs    c
                               inner join
                               dims_gtt d 
                               on (d.dim_id = c.dim_id2)
                        where  rownum <= v_batch_size-(trunc(v_batch_size*0.80))
                        )
                select prod_id
                ,      cust_id
                ,      v_time_id as time_id
                ,      channel_id
                ,      promo_id
                ,      case
                          when mod(rownum,10) = 0
                          and  prod_id between 113 and 130
                          then 1 + mod(rownum,4)
                          else 1
                       end as quantity_sold
                ,      case
                          when mod(rownum,10) = 0
                          and  prod_id between 113 and 130
                          then (1 + mod(rownum,4)) * prod_price
                          else prod_price
                       end as amount_sold
                from   facts;
                commit;
            end;'
        using in v_time_id, in v_dim_rows, in v_batch_size;
        v_time_id := v_time_id + 1;
        v_rows    := v_rows + v_batch_size;
        dbms_application_info.set_client_info('Loaded ' || to_char(v_rows) || ' of ' || to_char(v_target_rows));
    end loop;
    dbms_output.put_line(to_char(v_rows) || ' additional SALES rows generated.');
end;
/

prompt 
prompt Managing constraints and indexes...
alter session force parallel ddl parallel 8;
alter table sales parallel 8;
declare
    procedure enable_constraint ( p_table_name in varchar2, p_constraint_name in varchar2, p_step in number, p_total in number ) is
    begin
        dbms_application_info.set_action('Enabling constraint ' || p_constraint_name);
        dbms_application_info.set_client_info('Constraint ' || to_char(p_step) || ' of ' || to_char(p_total));
        execute immediate 'alter table ' || p_table_name || ' enable novalidate constraint ' || p_constraint_name;
        execute immediate 'alter table ' || p_table_name || ' enable constraint ' || p_constraint_name;
    end enable_constraint;
begin
   enable_constraint('SALES', 'SALES_CUSTOMER_FK', 1, 5);
   enable_constraint('SALES', 'SALES_CHANNEL_FK',  2, 5);
   enable_constraint('SALES', 'SALES_PRODUCT_FK',  3, 5);
   enable_constraint('SALES', 'SALES_PROMO_FK',    4, 5);
   enable_constraint('SALES', 'SALES_TIME_FK',     5, 5);
end;
/
alter table sales noparallel;
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
    rebuild('SALES_CHANNEL_BIX', 1, 5);
    rebuild('SALES_CUST_BIX',    2, 5);
    rebuild('SALES_PROD_BIX',    3, 5);
    rebuild('SALES_PROMO_BIX',   4, 5);
    rebuild('SALES_TIME_BIX',    5, 5);
end ;
/

prompt
prompt Removing temporary table...
begin
    dbms_application_info.set_action('Dropping temporary table');
    dbms_application_info.set_client_info(null);
    execute immediate 'truncate table dims_gtt';
    execute immediate 'drop table dims_gtt';
end;
/

prompt
prompt Gathering stats...
begin
    dbms_application_info.set_action('Gathering stats');
    dbms_application_info.set_client_info(null);
    dbms_stats.gather_table_stats(user, 'SALES', degree=>8, method_opt=>'for all columns size 1');
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
prompt SALES rows successfully generated.
prompt

undefine 1
undefine _gen_rows
