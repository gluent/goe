
alter table sh.sales disable constraint SALES_CUSTOMER_FK;
alter table sh.sales disable constraint SALES_PRODUCT_FK;
alter table sh.sales disable constraint SALES_TIME_FK;
alter table sh.sales disable constraint SALES_CHANNEL_FK;
alter table sh.sales disable constraint SALES_PROMO_FK;

alter index sh.SALES_CHANNEL_BIX unusable;
alter index sh.SALES_CUST_BIX unusable;
alter index sh.SALES_PROD_BIX unusable;
alter index sh.SALES_PROMO_BIX unusable;
alter index sh.SALES_TIME_BIX unusable;

declare
    rn number;
begin
    dbms_application_info.set_module('inflate_sales.sql', null);
    for r in (select time_id, 
                     row_number() over (order by time_id) as n, 
                     count(*) over () as m
              from  (select distinct time_id from sh.sales)
              order  by time_id)
    loop
        rn := dbms_random.value(225000, 235000);
        insert /*+ append parallel(16) */ 
        into  sh.sales
        with cust_data as (
                select cust_id, row_number() over (order by dbms_random.random) as join_id
                from  (
                        select cust_id from sh.customers
                        minus
                        select cust_id from sh.sales where time_id = r.time_id
                      )
                )
        ,    cost_data as (
                select c.*, row_number() over (order by dbms_random.random) as join_id 
                from   sh.costs c, table(sys.odcinumberlist(1,2))
                where  c.time_id = r.time_id
                )
        select co.prod_id, cu.cust_id, r.time_id, co.channel_id, co.promo_id, 
               1 as quantity_sold, round(co.unit_price * dbms_random.value(0.95, 1.1), 2) as amount_sold
        from   cust_data cu, cost_data co
        where  cu.join_id = co.join_id
        and    rownum <= rn;
        commit;
        dbms_application_info.set_action(to_char(r.n) || ' of ' || to_char(r.m) || ' completed.');
    end loop;
end;
/

alter table sh.sales enable novalidate constraint SALES_CUSTOMER_FK;
alter table sh.sales enable novalidate constraint SALES_PRODUCT_FK;
alter table sh.sales enable novalidate constraint SALES_TIME_FK;
alter table sh.sales enable novalidate constraint SALES_CHANNEL_FK;
alter table sh.sales enable novalidate constraint SALES_PROMO_FK;

declare
    v_tabs  odcivarchar2list := odcivarchar2list('CHANNEL','CUST','PROD','PROMO','TIME');
    v_index varchar2(30);
begin
    for i in 1 .. v_tabs.count loop
        v_index := 'SALES_' || v_tabs(i) || '_BIX';
        dbms_application_info.set_module(v_index, '0');
        for r in (select partition_name
                  ,      row_number() over (order by partition_position) as n 
                  ,      count(*) over () as m
                  from   dba_ind_partitions 
                  where  index_owner = 'SH'
                  and    index_name = v_index
                  and    status = 'UNUSABLE'
                  order  by partition_position) 
        loop
            dbms_application_info.set_action('Doing ' || to_char(r.n) || ' of ' || to_char(r.m));
            execute immediate 'alter index sh.' || v_index || ' rebuild partition ' || r.partition_name || ' parallel 16';
            dbms_application_info.set_action('Done ' || to_char(r.n) || ' of ' || to_char(r.m));
        end loop;
        execute immediate 'alter index sh.' || v_index || ' noparallel';
    end loop;
end;
/

