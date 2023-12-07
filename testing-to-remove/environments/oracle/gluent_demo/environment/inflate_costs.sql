
alter table sh.costs disable constraint COSTS_PRODUCT_FK;
alter table sh.costs disable constraint COSTS_TIME_FK;
alter table sh.costs disable constraint COSTS_CHANNEL_FK;
alter table sh.costs disable constraint COSTS_PROMO_FK;

alter index sh.COSTS_PROD_BIX unusable;
alter index sh.COSTS_TIME_BIX unusable;

set serveroutput on
declare
    rn number;
begin
    dbms_application_info.set_module('inflate_costs.sql', null);
    for r in (select time_id, row_number() over (order by time_id) as n, count(*) over () as m
              from  (select distinct time_id from sh.costs)
              order  by time_id)
    loop
        rn := dbms_random.value(135000, 140000);
        insert /*+ append parallel(16) */ 
        into  sh.costs
        with dims as (
                select prod_id, promo_id, channel_id from sh.products, sh.promotions, sh.channels
                minus
                select distinct prod_id, channel_id, promo_id from sh.costs where time_id = r.time_id
                )
        ,    data as (
                select d.prod_id, d.promo_id, d.channel_id,
                       round(p.prod_list_price * dbms_random.value(0.75, 0.85), 2) as unit_cost,
                       round(p.prod_list_price * dbms_random.value(1, 1.2), 2) as unit_price
                from   dims        d
                ,      sh.products p
                where  d.prod_id = p.prod_id
                order  by dbms_random.random 
                )
        select prod_id, r.time_id, promo_id, channel_id, unit_cost, unit_price
        from   data
        where  rownum <= rn;
        commit;
        dbms_application_info.set_action(to_char(r.n) || ' of ' || to_char(r.m) || ' completed.');
    end loop;
end;
/

alter table sh.costs enable novalidate constraint COSTS_PRODUCT_FK;
alter table sh.costs enable novalidate constraint COSTS_TIME_FK;
alter table sh.costs enable novalidate constraint COSTS_CHANNEL_FK;
alter table sh.costs enable novalidate constraint COSTS_PROMO_FK;

declare
    v_tabs  odcivarchar2list := odcivarchar2list('PROD','TIME');
    v_index varchar2(30);
begin
    for i in 1 .. v_tabs.count loop
        v_index := 'COSTS_' || v_tabs(i) || '_BIX';
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

