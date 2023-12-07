define _app_schema = &1

alter session set current_schema = &_app_schema;

create table ste_schedule
( table_name            varchar2(30) not null
, schedule_date         date         not null
, operation_no          number       not null
, keep_partitions       number
, start_time            date
, end_time              date
, constraint ste_schedule_pk
    primary key (table_name)
);

declare
    procedure add_date ( p_table_name      in varchar2,
                         p_keep_partitions in number default null,
                         p_base_date       in varchar2 default date '2015-06-30'
                       ) is
    begin
        insert into ste_schedule
            (table_name, schedule_date, operation_no, keep_partitions)
        values
            (p_table_name, p_base_date, 0, p_keep_partitions);
    end add_date;
begin
    delete
    from   ste_schedule;
    --
    add_date('GLOBAL_DATE');
    add_date('CHANNELS');
    add_date('COSTS', 5);
    add_date('CUSTOMERS');
    add_date('CUSTOMERS_H01');
    add_date('CUSTOMERS_H02');
    add_date('CUSTOMERS_H03');
    add_date('CUSTOMERS_P01_H');
    add_date('CUSTOMERS_P01_L');
    add_date('CUSTOMERS_P02_H');
    add_date('CUSTOMERS_P02_L');
    add_date('CUSTOMERS_P03_H');
    add_date('CUSTOMERS_P03_L');
    add_date('PRODUCTS');
    add_date('PROMOTIONS');
    add_date('SALES', 5);
    add_date('SALES_R01');
    add_date('SALES_R01_I', 2);
    add_date('SALES_R02');
    add_date('SALES_R02_I', 2);
    add_date('SALES_R03');
    add_date('SALES_R04');
    add_date('SALES_R05');
    add_date('SALES_R05_I', 2);
    add_date('SALES_R06');
    add_date('SALES_R06_I', 2);
    add_date('SALES_R07');
    add_date('SALES_R08');
    add_date('SALES_R08_I', 2);
    add_date('SALES_R09');
    add_date('SALES_R10');
    add_date('SALES_R10_I', 2);
    add_date('SALES_R11');
    add_date('SALES_R11_I', 2);
    add_date('SALES_R12');
    add_date('SALES_R13');
    add_date('SALES_R14');
    add_date('SALES_R14_I', 2);
    add_date('SALES_R15');
    add_date('SALES_R15_I', 2);
    add_date('SALES_R16');
    add_date('SALES_R17');
    add_date('SALES_R17_I', 2);
    add_date('SALES_R18');
    add_date('TIMES');
    --
    commit;
end;
/

