
set serveroutput on
whenever sqlerror exit
prompt Target number of customers...
define _gen_rows = &1

prompt
prompt Generating CUSTOMERS rows up to a target of &_gen_rows....

select to_char(sysdate, 'dd/mm/yyyy hh24:mi:ss') as start_time 
from   dual;

prompt
prompt Managing constraints and indexes...
alter table sales disable constraint sales_customer_fk;
alter table customers disable constraint customers_pk;
alter table customers disable constraint customers_country_fk;
alter index customers_gender_bix unusable;
alter index customers_marital_bix unusable;
alter index customers_yob_bix unusable;

prompt
prompt Generating data...
declare
    type customer_aat is table of customers%rowtype
        index by pls_integer;
    type street_name_rt is record
    ( street_name varchar2(200) 
    );
    type street_name_aat is table of street_name_rt
        index by pls_integer;
    type street_type_rt is record
    ( street_type varchar2(200) 
    );
    type street_type_aat is table of street_type_rt
        index by pls_integer;
    type address_rt is record
    ( cust_postal_code       varchar2(30)
    , cust_city              varchar2(100)
    , cust_city_id           number
    , cust_state_province    varchar2(100)
    , cust_state_province_id number
    , country_id             number
    );
    type address_aat is table of address_rt
        index by pls_integer;
    type first_name_rt is record
    ( cust_first_name varchar2(100)
    , cust_gender     varchar2(1)
    );
    type first_name_aat is table of first_name_rt
        index by pls_integer;
    type last_name_rt is record
    ( cust_last_name  varchar2(100)
    );
    type last_name_aat is table of last_name_rt
        index by pls_integer;
    type income_rt is record
    ( cust_income_level varchar2(30)
    , cust_credit_limit number
    );
    type income_aat is table of income_rt
        index by pls_integer;
    type marital_status_ntt is table of varchar2(30);
    v_street_names      street_name_aat;
    v_street_types      street_type_aat;
    v_addresses         address_aat;
    v_first_names       first_name_aat;
    v_last_names        last_name_aat;
    v_incomes           income_aat;
    v_marital_statuses  marital_status_ntt := marital_status_ntt('Married','Married','Married','Single','Single','Single',null,null,null,'Divorced','Widowed','Separated');
    v_customers         customer_aat;
    v_rows              pls_integer := 0;
    v_limit             pls_integer := 50000;
    v_target_rows       pls_integer := &_gen_rows;
    v_batch_size        pls_integer;
    v_custs             pls_integer;
    v_maxid             integer;
    v_random            pls_integer;
    function rv ( p_upper in pls_integer,
                  p_lower in pls_integer default 1 ) return pls_integer is
    begin
        return trunc(dbms_random.value(p_lower, p_upper + 1));
    end rv;
begin
    dbms_application_info.set_module('customers_generator.sql','Seeding lookup data...');
    -- Seed the arrays...
    select trim(regexp_substr(cust_street_address, '(^[0-9]+ )([A-Z0-9 ]+ )?([A-Z]+$)', 1, 1, 'i', 2)) as street_name
    bulk   collect into v_street_names
    from   customers
    ;
    select regexp_substr(cust_street_address, '([A-Z]+$)', 1, 1, 'i', 1) as street_type
    bulk   collect into v_street_types
    from   customers
    ;
    select distinct cust_postal_code, cust_city, cust_city_id, cust_state_province, cust_state_province_id, country_id
    bulk   collect into v_addresses
    from   customers
    ;
    select distinct cust_first_name, cust_gender
    bulk   collect into v_first_names
    from   customers
    ;
    select distinct cust_last_name
    bulk   collect into v_last_names
    from   customers
    ;
    select distinct cust_income_level, cust_credit_limit
    bulk   collect into v_incomes
    from   customers
    ;
    -- Determine how many customers already...
    select count(*), max(cust_id)
    into   v_custs, v_maxid
    from   customers
    ;
    v_target_rows := v_target_rows - v_custs;
    -- Generate data...
    dbms_application_info.set_action('Loading CUSTOMERS');
    pragma inline('rv','yes');
    while v_rows < v_target_rows loop
        v_batch_size := least(v_limit, v_target_rows-v_rows);
        for i in 1 .. v_batch_size loop
            begin
                v_customers(i).cust_id                := v_maxid + v_rows + i;
                v_random := rv(v_first_names.count);
                v_customers(i).cust_first_name        := v_first_names(v_random).cust_first_name;
                v_customers(i).cust_gender            := v_first_names(v_random).cust_gender;
                v_customers(i).cust_last_name         := v_last_names(rv(v_last_names.count)).cust_last_name;
                v_customers(i).cust_year_of_birth     := rv(1997, 1955);
                v_customers(i).cust_marital_status    := v_marital_statuses(rv(12));
                v_customers(i).cust_street_address    := to_char(rv(999)) || ' ' || v_street_names(rv(v_street_names.count)).street_name || ' ' || v_street_types(rv(v_street_types.count)).street_type;
                v_random := rv(v_addresses.count);
                v_customers(i).cust_postal_code       := v_addresses(v_random).cust_postal_code;
                v_customers(i).cust_city              := v_addresses(v_random).cust_city;
                v_customers(i).cust_city_id           := v_addresses(v_random).cust_city_id;
                v_customers(i).cust_state_province    := v_addresses(v_random).cust_state_province;
                v_customers(i).cust_state_province_id := v_addresses(v_random).cust_state_province_id;
                v_customers(i).country_id             := v_addresses(v_random).country_id;
                v_customers(i).cust_main_phone_number := to_char(rv(999, 100)) || '-' || to_char(rv(999, 100)) || '-' || to_char(rv(9999, 1000));
                v_random := rv(v_incomes.count);
                v_customers(i).cust_income_level      := v_incomes(v_random).cust_income_level;
                v_customers(i).cust_credit_limit      := v_incomes(v_random).cust_credit_limit;
                v_customers(i).cust_email             := lower(substr(v_customers(i).cust_first_name,1,1) || '.' || v_customers(i).cust_last_name || '@company.com');
                v_random := rv(1, 999);
                v_customers(i).cust_total             := 'A' || to_char(v_random);
                v_customers(i).cust_total_id          := v_random;
                v_customers(i).cust_src_id            := rv(10);
                v_customers(i).cust_eff_from          := date '1990-01-01' + rv(7500);
                v_customers(i).cust_eff_to            := case when rv(100) <= 10 then date '1990-01-01' + rv(7500, 1000) end;
                v_customers(i).cust_valid             := case when v_customers(i).cust_eff_to is not null then 'I' else 'A' end;
            exception
                when others then
                    dbms_output.put_line('cust_id                 : ' || v_customers(i).cust_id);
                    dbms_output.put_line('cust_first_name         : ' || v_customers(i).cust_first_name);
                    dbms_output.put_line('cust_gender             : ' || v_customers(i).cust_gender);
                    dbms_output.put_line('cust_last_name          : ' || v_customers(i).cust_last_name);
                    dbms_output.put_line('cust_year_of_birth      : ' || v_customers(i).cust_year_of_birth);
                    dbms_output.put_line('cust_marital_status     : ' || v_customers(i).cust_marital_status);
                    dbms_output.put_line('cust_street_address     : ' || v_customers(i).cust_street_address);
                    dbms_output.put_line('cust_postal_code        : ' || v_customers(i).cust_postal_code);
                    dbms_output.put_line('cust_city               : ' || v_customers(i).cust_city);
                    dbms_output.put_line('cust_city_id            : ' || v_customers(i).cust_city_id);
                    dbms_output.put_line('cust_state_province     : ' || v_customers(i).cust_state_province);
                    dbms_output.put_line('cust_state_province_id  : ' || v_customers(i).cust_state_province_id);
                    dbms_output.put_line('country_id              : ' || v_customers(i).country_id);
                    dbms_output.put_line('cust_main_phone_number  : ' || v_customers(i).cust_main_phone_number);
                    dbms_output.put_line('cust_income_level       : ' || v_customers(i).cust_income_level);
                    dbms_output.put_line('cust_credit_limit       : ' || v_customers(i).cust_credit_limit);
                    dbms_output.put_line('cust_email              : ' || v_customers(i).cust_email);
                    dbms_output.put_line('cust_total              : ' || v_customers(i).cust_total);
                    dbms_output.put_line('cust_total_id           : ' || v_customers(i).cust_total_id);
                    dbms_output.put_line('cust_src_id             : ' || v_customers(i).cust_src_id);
                    dbms_output.put_line('cust_eff_from           : ' || v_customers(i).cust_eff_from);
                    dbms_output.put_line('cust_eff_to             : ' || v_customers(i).cust_eff_to);
                    dbms_output.put_line('cust_valid              : ' || v_customers(i).cust_valid);
                    raise;
            end;
        end loop;
        forall i in indices of v_customers
            insert /*+ append_values */ into customers
            values v_customers(i);
        commit;
        v_rows := v_rows + v_batch_size;
        dbms_application_info.set_client_info('Loaded ' || to_char(v_rows) || ' of ' || to_char(v_target_rows));
        v_customers.delete;
    end loop;
    dbms_output.put_line(to_char(v_rows) || ' additional CUSTOMERS rows generated.');
    dbms_application_info.set_action('Completed');
end;
/

prompt 
prompt Managing constraints and indexes...
alter session force parallel ddl parallel 8;
alter table customers parallel 8;
alter table customers enable constraint customers_pk;
alter table customers enable novalidate constraint customers_country_fk;
alter table customers enable constraint customers_country_fk;
alter table customers noparallel;
alter index customers_gender_bix rebuild parallel 8;
alter index customers_marital_bix rebuild parallel 8;
alter index customers_yob_bix rebuild parallel 8;
alter index customers_gender_bix noparallel;
alter index customers_marital_bix noparallel;
alter index customers_yob_bix noparallel;
alter table sales parallel 8;
alter table sales enable novalidate constraint sales_customer_fk;
alter table sales enable constraint sales_customer_fk;
alter table sales noparallel;

prompt
prompt Gathering stats...
exec dbms_stats.gather_table_stats(user, 'CUSTOMERS', degree=>8, method_opt=>'for all columns size 1');

prompt
prompt Resetting PDDL...
alter session disable parallel ddl;

select to_char(sysdate, 'dd/mm/yyyy hh24:mi:ss') as end_time 
from   dual;

prompt
prompt CUSTOMERS rows successfully generated.
prompt

undefine 1
undefine _gen_rows
