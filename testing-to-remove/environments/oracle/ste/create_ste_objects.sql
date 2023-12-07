
define _app_schema = &1
define _hybrid_schema = &_app_schema._H

prompt *********************************************************************************
prompt
prompt Creating STE objects in the following schemas:
prompt
prompt * Application Schema = &_app_schema
prompt * Hybrid Schema      = &_hybrid_schema
prompt
prompt *********************************************************************************
prompt

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
   do_drop('PACKAGE', 'STE');
   do_drop('TYPE', 'STE_NUMBER_NTT');
end;
/

create or replace type ste_number_ntt as
    table of number;
/

create or replace package ste as

    -- Schedule date procedures...
    gc_global_schedule constant varchar2(30) := 'GLOBAL_DATE';

    procedure start_schedule ( p_table_name in varchar2 default gc_global_schedule );

    procedure end_schedule ( p_table_name in varchar2 default gc_global_schedule );

    procedure report_schedule ( p_errors_only   in  varchar2 default 'N',
                                p_schedule_data out sys_refcursor );

    procedure set_operation_no ( p_table_name   in varchar2,
                                 p_operation_no in number );

    procedure get_schedule_info ( p_table_name    in  varchar2 default gc_global_schedule,
                                  p_schedule_info out sys_refcursor );

    -- Partition maintenance procedures...
    procedure add_partition ( p_table_name     in varchar2,
                              p_partition_name in varchar2,
                              p_partition_hwm  in date );

    procedure split_partition ( p_table_name           in varchar2,
                                p_split_partition_name in varchar2,
                                p_partition_name       in varchar2,
                                p_partition_hwm        in date );

    procedure exchange_partition ( p_staging_table_name in varchar2,
                                   p_table_name         in varchar2,
                                   p_partition_name     in varchar2 );

    function get_partition_name ( p_table_name         in varchar2,
                                  p_partition_position in number default null ) return varchar2;

    -- Data procedures...
    gc_sample_pct constant number      := 5;
    gc_dml_update constant varchar2(1) := 'U';
    gc_dml_insert constant varchar2(1) := 'I';
    gc_dml_delete constant varchar2(1) := 'D';

    procedure load_data ( p_load_table_name       in varchar2,
                          p_load_partition_date   in date,
                          p_primary_key_column    in varchar2,
                          p_partition_key_column  in varchar2,
                          p_source_table_name     in varchar2,
                          p_source_partition_date in date );

    procedure update_data ( p_table_name     in varchar2,
                            p_column_name    in varchar2,
                            p_partition_name in varchar2 default null,
                            p_sample_pct     in number   default gc_sample_pct );

    procedure insert_data ( p_table_name           in varchar2,
                            p_primary_key_column   in varchar2 default null,
                            p_partition_name       in varchar2 default null,
                            p_partition_date       in date     default null,
                            p_partition_key_column in varchar2 default null,
                            p_from_partition_name  in varchar2 default null,
                            p_sample_pct           in number   default gc_sample_pct );

    procedure delete_data ( p_table_name     in varchar2,
                            p_partition_name in varchar2 default null,
                            p_sample_pct     in number   default gc_sample_pct );

    procedure patch_data ( p_patch_operation    in varchar2,
                           p_source_table_name  in varchar2,
                           p_primary_key_column in varchar2,
                           p_updated_column     in varchar2 );

    -- Validation procedures...
    procedure validate_table ( p_table_name in  varchar2,
                               p_results    out sys_refcursor );

end ste;
/

create or replace package body ste as

    -- Generic "stuff"...
    ------------------------------------------------------------------------------------------------------------
    type args_ntt is table of varchar2(32767);

    type column_rt is record
    ( column_name varchar2(30)
    , data_type   varchar2(30)
    , data_length number
    );

    type columns_aat is table of column_rt
        index by varchar2(30);

    type rowid_ntt is table of rowid;

    ------------------------------------------------------------------------------------------------------------
    function format_string ( p_string in varchar2,
                             p_args   in args_ntt ) return varchar2 is
        v_string varchar2(32767) := p_string;
    begin
        if p_args is not null or p_args is not empty then
            for i in 1 .. p_args.count loop
                v_string := replace(v_string, '%' || i, p_args(i));
            end loop;
        end if;
        return v_string;
    end format_string;

    ------------------------------------------------------------------------------------------------------------
    procedure exec_sql ( p_sql in varchar2,
                         p_args in args_ntt default null ) is
        v_sql varchar2(32767);
    begin
        v_sql := format_string( p_string => p_sql, p_args => p_args);
        execute immediate v_sql;
    exception
        when others then
            raise_application_error(-20000, 'Error:' || SQLERRM || ' for SQL [' || v_sql || ']', true);
    end exec_sql;

    ------------------------------------------------------------------------------------------------------------
    procedure p ( p_string in varchar2,
                  p_args   in args_ntt default null ) is
    begin
        dbms_output.put_line( 'STE: ' || format_string( p_string => p_string, p_args => p_args ));
    end p;

    ------------------------------------------------------------------------------------------------------------
    function get_columns ( p_table_name  in  varchar2,
                           p_column_name in  varchar2 default null ) return columns_aat is
        v_columns columns_aat;
    begin
        for r_col in ( select table_name, column_name, data_type, data_length
                       from   user_tab_columns
                       where  table_name = p_table_name
                       and   (    column_name = p_column_name
                              or p_column_name is null )
                       order  by column_id )
        loop
            v_columns(r_col.column_name).column_name := r_col.column_name;
            v_columns(r_col.column_name).data_type   := r_col.data_type;
            v_columns(r_col.column_name).data_length := r_col.data_length;
        end loop;
        return v_columns;
    end get_columns;

    ------------------------------------------------------------------------------------------------------------
    function get_column_list ( p_table_name in varchar2 ) return varchar2 is
        v_columns     columns_aat;
        v_column_name varchar2(30);
        v_column_list varchar2(32767);
    begin
        v_columns := get_columns( p_table_name  => p_table_name );
        v_column_name := v_columns.first;
        while v_column_name is not null loop
            v_column_list := v_column_list || ',' || v_column_name;
            v_column_name := v_columns.next(v_column_name);
        end loop;
        return trim(leading ',' from v_column_list);
    end get_column_list;

    ------------------------------------------------------------------------------------------------------------
    function get_from_clause ( p_table_name     in varchar2,
                               p_partition_name in varchar2 default null,
                               p_sample_pct     in number   default null ) return varchar2 is
    begin
        return p_table_name ||
               case
                  when p_partition_name is not null
                  then ' partition (' || p_partition_name || ')'
               end ||
               case
                  when p_sample_pct is not null
                  then ' sample (' || p_sample_pct || ')'
               end;
    end get_from_clause;

    ------------------------------------------------------------------------------------------------------------
    function get_partition_name ( p_table_name         in varchar2,
                                  p_partition_position in number default null ) return varchar2 is
        v_partition_name varchar2(30);
    begin
        select partition_name
        into   v_partition_name
        from   user_tab_partitions
        where  table_name = p_table_name
        and   (   (    p_partition_position is not null
                   and partition_position = p_partition_position )
               or (    p_partition_position is null
                   and partition_position = (select max(p.partition_position)
                                             from   user_tab_partitions p
                                             where  p.table_name = p_table_name) ) );
        return v_partition_name;
    end get_partition_name;

    ------------------------------------------------------------------------------------------------------------
    function get_sample_rowids ( p_table_name     in varchar2,
                                 p_partition_name in varchar2,
                                 p_sample_pct     in number ) return rowid_ntt is

        v_rowids  rowid_ntt;
    begin
        execute immediate
            'select rowid from ' || get_from_clause( p_table_name     => p_table_name,
                                                     p_partition_name => p_partition_name,
                                                     p_sample_pct     => p_sample_pct )
        bulk collect into v_rowids;
        return v_rowids;
    end get_sample_rowids;

    -- Batch date procedures...
    ------------------------------------------------------------------------------------------------------------
    procedure start_schedule ( p_table_name in varchar2 default gc_global_schedule ) is
    begin
        update ste_schedule
        set    schedule_date = case
                                  when operation_no = 0
                                  then case
                                          when to_char(schedule_date, 'DY') in ('FRI','SAT','SUN')
                                          then next_day(schedule_date, 'MONDAY')
                                          else schedule_date + 1
                                       end
                                  else schedule_date
                               end
        ,      operation_no  = decode(operation_no, 0, 1, operation_no)
        ,      start_time    = nvl2(end_time, sysdate, nvl(start_time, sysdate))
        ,      end_time      = null
        where  table_name    = p_table_name;
        commit;
    end start_schedule;

    ------------------------------------------------------------------------------------------------------------
    procedure end_schedule ( p_table_name in varchar2 default gc_global_schedule ) is
    begin
        update ste_schedule
        set    operation_no = 0
        ,      end_time     = sysdate
        where  table_name   = p_table_name;
        commit;
    end end_schedule;

    ------------------------------------------------------------------------------------------------------------
    procedure set_operation_no ( p_table_name   in varchar2,
                                 p_operation_no in number ) is
    begin
        update ste_schedule
        set    operation_no = p_operation_no
        where  table_name   = p_table_name;
        commit;
    end set_operation_no;

    ------------------------------------------------------------------------------------------------------------
    procedure get_schedule_info ( p_table_name    in  varchar2 default gc_global_schedule,
                                  p_schedule_info out sys_refcursor ) is
    begin
        open p_schedule_info
        for
            select table_name                                                                as table_name
            ,      to_char(schedule_date, 'YYYY-MM-DD')                                      as schedule_date
            ,      to_char(schedule_date_prev, 'YYYY-MM-DD')                                 as schedule_date_prev
            ,      operation_no                                                              as operation_no
            ,      nvl(to_char(keep_partitions), '-')                                        as older_than_days
            ,      nvl(to_char(older_than_date, 'YYYY-MM-DD'), '-')                          as older_than_date
            ,      decode(range_partitioned, 'Y', to_char(partition_hwm, 'YYYY-MM-DD'), '-') as partition_hwm
            ,      range_partitioned                                                         as range_partitioned
            ,      interval_partitioned                                                      as interval_partitioned
            from  (
                    select table_name
                    ,      schedule_date
                    ,      operation_no
                    ,      keep_partitions
                    ,      partition_hwm
                    ,      range_partitioned
                    ,      interval_partitioned
                    ,      schedule_date - decode(to_char(schedule_date, 'DY'),'MON',3,1) as schedule_date_prev
                    ,      keep_date - case
                                          when to_number(to_char(partition_hwm, 'D')) > to_number(to_char(schedule_date, 'D'))
                                          then 2
                                          else 0
                                       end as older_than_date
                    from  (
                           select s.table_name
                           ,      s.schedule_date
                           ,      s.operation_no
                           ,      s.keep_partitions
                           ,      s.schedule_date - (s.keep_partitions - 1) as keep_date
                           ,      s.schedule_date + 1 as partition_hwm
                           ,      decode(p.partitioning_type, 'RANGE', 'Y', 'N') as range_partitioned
                           ,      nvl2(p.interval, 'Y', 'N') as interval_partitioned
                           from   ste_schedule       s
                                  left outer join
                                  user_part_tables   p
                                  on (p.table_name = s.table_name)
                           where  s.table_name = p_table_name
                          )
                  );
    end get_schedule_info;

    ------------------------------------------------------------------------------------------------------------
    procedure report_schedule ( p_errors_only   in  varchar2 default 'N',
                                p_schedule_data out sys_refcursor ) is
    begin
        open p_schedule_data
        for
            select table_name
            ,      to_char(schedule_date, 'DD-MON-YYYY')                                as schedule_date
            ,      operation_no
            ,      to_char(start_time, 'DD-MON-YYYY HH24:MI:SS')                        as start_time
            ,      to_char(end_time, 'DD-MON-YYYY HH24:MI:SS')                          as end_time
            ,      lpad(to_char(round((end_time-start_time)*1440,2), 'fm9,990.00'), 12) as elapsed_mins
            from   ste_schedule
            where  decode(p_errors_only, 'Y', operation_no, 1) > 0
            order  by
                   decode(table_name, 'GLOBAL_DATE', 2, 1)
            ,      start_time;
    end report_schedule;

    -- Partition maintenance procedures...
    ------------------------------------------------------------------------------------------------------------
    procedure add_partition ( p_table_name     in varchar2,
                              p_partition_name in varchar2,
                              p_partition_hwm  in date ) is
    begin
        exec_sql( p_sql => q'{ALTER TABLE %1 ADD PARTITION %2 VALUES LESS THAN (DATE '%3')}',
                  p_args => args_ntt( p_table_name, p_partition_name, TO_CHAR(p_partition_hwm, 'YYYY-MM-DD') ) );
    end add_partition;

    ------------------------------------------------------------------------------------------------------------
    procedure split_partition ( p_table_name           in varchar2,
                                p_split_partition_name in varchar2,
                                p_partition_name       in varchar2,
                                p_partition_hwm        in date ) is
    begin
        exec_sql( p_sql => q'{ALTER TABLE %1 SPLIT PARTITION %2 AT (DATE '%3') INTO (PARTITION %4, PARTITION %5)}',
                  p_args => args_ntt( p_table_name, p_split_partition_name, TO_CHAR(p_partition_hwm, 'YYYY-MM-DD'),
                                      p_partition_name, p_split_partition_name ) );
    end split_partition;

    ------------------------------------------------------------------------------------------------------------
    procedure exchange_partition ( p_staging_table_name in varchar2,
                                   p_table_name         in varchar2,
                                   p_partition_name     in varchar2 ) is
    begin
        exec_sql( p_sql => q'{ALTER TABLE %1 EXCHANGE PARTITION %2 WITH TABLE %3 INCLUDING INDEXES WITHOUT VALIDATION}',
                  p_args => args_ntt( p_table_name, p_partition_name, p_staging_table_name ) );
    end exchange_partition;

    -- Data procedures...
    ------------------------------------------------------------------------------------------------------------
    procedure load_data ( p_load_table_name       in varchar2,
                          p_load_partition_date   in date,
                          p_primary_key_column    in varchar2,
                          p_partition_key_column  in varchar2,
                          p_source_table_name     in varchar2,
                          p_source_partition_date in date ) is

        v_column_list varchar2(32767);
        v_insert      varchar2(32767);
        v_select      varchar2(32767);

    begin

        -- Fetch the columns for the insert...
        v_column_list := get_column_list( p_table_name  => p_load_table_name );

        -- Prepare the insert statement...
        v_insert := 'insert /*+ append */ into ' || get_from_clause ( p_table_name => p_load_table_name ) || '(' || v_column_list || ')';

        -- Prepare the select statement...
        if p_primary_key_column is not null then
            v_column_list := regexp_replace(v_column_list, '(^|,)(' || p_primary_key_column || ')(,|$)', '\1(SELECT MAX(' || p_primary_key_column || ') FROM ' || p_source_table_name || ') + ROWNUM\3');
        end if;
        if p_partition_key_column is not null then
            v_column_list := regexp_replace(v_column_list, '(^|,)(' || p_partition_key_column || ')(,|$)', '\1:bv_load_part_date\3');
        end if;
        v_select := ' select ' || v_column_list ||
                    ' from   ' || get_from_clause( p_table_name => p_source_table_name ) ||
                    ' where  ' || p_partition_key_column || ' = ' || ':bv_source_part_date';

        -- Execute it...
        execute immediate v_insert || v_select using in p_load_partition_date, in p_source_partition_date;

        p('Loaded [%1] rows into table [%2].', args_ntt(sql%rowcount, p_load_table_name));

        commit;

    end load_data;

    ------------------------------------------------------------------------------------------------------------
    procedure update_data ( p_table_name        in varchar2,
                            p_column_name       in varchar2,
                            p_partition_name    in varchar2 default null,
                            p_sample_pct        in number   default gc_sample_pct ) is

        v_rowids  rowid_ntt;
        v_columns columns_aat;
    begin

        -- Fetch a sample of ROWIDs to target the updates (doing it this way to support the SAMPLE clause)...
        v_rowids := get_sample_rowids( p_table_name     => p_table_name,
                                       p_partition_name => p_partition_name,
                                       p_sample_pct     => p_sample_pct );

        -- Determine data type of column provided...
        v_columns := get_columns( p_table_name  => p_table_name,
                                  p_column_name => p_column_name );

        -- Updates based on all of the above...
        forall i in indices of v_rowids
            execute immediate
                'update ' || p_table_name || ' ' ||
                'set    ' || p_column_name || ' = ' || case
                                                          when v_columns(p_column_name).data_type in ('VARCHAR2', 'CHAR')
                                                          then 'dbms_random.string(''U'', 5) || substr(' || p_column_name || ', 6)'
                                                          when v_columns(p_column_name).data_type in ('NUMBER', 'FLOAT', 'INTEGER')
                                                          then p_column_name || ' + 1'
                                                          else p_column_name
                                                       end || ' ' ||
                ' where  rowid = :b0'
            using in v_rowids(i);

        p('Updated [%1] sample rows in table [%2] (partition [%3]).', args_ntt(sql%rowcount, p_table_name, nvl(p_partition_name, 'NULL')));

        commit;

    end update_data;

    ------------------------------------------------------------------------------------------------------------
    procedure insert_data ( p_table_name           in varchar2,
                            p_primary_key_column   in varchar2 default null,
                            p_partition_name       in varchar2 default null,
                            p_partition_date       in date     default null,
                            p_partition_key_column in varchar2 default null,
                            p_from_partition_name  in varchar2 default null,
                            p_sample_pct           in number   default gc_sample_pct ) is

        v_column_list varchar2(32767);
        v_insert      varchar2(32767);
        v_select      varchar2(32767);

    begin

        -- Fetch the columns for the insert...
        v_column_list := get_column_list( p_table_name  => p_table_name );

        -- Prepare the insert statement...
        v_insert := 'insert into ' || get_from_clause ( p_table_name     => p_table_name,
                                                        p_partition_name => p_partition_name ) || '(' || v_column_list || ')';

        -- Prepare the select statement...
        if p_primary_key_column is not null then
            v_column_list := regexp_replace(v_column_list, '(^|,)(' || p_primary_key_column || ')(,|$)', '\1(SELECT MAX(' || p_primary_key_column || ') FROM ' || p_table_name || ') + ROWNUM\3');
        end if;
        if p_partition_key_column is not null then
            v_column_list := regexp_replace(v_column_list, '(^|,)(' || p_partition_key_column || ')(,|$)', '\1DATE ''' || to_char(p_partition_date, 'YYYY-MM-DD') || '''\3');
        end if;
        v_select := ' select ' || v_column_list || ' from ' || get_from_clause( p_table_name     => p_table_name,
                                                                                p_partition_name => p_from_partition_name,
                                                                                p_sample_pct     => p_sample_pct );

        -- Execute it...
        execute immediate v_insert || v_select;

        p('Inserted [%1] sample rows into table [%2] (partition [%3]).', args_ntt(sql%rowcount, p_table_name, nvl(p_partition_name, 'NULL')));

        commit;

    end insert_data;

    ------------------------------------------------------------------------------------------------------------
    procedure delete_data ( p_table_name     in varchar2,
                            p_partition_name in varchar2 default null,
                            p_sample_pct     in number   default gc_sample_pct ) is

        v_rowids  rowid_ntt;
    begin

        -- Fetch a sample of ROWIDs to target the deletes (doing it this way to support the SAMPLE clause)...
        v_rowids := get_sample_rowids( p_table_name     => p_table_name,
                                       p_partition_name => p_partition_name,
                                       p_sample_pct     => p_sample_pct );

        -- Delete the corresponding records...
        forall i in indices of v_rowids
            execute immediate
                'delete ' ||
                'from   ' || p_table_name || ' ' ||
                'where rowid = :b0'
            using in v_rowids(i);

        p('Deleted [%1] sample rows from table [%2] (partition [%3]).', args_ntt(sql%rowcount, p_table_name, nvl(p_partition_name, 'NULL')));

        commit;

    end delete_data;

    ------------------------------------------------------------------------------------------------------------
    procedure patch_data ( p_patch_operation    in varchar2,
                           p_source_table_name  in varchar2,
                           p_primary_key_column in varchar2,
                           p_updated_column     in varchar2 ) is

        v_cursor           sys_refcursor;
        v_ids              ste_number_ntt;
        v_column_list      varchar2(32767);
        v_rowsource_a      varchar2(100);
        v_rowsource_b      varchar2(100);
        v_patch_dml        varchar2(32767);
        v_rowcount         pls_integer := 0;
        v_dmlcount         pls_integer := 0;
        v_hybrid_view_name varchar2(100) := sys_context('userenv','current_schema') || '_H.' || p_source_table_name;
        v_patch_view_name  varchar2(100) := v_hybrid_view_name || '_UV';

    begin

        -- TODO: a better way than this to identify the data to patch. Probably integrate this procedure with the update_data, delete_data,
        --       insert_data procedures so that patching also performs the source data updates as well...

        -- Prepare the patch DML up-front...
        if p_patch_operation = gc_dml_insert then
            v_column_list := get_column_list( p_table_name  => p_source_table_name );
            v_patch_dml := 'insert into ' || v_patch_view_name || '(' || v_column_list || ') select ' || v_column_list || ' from ' || p_source_table_name || ' where ' || p_primary_key_column || ' in (select column_value from table(:bv0))';
        elsif p_patch_operation = gc_dml_update then
            v_patch_dml := 'update ' || v_patch_view_name || ' u set ' || p_updated_column || ' = ( select s.' || p_updated_column || ' from ' || p_source_table_name || ' s where s.' || p_primary_key_column || ' = u.' || p_primary_key_column || ') where u.' || p_primary_key_column || ' in (select column_value from table(:bv0))';
        elsif p_patch_operation = gc_dml_delete then
            v_patch_dml := 'delete from ' || v_patch_view_name || ' where ' || p_primary_key_column || ' in (select column_value from table(:bv0))';
        end if;

        -- Identify any differences that need to be patched...
        if p_patch_operation in (gc_dml_insert, gc_dml_update) then
            v_rowsource_a := p_source_table_name;
            v_rowsource_b := v_hybrid_view_name;
        else
            v_rowsource_a := v_hybrid_view_name;
            v_rowsource_b := p_source_table_name;
        end if;

        -- Find the changes to patch...
        open v_cursor
        for
            'select ' || p_primary_key_column || '
             from   (
                     select ' || p_primary_key_column || ',' || nvl(p_updated_column, 'null') || '
                     from   ' || v_rowsource_a || '
                     minus
                     select ' || p_primary_key_column || ',' || nvl(p_updated_column, 'null') || '
                     from   ' || v_rowsource_b ||
                   ')';
        loop
            fetch v_cursor bulk collect into v_ids limit 10000;
            v_rowcount := v_rowcount + v_ids.count;
            execute immediate v_patch_dml using in v_ids;
            v_dmlcount := v_dmlcount + sql%rowcount;
            commit;
            exit when v_cursor%notfound;
        end loop;
        close v_cursor;

        p('Found [%1] rows to be patched as [%2] for offloaded table [%3]).', args_ntt(v_rowcount, p_patch_operation, p_source_table_name));
        p('Patched [%1] [%2] rows for offloaded table [%3]).', args_ntt(v_dmlcount, p_patch_operation, p_source_table_name));

        commit;

    end patch_data;

    ------------------------------------------------------------------------------------------------------------
    procedure validate_table ( p_table_name in  varchar2,
                               p_results    out sys_refcursor ) is
    begin
        open p_results for
            'select count(*)
             from  (
                     select *
                     from   ' || p_table_name || '
                     minus
                     select *
                     from   ' || sys_context('userenv','current_schema') || '_h.' || p_table_name || '
                     union all
                     select *
                     from   ' || sys_context('userenv','current_schema') || '_h.' || p_table_name || '
                     minus
                     select *
                     from   ' || p_table_name || '
                   )';
    end validate_table;

end ste;
/
