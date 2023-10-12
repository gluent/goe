
spool create_test_tables.log

-- Range date...
@@create_test_table.sql gl_range_day
@@create_test_table.sql gl_range_month
@@create_test_table.sql gl_range_year
@@create_test_table.sql gl_range_mixed
@@create_test_table.sql gl_range_maxvalue
@@create_test_table.sql gl_range_subday
@@create_test_table.sql gl_range_iot_month

-- Range date intervals...
@@create_test_table.sql gl_range_interval_day
@@create_test_table.sql gl_range_interval_5day
@@create_test_table.sql gl_range_interval_subday

-- Range timestamp...
@@create_test_table.sql gl_range_timestamp_day
@@create_test_table.sql gl_range_timestamp_fractional3
@@create_test_table.sql gl_range_timestamp_fractional6
@@create_test_table.sql gl_range_timestamp_fractional9
@@create_test_table.sql gl_range_timestamp_subday

-- Range other...
@@create_test_table.sql gl_range_number
@@create_test_table.sql gl_range_varchar2

-- Range composite...
@@create_test_table.sql gl_range_range_day
@@create_test_table.sql gl_range_list_day
@@create_test_table.sql gl_range_hash_day

-- List dates...
@@create_test_table.sql gl_list_day
@@create_test_table.sql gl_list_week
@@create_test_table.sql gl_list_default

-- List timestamps...
@@create_test_table.sql gl_list_timestamp_day
@@create_test_table.sql gl_list_timestamp_week
@@create_test_table.sql gl_list_timestamp_default

-- Other...
@@create_test_table.sql gl_hash
--@@create_test_table.sql gl_ref
--@@create_test_table.sql gl_system
@@create_test_table.sql gl_iot

spool off
