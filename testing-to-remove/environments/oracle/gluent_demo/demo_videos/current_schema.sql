
-- Run the demo from SYSTEM...

alter session set current_schema = system;
create or replace synonym sales for &1..sales;
create or replace synonym channels for sh.channels;
create or replace synonym times for sh.times;
create or replace synonym products for sh.products;
create or replace synonym promotions for sh.promotions;
create or replace synonym my_plsql_func for sh.my_plsql_func;
