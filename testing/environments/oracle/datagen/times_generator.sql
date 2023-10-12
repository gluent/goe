
set serveroutput on
whenever sqlerror exit

prompt
prompt Define number of years to generate from 2011 onwards...
define _num_years = &1
define _base_date = "2010-12-31"

prompt
prompt Generating &_num_years of TIMES data...

select to_char(sysdate, 'dd/mm/yyyy hh24:mi:ss') as start_time 
from   dual;

prompt
prompt Generating data...

insert into times ( time_id
                  , day_name
                  , day_number_in_week
                  , day_number_in_month
                  , calendar_week_number
                  , fiscal_week_number
                  , week_ending_day
                  , week_ending_day_id
                  , calendar_month_number
                  , fiscal_month_number
                  , calendar_month_desc
                  , calendar_month_id
                  , fiscal_month_desc
                  , fiscal_month_id
                  , days_in_cal_month
                  , days_in_fis_month
                  , end_of_cal_month
                  , end_of_fis_month
                  , calendar_month_name
                  , fiscal_month_name
                  , calendar_quarter_desc
                  , calendar_quarter_id
                  , fiscal_quarter_desc
                  , fiscal_quarter_id
                  , days_in_cal_quarter
                  , days_in_fis_quarter
                  , end_of_cal_quarter
                  , end_of_fis_quarter
                  , calendar_quarter_number
                  , fiscal_quarter_number
                  , calendar_year
                  , calendar_year_id
                  , fiscal_year
                  , fiscal_year_id
                  , days_in_cal_year
                  , days_in_fis_year
                  , end_of_cal_year
                  , end_of_fis_year
                  )
with data as (
        select time_id 
        ,      time_id as d
        ,      case
                   when extract(month from time_id) = extract(month from next_day(time_id-1, 'sunday'))
                   then time_id
                   else next_day(time_id,'monday')
               end as fd
        ,      date '1990-01-01' as d0 
        from  (
                select date '&_base_date' + rownum as time_id
                from   dual
                connect by rownum <= (last_day(add_months(date '&_base_date', 12*&_num_years))-date '&_base_date')
              )
        )
,    calendar_data as (
        select time_id
        ,      trim(to_char(d, 'Day'))                        as day_name
        ,      to_number(to_char(d, 'D'))                     as day_number_in_week
        ,      extract(day from d)                            as day_number_in_month
        ,      to_number(to_char(d, 'WW'))                    as calendar_week_number
        ,      next_day(d+6, 'sunday')-7                      as week_ending_day
        ,     (next_day(d+6, 'sunday')-7)-d0                  as week_ending_day_id
        ,      extract(month from d)                          as calendar_month_number
        ,      to_char(d, 'yyyy-mm')                          as calendar_month_desc
        ,      trunc(d, 'month')-d0                           as calendar_month_id
        ,      extract(day from last_day(d))                  as days_in_cal_month
        ,      last_day(d)                                    as end_of_cal_month
        ,      trim(to_char(d, 'Month'))                      as calendar_month_name
        ,      to_char(d, 'yyyy-"0"q')                        as calendar_quarter_desc
        ,      trunc(d, 'q')-d0+123                           as calendar_quarter_id
        ,      add_months(trunc(d, 'q'), 3)-trunc(d, 'q')     as days_in_cal_quarter
        ,      add_months(trunc(d, 'q'), 3)-1                 as end_of_cal_quarter
        ,      to_number(to_char(d, 'q'))                     as calendar_quarter_number
        ,      extract(year from d)                           as calendar_year
        ,      extract(year from d)-150                       as calendar_year_id
        ,      trunc(add_months(d,12),'year')-trunc(d,'year') as days_in_cal_year
        ,      trunc(add_months(d,12),'year')-1               as end_of_cal_year
        from   data
        )
,    fiscal_data as (
        select time_id
        ,      trunc(((d)-next_day(trunc(d,'year')-7,'monday'))/7)                    as fiscal_week_number
        ,      extract(month from fd)                                                   as fiscal_month_number
        ,      to_char(fd, 'yyyy-mm')                                                   as fiscal_month_desc
        ,      to_number(trunc(fd, 'month')-d0+234)                                     as fiscal_month_id
        ,      next_day(last_day(fd)-7, 'Sunday')                                       as end_of_fis_month
        ,      trim(to_char(fd, 'Month'))                                               as fiscal_month_name
        ,      to_char(fd, 'yyyy-"0"q')                                                 as fiscal_quarter_desc
        ,      trunc(fd, 'q')-d0+188                                                    as fiscal_quarter_id
        ,      next_day(trunc(add_months(fd,3), 'q')-7,'Sunday')                        as end_of_fis_quarter
        ,      to_number(to_char(fd, 'q'))                                              as fiscal_quarter_number
        ,      extract(year from fd)                                                    as fiscal_year
        ,      extract(year from fd)-212                                                as fiscal_year_id
        ,      next_day(trunc(add_months(d,12),'year')-8,'Sunday')                      as end_of_fis_year
        ,      next_day(trunc(d,'year')-7,'Monday')                                     as start_of_fis_year
        from   data
        )
,    final_dataset as (
        select time_id
        ,      day_name
        ,      day_number_in_week
        ,      day_number_in_month
        ,      calendar_week_number
        ,      case when extract(year from time_id) != extract(year from next_day(time_id, 'Monday')) then 1 else ceil(((time_id-start_of_fis_year)+1)/7) end as fiscal_week_number
        ,      week_ending_day
        ,      week_ending_day_id
        ,      calendar_month_number
        ,      fiscal_month_number
        ,      calendar_month_desc
        ,      calendar_month_id
        ,      fiscal_month_desc
        ,      fiscal_month_id
        ,      days_in_cal_month
        ,      end_of_fis_month - min(time_id) over (partition by fiscal_month_desc) + 1 as days_in_fis_month
        ,      end_of_cal_month
        ,      end_of_fis_month
        ,      calendar_month_name
        ,      fiscal_month_name
        ,      calendar_quarter_desc
        ,      calendar_quarter_id
        ,      fiscal_quarter_desc
        ,      fiscal_quarter_id
        ,      days_in_cal_quarter
        ,      end_of_fis_quarter - min(time_id) over (partition by fiscal_quarter_desc) + 1 as days_in_fis_quarter
        ,      end_of_cal_quarter
        ,      end_of_fis_quarter
        ,      calendar_quarter_number
        ,      fiscal_quarter_number
        ,      calendar_year
        ,      calendar_year_id
        ,      fiscal_year
        ,      fiscal_year_id
        ,      days_in_cal_year
        ,     (end_of_fis_year - start_of_fis_year) + 1 as days_in_fis_year
        ,      end_of_cal_year
        ,      end_of_fis_year
        ,      start_of_fis_year
        from   calendar_data cd
               inner join
               fiscal_data   fd
               using (time_id)
        )
select time_id
,      day_name
,      day_number_in_week
,      day_number_in_month
,      calendar_week_number
,      fiscal_week_number
,      week_ending_day
,      week_ending_day_id
,      calendar_month_number
,      fiscal_month_number
,      calendar_month_desc
,      calendar_month_id
,      fiscal_month_desc
,      fiscal_month_id
,      days_in_cal_month
,      days_in_fis_month
,      end_of_cal_month
,      end_of_fis_month
,      calendar_month_name
,      fiscal_month_name
,      calendar_quarter_desc
,      calendar_quarter_id
,      fiscal_quarter_desc
,      fiscal_quarter_id
,      days_in_cal_quarter
,      days_in_fis_quarter
,      end_of_cal_quarter
,      end_of_fis_quarter
,      calendar_quarter_number
,      fiscal_quarter_number
,      calendar_year
,      calendar_year_id
,      fiscal_year
,      fiscal_year_id
,      days_in_cal_year
,      days_in_fis_year
,      end_of_cal_year
,      end_of_fis_year
from   final_dataset
where  time_id not in (select t.time_id from times t)
;

commit
;

prompt
prompt Gathering stats...
exec dbms_stats.gather_table_stats(user, 'TIMES', degree=>8, method_opt=>'for all columns size 1');

select to_char(sysdate, 'dd/mm/yyyy hh24:mi:ss') as end_time 
from   dual;

prompt
prompt &_num_years years of TIMES data successfully generated.
prompt

undefine _num_years
