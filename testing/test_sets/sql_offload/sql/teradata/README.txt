Some details are included in file/table names as below:

Token   Indicates
------- ---------------------------------
"part"  Partitioned table
"cp"    Columnar partitioned
"range" Row partitioned by RANGE_N
        Followed by an indicator of column(s) used to partition
"case"  Row partitioned by CASE_N
        Followed by an indicator of column(s) used to partition
"pi"    Primary index included
        Followed by an indicator of column(s) in the index
"nopi"  No primary index
"pa"    Primary AMP index included
        Followed by an indicator of column(s) in the index
