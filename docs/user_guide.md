Offload supports several scenarios for offloading data from the RDBMS:

- [Full Offload](#full-offload)
- [Partition-Based Offload](#partition-based-offload)
- Subpartition-Based Offload
- Predicate-Based Offload
- Offloading Joins

# Full Offload

Heap, partitioned and index-organized tables can be fully-offloaded from the RDBMS as follows.

## Example 1: Offload a Full Table
```shell
$ $OFFLOAD_HOME/bin/offload -t SH.PRODUCTS -x
```

A log file containing all of the steps in the offload process will be generated in `$OFFLOAD_HOME/log`. Using the `-v` or `--vv` option will result in more screen output during the offload.

Backend tables can be optionally partitioned, even if the source RDBMS table is not partitioned. See [Managing Backend Partitioning](#managing-backend-partitioning) for details.

It is possible to rename the target backend table when offloading with the `--target-name` option. This can be useful for a number of reasons, including when:
- The source RDBMS table name includes a character that is not supported by the backend (for example, Oracle Database allows `$` to be used for identifiers but this is not supported by Impala or BigQuery)
- The naming standards used for applications in the backend system are different to the source RDBMS application (either for database names or object names)
- The backend idenfieier limitchema cannot accommodate necessary Offload extensions such as when using `DB_NAME_PREFIX`

## Example 2: Change the Offload Target Name
The following example offloads a RDBMS table named `SH.SALES$` to a backend that doesn’t support the `$` character in table names, meaning that the target table needs to be renamed to remove the `$`.

```shell
$ $OFFLOAD_HOME/bin/offload -t 'SH.SALES$' -x --target-name=SH.SALES
```

# Partition-Based Offload

Partition-Based Offload enables some or all partitions of a partitioned table to be offloaded across one or more offload sessions. A common use of this feature is to initially offload the historical partitions for a table and then periodically offload new partitions as they arrive in the RDBMS, appending the data to the backend version of the table.

Partition-Based Offload can be used for the following scenarios:

- [Offloading Range-Partitioned Tables](offloading-range-partitioned-tables)
- [Offloading Interval-Partitioned Tables](offloading-interval-partitioned-tables)
- [Offloading List-Partitioned Tables](offloading-list-partitioned-tables)
- [Offloading List-Partitioned Tables as Range](offloading-list-partitioned-tables-as-range)

## Offloading Range-Partitioned Tables

Tables that are range-partitioned on `DATE`, `TIMESTAMP`, `NUMBER` or `[N]VARCHAR2` columns can be fully or partially offloaded. Partitions can be offloaded contiguously up to a boundary by specifying a high water mark in the offload command. Partition boundaries can be increased in subsequent offloads to append additional partitions without affecting historical partitions.

### Partition Boundary Options
To offload a contiguous range of partitions from a range-partitioned table, one of the following boundary options must be used:

- `--older-than-date`: Offload `DATE` or `TIMESTAMP` partitions with a high water mark less than this value. Synonym for `--less-than-value`, e.g.

```
--older-than-date=2015-10-01
```

- `--older-than-days`: Offload partitions older than this number of days (exclusive, i.e. the boundary partition is not offloaded). This option is suitable for keeping data up to a certain age in the source table and is an alternative to the `--older-than-date` option. If both are supplied, `--older-than-date` will be used, e.g.

```
--older-than-days=90
```

- `--less-than-value`: Offload partitions with a high water mark less than this value. Will accept integer, string or date values and allows intraday date and timestamp values to be specified, e.g.

```
--less-than-value=100000000
--less-than-value=2015-01-01
--less-than-value="2015-01-01 12:00:00"
--less-than-value=M
```
- `--partition-names`: Offload partitions with a high water mark matching that of the named partition, e.g.

```
--partition-names=P201509
```

Boundary options for range-partitioned tables are exclusive and the partition that contains data for the specified high water mark will not be offloaded. Furthermore, Offload will not offload partial partitions. For example, suppose a partition has data with a value range of 2000-01-01 to 2000-01-31. Using `--older-than-date=2000-01-15` will not offload data from 2000-01-01 to 2000-01-14 because the partition has the potential for data up to and including 2000-01-31. Instead, data will be offloaded up to the boundary of the previous range partition. To offload the example partition, an `--older-than-date=2000-02-01` value must be used, but only when loading for the entire partition has completed.

### Offloading with Date Partition Boundaries
The following is an example of offloading all partitions below a given date boundary.

#### Example 3: Offload a Range of Date Partitions
```shell
$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --older-than-date=2015-10-01
```

By default, Example 3 above will use the RDBMS partition key column as the backend partition column for backends that support partitioning (they can be different if required; see [User-Defined Partitioning](#user-defined-partitioning) for details). It is common to use a different granularity for the backend partitions, especially useful when the backend system better supports either larger or fewer partitions. For example, an RDBMS table partitioned by day might be better offloaded to monthly partitions in a Google BigQuery-based backend. See `--partition-granularity` for details of the default granularities for various scenarios and backends.

### Offloading with Numeric Partition Boundaries
For range-partitioned tables with numeric partition keys, the partition granularity of the backend table must be specified. The command syntax differs slightly according to the target backend, as the following examples demonstrate.

#### Example 5: Offload a Range of Numeric Partitions (BigQuery)
For Google BigQuery, the full range of potential partition key values must be specified when creating a table with numeric partitions, hence the additional Offload options in the following example.

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.NUMERIC_PARTITIONED_FACT -x \
      --less-than-value=5000000000 \
      --partition-granularity=10000000 \
      --partition-lower-value=1 \
      --partition-upper-value=99999999999999999
```

***

__NOTE:__ Any partition key data that falls outside the range specified by the lower and upper values will be loaded into the `__UNPARTITIONED__` partition by BigQuery.

***

### Offloading with String Partition Boundaries

Google BigQuery does not support partitioning on STRING columns; therefore the source partition column data must be converted to `INT64` with a custom partition function (see `--partition-functions`) to enable the backend table to be synthetically partitioned.

#### Example 7: Offload a Range of String Partitions (BigQuery)

Google BigQuery does not have native `STRING` partitioning support, but the source string data can still be used to partition the backend table synthetically if a custom UDF is provided to convert the string data to an INT64 type. The following example shows a `VARCHAR2` partition key used as the source for Google BigQuery partitioning. A custom UDF is provided to generate an ASCII value for the first character of the source data and the resulting INT64 value is used to partition the backend table. The full range of potential partition key values must be specified when offloading a table with a `VARCHAR2` partition column.

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.VARCHAR2_PARTITIONED_FACT -x \
      --less-than-value=F \
      --partition-granularity=1 \
      --partition-lower-value=65 \
      --partition-upper-value=150 \
      --partition-functions=MY_UDF_DATASET.STRING_TO_ASCII
```

### Fully Offloading Range-Partitioned Tables with Offload Type

Range-partitioned tables are also supported for full offload, using either the `--offload-type` option with a value of `FULL` or by excluding any of the partition boundary options described above. The following option combinations will fully offload a range-partitioned table:

- `--offload-type=FULL`: Offloads all partitions and creates a simple hybrid view that queries all the data from the backend
- `--offload-type=FULL` plus one of the boundary options: Offloads all partitions and data to the backend but creates a composite hybrid view that queries the data below the specified boundary from the backend and data above the specified boundary from the RDBMS
- None of the offload type or partition boundary options described above: This is the same as specifying `--offload-type=FULL`

### MAXVALUE Partition Considerations

Offloads for range-partitioned tables with a `MAXVALUE` partition as the latest partition will behave differently depending on the offload options used, as follows:

- If neither the `--offload-type` option nor any of the partition boundary options are specified, the `MAXVALUE` partition will be offloaded
- If `--offload-type=FULL` is specified, either with or without a partition boundary option, the `MAXVALUE` partition will be offloaded
- If `--offload-type=INCREMENTAL` is specified, either with or without a partition boundary option, the `MAXVALUE` partition will not be offloaded
- If a partition boundary option is specified but not `--offload-type`, the `MAXVALUE` partition will not be offloaded

***

__NOTE:__ Offloading the `MAXVALUE` partition for a table will prevent any further partition-based offloads for that table.

***

### Multi-Column Partitioning

Oracle Database supports multi-column partitioning, whereby a table is partitioned on a set of columns rather than a single partition key (for example, `PARTITION BY RANGE (yr, mon)`). Offload supports this for range-partitioned tables. Any boundary condition must be provided via the `--less-than-value` option as comma-separated-values. For example:

```
--less-than-value=2015,10
```

Or:

```
--less-than-value=2015-10-31,200
```

When offloading a multi-column partitioned table to Google BigQuery, the generated BigQuery table will be partitioned according to the leading partition key column only. The `--partition-columns` option will only allow one column to be specified when offloading to Google BigQuery.


## Offloading Interval-Partitioned Tables

Offload supports interval-partitioned tables in exactly the same way as range-partitioned tables.

## Offloading List-Partitioned Tables

Tables that are list-partitioned on `DATE`, `TIMESTAMP`, `NUMBER`, `[N]VARCHAR2` or `[N]CHAR` columns can be partially or fully offloaded. Discrete sets of one or more partitions can be offloaded by specifying either partition key values or partition names in the `offload` command. Additional partitions can be offloaded in subsequent offloads without affecting historical partitions.

List Partition Specification
Partitions in a list-partitioned table can be offloaded using either of the options below:

- `--equal-to-values`: Offload partitions by partition key value, accepting partition key literals that match the RDBMS partitions, e.g.

```
--equal-to-values=2015-01-01
--equal-to-values=US --equal-to-values=AP --equal-to-values=EU
--equal-to-values=201501,201502,201503
```

- `--partition-names`: Offload partitions by partition name, e.g.

```
--partition-names=TRANSACTIONS_P2015Q1,TRANSACTIONS_P2015Q2
```

When using `--equal-to-values`, each partition must have its own option specification, as shown above and in Example 8 below.

### Example 8: Offloading Partitions from a List-Partitioned Table

The following example offloads two partitions from a date list-partitioned table:

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --equal-to-values=2015-01-01 --equal-to-values=2015-01-02
```

### Example 9: Offloading Multi-Valued Partitions from a List-Partitioned Table
When using `--equal-to-values`, each option must equal the full high values specification for a single partition. The following example offloads two multi-valued list partitions where the partition key is a numeric representation of year-month:

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.TRANSACTIONS -x --equal-to-values=201501,201502,201503 --equal-to-values=201504,201505,201506
```

Offloads for tables with numeric list-partition keys does not require the `--partition-granularity` option, but for offloading to Google BigQuery the `--partition-lower-value` and `--partition-upper-value` options must still be used. See [Managing Backend Partitioning](#managing-backend-partitioning) for details.

### Example 10: Offloading List-Partitions by Partition Name

The `--partition-names` option is an alternative way to specify multiple partitions to offload and can be useful if partition names are well-formed and known. The following example offloads four partitions from a table that is list-partitioned on financial quarter:

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --partition-names=SALES_P2015Q1,SALES_P2015Q2,SALES_P2015Q3,SALES_P2015Q4
```

### DEFAULT Partition Considerations
Offloads for list-partitioned tables with a DEFAULT partition will behave differently depending on the offload options used, as follows:

- If neither the `--offload-type` nor any of the partition identification options (`--equal-to-values`, `--partition-names`) are specified, the `DEFAULT` partition will be offloaded
- If `--offload-type=FULL` is specified, either with or without a partition identification option, the `DEFAULT` partition will be offloaded
- If `--offload-type=INCREMENTAL` is specified, either with or without a partition identification option, the `DEFAULT` partition will not be offloaded
- If a partition identification option is specified but not `--offload-type`, the `DEFAULT` partition will not be offloaded

***

__NOTE:__ Offloading the DEFAULT partition for a table will prevent any further partition-based offloads for that table.

***

## Offloading List-Partitioned Tables as Range

In cases where a list-partitioned table has been structured to mimic range partitioning, the table can be offloaded exactly as described in [Offloading Range-Partitioned Tables](offloading-range-partitioned-tables). To use this feature the table should adhere to the following:

- Each partition must have a single literal as its high value
- All new partitions must be added with high values that are greater than those that have already been offloaded
- Supported data types must match those referenced in [Offloading Range-Partitioned Tables](offloading-range-partitioned-tables)

In this scenario, a `DEFAULT` list partition will be treated in the same way that a `MAXVALUE` partition is treated for range partition offload (see [MAXVALUE` Partition Considerations](#maxvalue-partition-considerations) for details).

Backend partition granularities differ between range-partitioned tables and list-partitioned tables offloaded as range. See [Managing Backend Partitioning](#managing-backend-partitioning) for details.

### Example 11: Offload a Set of List Partitions Using a Date Range Partition Boundary

The following example offloads all list partitions with a partition key value of less than 2015-02-01.

An example using `--older-than-date`:

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --older-than-date=2015-02-01
```

Boundary options are exclusive and will not offload a partition with a key that equals the supplied value.

If using the `--partition-names` option to specify the partition to use as an offload boundary, the `--offload-predicate-type` option must also be specified with a value of `LIST_AS_RANGE`, otherwise Offload will treat the table as a basic list-partitioned table rather than use range-partitioned table behavior.

# Subpartition-Based Offload

Tables that are range-subpartitioned on `DATE`, `TIMESTAMP`, `NUMBER` or `[N]VARCHAR2` columns can be fully or partially offloaded. With Subpartition-Based Offload, partial offloading is supported even if the top-level partitioning scheme of a table is unsupported, because the subpartition high values are used to determine the offload boundary. To offload a range-subpartitioned table by subpartition, the `--offload-by-subpartition` option must be used.

Subpartition-Based Offload is useful for tables that are organized with ranged data (such as a time series or business date) at subpartition level, rather than partition level. In such cases, the subpartition boundaries can be used to determine the data to be offloaded, even when the partition-level data is also ranged.

When offloading a subpartitioned table with Partition-Based Offload, all subpartitions within the identified range or list partition(s) are offloaded as standard.

## Example 12: Offload a Set of Range Subpartitions Using a Date Range Boundary

The following example demonstrates offloading all subpartitions below a date threshold of 2015-07-01:

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --older-than-date=2015-07-01 --offload-by-subpartition
```

## Subpartition Boundary Options
The Offload options for specifying subpartition boundaries are the same as for range partition offloading. See [Partition Boundary Options](#partition-boundary-options) for details.

Subpartition-Based Offload has an additional restriction, such that the boundary option value must guarantee that no subpartitions have potential for more data to be added. For example, suppose every partition in a table contains two range subpartitions with high values of 90 and 100. In this case, the boundary option of --less-than-value=100 identifies a common subpartition boundary across the entire partition set and this guarantees that all subpartitions below this threshold are “complete” and ready for offloading. However, suppose just one of the partitions has range subpartitions with high values of 90 and 110. In this case, `--less-than-value=100` option would not be able to offload all subpartitions below this threshold because the boundary is not common (i.e. the subpartition with a high value of 110 cannot be guaranteed to be “complete”).

In cases where the specified boundary is not valid, Offload will terminate with a warning and recommend that a common boundary is identified and used. In many cases it is possible to project the boundary value forward until a value is found that guarantees the safe offloading and appending of “complete” subpartitions.

Boundary options are exclusive and will not offload subpartitions with a key that equals the supplied value.

## Other Subpartition-Based Offload Behavior

Subpartition-Based Offload behavior is the same as Partition-Based Offload in almost all cases. Considerations such as `MAXVALUE` subpartitions, full offloading with `--offload-type`, moving the high water mark in the hybrid view, using multi-column subpartition keys and so on are the same for range subpartitions as they are for range partitions. See Offloading Range-Partitioned Tables for details. Note that any examples would need the `--offload-by-subpartition` option to apply the same principle to range-subpartitioned tables.

# Predicate-Based Offload

Offload supports the offloading of subsets of data (or an entire table) by predicate. This feature applies to any table type (partitioned or non-partitioned) and is known as Predicate-Based Offload.

With this feature, three offloading patterns are possible:

- [Simple Predicate-Based Offload](simple-predicate-based-offload): Offload one or more non-overlapping subsets of data from a table
- [Late-Arriving Predicate-Based Offload](late-arriving-predicate-based-offload): Offload one or more late-arriving subsets of data from a table or partition that has already been offloaded
- [Intra-Day Predicate-Based Offload](intra-day-predicate-based-offload): Offload a new partition in non-overlapping subsets as soon as each subset of data is loaded

Predicates are provided using a simple grammar as described in [Predicate Grammar](predicate-grammar).

## Simple Predicate-Based Offload

### Example 13: Offload a Subset of Data from a Table Using a Predicate

The following example demonstrates a simple Predicate-Based Offload scenario by offloading all data for the ‘Electronics’ category.

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.PRODUCTS -x --offload-predicate='column(PROD_CATEGORY) = string("Electronics")'
```

All data matching this predicate will be offloaded and the predicate will be added to the offload boundary in the hybrid view. Predicates are additive and any predicate that has been offloaded will not be re-offloaded, even if specified in future offload commands.

Backend tables can be optionally partitioned when using simple Predicate-Based Offload. See [Managing Backend Partitioning](#managing-backend-partitioning) for more details.

## Late-Arriving Predicate-Based Offload

The late-arriving scenario caters for tables or (sub)partitions that sometimes have small subsets of data arriving after the original offload has taken place. This scenario benefits from the efficiencies of Full Offload or (Sub)Partition-Based Offload but with the flexibility of Predicate-Based Offload for “topping-up” the offloaded data. This pattern can be used with fully offloaded tables, range-(sub)partitioned tables or list-partitioned tables offloaded with range partition semantics.

- When using Late-Arriving Predicate-Based Offload with tables previously offloaded with (Sub)Partition-Based Offload:
  - The Predicate-Based Offload command must include the `--offload-predicate-type` option with a value of `RANGE` or `LIST_AS_RANGE`
  - The late-arriving subset of data to be offloaded must not overlap with any data that has already been offloaded
  - The late-arriving subset of data to be offloaded must not reference data in (sub)partitions that have not yet been offloaded

- When using Late-Arriving Predicate-Based Offload with tables previously offloaded with Full Offload:
  - The `--offload-predicate-type` option is not required
  - The late-arriving subset of data to be offloaded must not overlap with any data that has already been offloaded

The following examples demonstrate Late-Arriving Predicate-Based Offload for a table previously offloaded with Full Offload and a set of partitions previously offloaded with Partition-Based Offload.

### Example 14: Offload a Late-Arriving Subset of Data from a Previously-Offloaded Table

In the following example, the SH.CUSTOMERS table has been fully-offloaded using Full Offload. The Predicate-Based Offload command below is used to additionally offload a small set of data for new customers that have been inserted since the original offload took place.

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.CUSTOMERS -x \
      --offload-predicate='column(CUST_CREATED_DATE) > datetime(2019-01-15 13:23:34)'
```

All data matching this predicate will be offloaded but the predicate itself will not be added to the hybrid view or its metadata.

### Example 15: Offload a Late-Arriving Subset of Data from a Previously-Offloaded Range Partition
In the following example, the SH.SALES table has previously been offloaded using Partition-Based Offload up to and including data for `2019-01-15`. The Predicate-Based Offload command below is used to additionally offload a small set of data for product 123 that has arrived late and been loaded into a partition that has already been offloaded.

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x \
      --offload-predicate='column(TIME_ID) = datetime(2019-01-15) and column(PROD_ID) = numeric(123)' \
      --offload-predicate-type=RANGE
```

All data matching this predicate will be offloaded but the predicate itself will not be added to the offload boundary or metadata for the hybrid view (only the existing Partition-Based Offload boundary will be used).

## Intra-Day Predicate-Based Offload

This offload pattern can be used to switch between Partition-Based Offload and Predicate-Based Offload as required. It can be useful as a means to offload the majority of a partitioned table by partition boundaries, but swap to offloading subsets of data for a new partition as soon as they arrive (rather than wait for the entire partition to be loaded before offloading). This can ensure that data is available in the target backend earlier than would otherwise be possible. This pattern can only be used with range-partitioned tables or list-partitioned tables offloaded with range partition semantics.

### Example 16: Offload a Subset of Data for a New Range Partition (Intra-Day Offloading)

In the following example, the historical data for the SH.SALES table is offloaded by Partition-Based Offload (range partitioned by TIME_ID) at time T0 to begin the offload lifecycle for this table. For new data, rather than wait a full day for an entire partition of data to be ready, data is instead offloaded as soon as each product set is loaded into the new partition (3 separate loads at times T1, T2, T3). When all loads and offloads have completed, the table metadata is reset (at time T4) and the table is ready to repeat the same Predicate-Based Offload pattern for the next processing cycle.

| Time | Description | Command |
| :---- | :---- | :---- |
| T0 | Offload all history by range partition | `$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --older-than-date=2020-07-01`  |
| T1 | Offload first set of products loaded at time T1 | `$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --offload-predicate='column(TIME_ID) = datetime(2020-07-01) and column(PROD_ID) in (numeric(123), numeric(234))'        --offload-predicate-type=RANGE_AND_PREDICATE`  |
| T2 | Offload second set of products loaded at time T2 | `$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x        --offload-predicate='column(TIME_ID) = datetime(2020-07-01) and column(PROD_ID) in (numeric(345), numeric(456))'        --offload-predicate-type=RANGE_AND_PREDICATE`  |
| T3 | Offload third set of products loaded at time T3 | `$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x        --offload-predicate='column(TIME_ID) = datetime(2020-07-01) and column(PROD_ID) in (numeric(567), numeric(678))'        --offload-predicate-type=RANGE_AND_PREDICATE`  |
| T4 | All products now loaded and offloaded, reset the metadata for the table | `$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --older-than-date=2020-07-02`  |

The Predicate-Based Offload commands for T1-T3 require that the `--offload-predicate` option includes a predicate for the Partition-Based Offload partition key and that it matches the value of the new data (`TIME_ID=2020-07-01` in this example). The `--offload-predicate-type` option must also be used (see option reference for valid values).

When the metadata is reset after the end of a sequence of Predicate-Based Offload commands (as per T4 above), either the same pattern of offloading can continue for the next partition or, if preferred, Partition-Based Offload can resume (i.e. wait for the next partition to be fully-loaded before offloading in one command as described in [Partition-Based Offload](partitioned-based-offload)).

## Predicate Grammar

At present, the offload predicate DSL or grammar used for the `--offload-predicate` option covers a simple range of possibilities, based on a `<column> <operator> <value>` model.

### Predicate Form

Predicates can take several forms:

- `<column> <operator> <value>`
- `<column> [NOT] IN (<value>[, <value>, ...])`
- `<column> IS [NOT] NULL`

The DSL allows specification of any number of predicates in a range of forms, which can be logically combined and nested with `AND/OR`, for example:

- `<predicate>) AND|OR <predicate>`
- `(<predicate> AND|OR <predicate>) AND|OR (<predicate>)`

Parentheses are required to form logical predicate groups, as shown in the second example above. Whitespace is insignificant.

### Column Form
Predicates always contain a single column written with the `column` keyword and a “function-call” syntax. Columns are not case-sensitive and can include aliases. The following examples are all valid:

- `column(CUST_ID)`
- `column(SALES.CUST_ID)`
- `column(cust_id)`

Columns must exist in the table being offloaded and expressions/functions are not supported.

### Operator Form

The basic SQL operators are supported and are written as in SQL. Member operators and null-value expressions are also supported. The full range is as follows:

- `=, !=, <, <=, >, >=`
- `[NOT] IN`
- `IS [NOT] NULL`

### Value Form

Values must be qualified with a data type classification and have a “function-call” syntax as follows:

- `string("This is a string")`
- `numeric(123)`
- `datetime(2020-07-01)`

String values use the `string` keyword and double-quotes as shown above. Double-quote characters can be included in the string value by escaping with a backslash.

Numeric values use the `numeric` keyword and both signed integral and signed decimal values are supported with the same keyword.

Date/time values use the `datetime` keyword and can be specified at several levels of precision from dates to timestamps with nanosecond precision. The following are all valid examples of a `datetime` value:

- `datetime(2020-07-01)`
- `datetime(2020-07-01 13:01:01)`
- `datetime(2020-07-01 13:01:01.123)`
- `datetime(2020-07-01 13:01:01.123456789)`

Value lists for use in the `[NOT] IN` operator must be surrounded by parentheses and separated by commas. The values in a value list must be specified as normal, for example:

- `column(cust_id) IN (numeric(120), numeric(121), numeric(122))`

The predicate DSL currently supports columns of the following Oracle Database data types:

### Table 2: Predicate-Based Offload DSL Data Type Support

| DSL Data Type Class | Supported Oracle Database Data Types |
| :---- | :---- |
| String | `VARCHAR2`, `CHAR`, `NVARCHAR2`, `NCHAR` |
| Numeric | `NUMBER`, `FLOAT`, `BINARY_FLOAT`, `BINARY_DOUBLE` |
| Datetime | `DATE`, `TIMESTAMP` |


# Offload Data Types

When offloading data, Offload will map data types from the source RDBMS to the backend database automatically. However, as with most operations in Offload, it is possible to override many of the default data type mappings if required. See Tables 4 and 6 below for details.

## Supported Data Types

GOE currently supports all of the Oracle Database data types listed in Table 4 below. Oracle Database tables that contain columns of any data type not listed cannot currently be offloaded.

## Default Data Type Mappings

Table 4 lists the default data type mappings when offloading data from Oracle Database to Impala (Cloudera Data Hub, Cloudera Data Platform), Google BigQuery, Snowflake or Azure Synapse Analytics.

### Table 4: Offload Default Data Type Mappings (Oracle Database)

***

__NOTE:__ All backends other than Google BigQuery are currently disabled.

***

| Oracle Database | Impala | Google BigQuery | Snowflake | Azure Synapse Analytics | Comments |
| :---- | :---- | :---- | :---- | :---- | :---- |
| `CHAR` | `STRING` | `STRING` | `VARCHAR` | `char` | |
| `NCHAR` | `STRING` | `STRING` | `VARCHAR` | `nchar` | |
| `CLOB` | `STRING` | `STRING` | `VARCHAR` | `varchar(max)` | |
| `NCLOB` | `STRING` | `STRING` | `VARCHAR` | `nvarchar(max)` | |
| `VARCHAR2` | `STRING` | `STRING` | `VARCHAR` | `varchar` | |
| `NVARCHAR2` | `STRING` | `STRING` | `VARCHAR` | `nvarchar` | |
| `RAW` | `STRING` | `BYTES` | `BINARY` | `varbinary` | |
| `BLOB` | `STRING` | `BYTES` | `BINARY` | `varbinary(max)` | |
| `NUMBER(<=4,0)` | `BIGINT` | `INT64` | `NUMBER(p,0)` | `smallint` | |
| `NUMBER([5-9],0)` | `BIGINT` | `INT64` | `NUMBER(p,0)` | `int` | |
| `NUMBER([10-18],0)` | `BIGINT` | `INT64` | `NUMBER(p,0)` | `bigint` |  |
| `NUMBER(>18,0)` | `DECIMAL(38,0)` | `NUMERIC` `BIGNUMERIC` | `NUMBER(38,0)` | `numeric(38,0)` | See [Offloading Numeric Data to Google BigQuery](offloading-numeric-data-to-google-bigquery) |
| `NUMBER(*,*)` | `DECIMAL(38,s)` | `NUMERIC` `BIGNUMERIC` | `NUMBER(p,s)` | `numeric(p,s)` | See [Offloading Numeric Data to Google BigQuery](offloading-numeric-data-to-google-bigquery) |
| `FLOAT` | `DECIMAL` | `NUMERIC` | `NUMBER(p,s)` | `numeric(38,18)` | |
| `BINARY_FLOAT` | `FLOAT` | \- | \- | `real` | See [Floating Point Data Types in Google BigQuery](offload-floating-point-data-types-in-google-bigquery) |
| `BINARY_DOUBLE` | `DOUBLE` | `FLOAT64` | `FLOAT` | `float` | |
| `DATE` | `TIMESTAMP` | `DATETIME` | `TIMESTAMP_NTZ` | `datetime2` | |
| `TIMESTAMP` | `TIMESTAMP` | `DATETIME` | `TIMESTAMP_NTZ` | `datetime2` | See [Offloading High-Precision Timestamp Data to Google BigQuery and Azure Synapse Analytics](offload-offloading-high-precision-timestamp-data-to-google-bigquery) |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` | `TIMESTAMP` | `TIMESTAMP_TZ` | `datetimeoffset` | See [Offloading High-Precision Timestamp Data to Google BigQuery and Azure Synapse Analytics](offload-offloading-high-precision-timestamp-data-to-google-bigquery) and [Offloading Time Zoned Data](offload-offloading-time-zoned-data) |
| `INTERVAL DAY TO SECOND` | `STRING` | `STRING` | `VARCHAR` | `varchar` | See [Offloading Interval Data Types](offload-offloading-interval-data-types) |
| `INTERVAL YEAR TO MONTH` | `STRING` | `STRING` | `VARCHAR` | `varchar` | See [Offloading Interval Data Types](offload-offloading-interval-data-types) |

## Data Sampling During Offload

GOE fine-tunes the default data mappings by sampling data for all columns in the source RDBMS table that are either date or timestamp-based or defined as a number without a precision and scale.

Offload bases the volume of data to be sampled on the size of the RDBMS table. This can be overridden by adding the `--data-sample-percent` option to the offload command (specifying a percentage between 0 and 100, where 0 disables sampling altogether).

Offload determines the degree of parallelism to use when sampling data from the value of `DATA_SAMPLE_PARALLELISM`. This can be overridden by adding the `--data-sample-parallelism` option to the offload command (specifying a degree of 0 or a positive integer, where 0 disables parallelism)

## Offloading Not Null Columns to Google BigQuery

Google BigQuery allow columns to be defined as mandatory (i.e. NOT NULL). When offloading a table to Google BigQuery for the first time, by default Offload will copy mandatory column definitions from the RDBMS to the backend. This behavior is defined globally by the `OFFLOAD_NOT_NULL_PROPAGATION` configuration option, which defaults to `AUTO` (i.e. propagate all `NOT NULL` constraints from the RDBMS to the backend). To avoid propagating any `NOT NULL` constraints, this option can be set to `NONE`.

Offload only considers columns to be mandatory if they are defined by Oracle Database as not nullable (`DBA_TAB_COLUMNS.NULLABLE = 'NO'`). The following `NOT NULL` Oracle Database columns are not automatically propagated by Offload:

- Columns with user-defined check constraints rather than a `NOT NULL` definition (e.g. `CHECK (column_name IS NOT NULL)`)
- Columns defined with `NOT NULL ENABLE NOVALIDATE`

To override the global `OFFLOAD_NOT_NULL_PROPAGATION` configuration value, or to include columns with mandatory constraints that are not automatically propagated, the `--not-null-columns` option can be added to an Offload command. This option accepts a list of one or more valid columns, one or more wildcards, or a combination of the two. Offload will define all specified columns as `NOT NULL` when creating the backend table, regardless of their status in the RDBMS.

Care should be taken when propagating constraints for columns that cannot be guaranteed to contain no NULLs. During validation of the staging data, Offload will specifically check for NULLs for any column that is defined as mandatory in the backend.

## Offloading Numeric Data to Google BigQuery

Google BigQuery provides two decimal data types: `NUMERIC` and `BIGNUMERIC`. The `NUMERIC` data type has a specification of `(38,9)` with a fixed decimal point, meaning a maximum of 29 digits to the left of the decimal point and a maximum of 9 digits to the right. The BIGNUMERIC data type has a specification of `(76,38)` with a fixed decimal point, meaning a maximum of 38 digits to the left of the decimal point and a maximum of 38 digits to the right (more precisely, the specification of `BIGNUMERIC` is `(76,38)`, allowing for some numbers with 39-digits to the left of the decimal point).

When offloading numeric data such as decimals or large integrals to Google BigQuery, Offload determines which BigQuery type is most appropriate, based on either the known precision and scale of Oracle columns of type `NUMBER(p,s)` or from sampled data for Oracle columns of unbounded type `NUMBER`. Data that offloads to NUMERIC by default can be offloaded to `BIGNUMERIC` with the `--decimal-columns` and `--decimal-columns-type` override options (see [Table 6: Offload Override Data Type Mappings (Oracle Database)](offload-override-data-type-mappings-oracle-database) below).

For numeric data that exceeds the specifications of NUMERIC and BIGNUMERIC, Offload offers two options:

- Use decimal rounding options during offload (see Decimal Scale Rounding below)
- Offload numeric data to a floating point type (see Converting Numeric Data to Double below)

***

__NOTE:__ Both of these options result in some data change and it will be for users to decide if this is a valid course of action.

***

## Floating Point Data Types in Google BigQuery
Google BigQuery provides a single 64-bit floating point data type (`FLOAT64`). This means that the Oracle Database 32-bit `BINARY_FLOAT` data type does not have a corresponding type and will be offloaded to the 64-bit floating point data type. This has potential to be lossy - see [Lossy Data Operations](lossy-data-operations) (see below).

## Offloading High-Precision Timestamp Data to Google BigQuery

At the time of writing, Google BigQuery’s `DATETIME` and `TIMESTAMP` data types both support microsecond precision (i.e. a precision of 6). Oracle Database supports up to nanosecond precision (i.e. a precision of 9) with its `TIMESTAMP` and `TIMESTAMP WITH TIME ZONE` data types. When Offload detects a source column with a higher level of precision than that supported by the backend (e.g. when attempting to offload `TIMESTAMP(7)` to BigQuery), it will terminate. In such cases, the `--allow-nanosecond-timestamp-columns` option will allow the offload operation to continue, but should ideally be used only when the data to be offloaded is known to be at microsecond precision or lower.

***

__NOTE:__ Allowing high-precision timestamp columns to be offloaded to Google BigQuery is potentially lossy (see [Lossy Data Operations](lossy-data-operations) below for the implications of this).

***

## Offloading Time Zoned Data
For Oracle Database columns of `TIMESTAMP WITH TIME ZONE` data type, data is normalized to Coordinated Universal Time (UTC) during offload to Google BigQuery. Offload converts named time zones, e.g. `2016-01-14 22:39:44 US/Pacific`, to time zone offsets, e.g. `2016-01-14 10:39:44 -8:00` to ensure backend systems can process the values regardless of their time zone database edition.

## Offloading Interval Data Types

Oracle Database `INTERVAL DAY TO SECOND` and `INTERVAL YEAR TO MONTH` data types are offloaded as strings in a normalized format, as shown in Table 5 below.

### Table 5: Interval Data Type String Formats

| Interval Type | Sample Source Data | Google BigQuery |
| :---- | :---- | :---- |
| Day to second (+ve) | \+000000001 02:15:30.123456000 | \+1 02:15:30.123456 |
| Day to second (-ve) | \-000000001 02:15:30.123456000 | \-1 02:15:30.123456 |
| Year to month (+ve) | \+000000001-01 | \+1-1 |
| Year to month (-ve) | \-000000001-01 | \-1-1 |

## Override Data Type Mappings

When initially offloading a table, it is possible to override some of the default data type mappings to change the specification of the backend table. Table 6 lists the override mappings and associated options that are available when offloading data.

Table 6: Offload Override Data Type Mappings (Oracle Database)

| Oracle Database | Offload Option | BigQuery | Comments |
| :---- | :---- | :---- | :---- |
| `CHAR` | `--unicode-string-columns` | \- |  |
| `VARCHAR2` | `--unicode-string-columns` | \- |  |
| `CLOB` | `--unicode-string-columns` | \- |  |
| `DATE` | `--date-columns` | `DATE` |  |
|  | `--timestamp-tz-columns` | `TIMESTAMP` |  |
|  | `--variable-string-columns` | `STRING` |  |
| `TIMESTAMP` | `--date-columns` | `DATE` |  |
|  | `--timestamp-tz-columns` | `TIMESTAMP` |  |
|  | `--variable-string-columns` | `STRING` |  |
| `NUMBER` | `--integer-1-columns` | `INT64` | Use for `NUMBER([1-2],0)` |
|  | `--integer-2-columns` | `INT64` | Use for `NUMBER([3-4],0)` |
|  | `--integer-4-columns` | `INT64` | Use for `NUMBER([5-9],0)` |
|  | `--integer-8-columns` | `INT64` | Use for `NUMBER([10-18],0)` |
|  | `--integer-38-columns` | `INT64` | Use for `NUMBER(>18,0)` |
|  | `--decimal-columns` | `NUMERIC` `BIGNUMERIC` | Use for `NUMBER(p,s)` |
|  | `--double-columns` | `FLOAT64` | See [Converting Numeric Data to Double](offload-converting-numeric-data-to-double) |
| `FLOAT` | `--double-columns` | `FLOAT64` | See [Converting Numeric Data to Double](offload-converting-numeric-data-to-double) |
| `BINARY_FLOAT` | `--double-columns` | \- | See [Converting Numeric Data to Double](offload-converting-numeric-data-to-double) |

### Example 18: Overriding Data Types During an Offload

In the following example, the SH.SALES table is offloaded with several data type overrides.

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x \
      --date-columns=TIME_ID \
      --integer-4-columns=PROD_ID,CHANNEL_ID \
      --decimal-columns-type=10,2 --decimal-columns=AMOUNT_SOLD
```

Each override can be given one or more columns. The `--decimal-columns` override can be used to choose a BigQuery `BIGNUMERIC` over `NUMERIC` (or vice-versa if required). This override option behaves differently to the others in that it must be paired with a `--decimal-columns-type` option to define the precision and scale for the columns listed. Multiple different names/type pairings can be provided in the same command (see `--decimal-columns` for further details).

Data type override options support use of the wildcard character `*`. This is useful when a logical model has column naming rules indicating the data they contain. The example above could use `--decimal-columns=AMOUNT*` to match multiple columns containing monetary values.

## Managing Exceptional Data

No two systems align perfectly with their data type specifications and implementations. There are several cases where data in the source Oracle Database table might not be easily offloaded to the backend system. Some have already been described, but include:

- Source RDBMS numbers with a precision greater than that supported by the backend (for example, the 38 digit limit of BigQuery `BIGNUMERIC`)
- Source RDBMS timestamps with a higher level of precision than BigQuery’s microsecond limit
- Source RDBMS 32-bit floating point numbers offloading to a backend system without a 32-bit floating point data type (e.g. Google BigQuery)
- Source RDBMS large objects (LOBs) with data that exceeds the limit of the corresponding backend data type (e.g. Google BigQuery’s row size limit)

When exceptional data is detected, Offload will in most cases terminate with an exception (and in some cases provide advice on how to proceed).

***

__NOTE:__ Offload will never attempt to automatically offload exceptional data when it would cause a potential data loss to do so. In all cases, the user must provide an explicit course of action by providing the appropriate options with the offload command.

***

## Decimal Scale Rounding

The Oracle Database `NUMBER` data type is extremely flexible and caters for high-scale decimal data that exceeds the limits available in some target backend systems (such as Snowflake). To offload decimal data that exceeds the specification of the backend system, the `--allow-decimal-scale-rounding` option must be used in conjunction with the `--decimal-columns-type` and `--decimal-columns` options to define the target type for the data. By using the rounding option, the user acknowledges that some data loss due to decimal rounding is likely and acceptable.

## Converting Numeric Data to Double

Using the `--double-columns` override option to offload high-precision/scale numeric data or 32-bit floating point data to a 64-bit double can lead to data loss and should only be considered when there is no alternative and the consequences are both understood and acceptable.

## Converting Floating Point Special Values

Not all backend systems support floating point special values such as NaN and Infinity. For such systems, Offload will not allow `BINARY_FLOAT` or `BINARY_DOUBLE` columns to be offloaded without the `--allow-floating-point-conversions` override option. Using this option will enable Offload to convert special floating point values to `NULL`. This option currently applies only to Azure Synapse Analytics.

## Lossy Data Operations

To summarize, the potentially-lossy offload operations and their corresponding options are as follows:

- Rounding decimals: Using the --allow-decimal-scale-rounding option to round decimals that exceed the backend specification for decimal data
- Offloading sub-microsecond timestamps: Using the --allow-nanosecond-timestamp-columns option to offload columns with a sub-microsecond specification to a backend system that doesn’t support the same level of precision
- Converting data to a 64-bit floating point number: Using the `--double-columns` option to offload decimal or 32-bit floating-point data that would not otherwise be possible

***

__NOTE:__ Enabling lossy offloads can cause wrong results with some hybrid queries when data loss has occurred; such as when Smart Connector pushes down an equality or inequality predicate containing values from the lossy column(s). It is not advised to enable lossy data type overrides for columns that are likely to be used in hybrid query predicates or joins unless the offload was known to be lossless.

***

# Offload Transport

Offload Transport describes the part of the offload process that:

- Copies the source RDBMS data to a staging location
- Validates the staged data
- Loads the staged data into the backend target table

The two-phased approach of staging and loading data provides two main benefits:

- Atomicity: The load from staged data to target table either fully succeeds or completely fails
- Partitioning: For backend tables that use GOE’s synthetic partitioning scheme, Offload can synthesize partition key values when loading the table

The transport phase of an offload is split into four main operations:

- [Transport Data to Staging](transport-data-to-staging)
- [Validate Staged Data](validate-staged-data)
- [Validate Type Conversions](validate-type-conversions)
- [Load Staged Data](load-staged-data)

## Transport Data to Staging

GOE uses one of a number of tools to extract data from the source RDBMS. The appropriate tool will be chosen automatically based on configuration preferences. Each tool reads the source data to offload and attempts to split the source data equally and in a non-overlapping way between concurrent reader processes.

Data is staged in either Avro or Parquet format (depending on the backend platform) and native data types are used where possible. For Avro staging files, `STRING` is used where there is no identical match between Avro and the source RDBMS table. For example, Oracle Database `DATE`, `TIMESTAMP` and `NUMBER` data types have no direct equivalent in Avro; therefore data is staged as STRING and converted to the appropriate backend data type during the final loading phase.

The data extraction tools available to Offload are:

- [Google Cloud Platform Dataproc](google-cloud-platform-dataproc)
- [Apache Spark](apache-spark)
- [Query Import](query-import)

Dataproc Serverless is the recommended extraction tool.

Data is staged to cloud storage and requires a small amount of configuration:

- `OFFLOAD_FS_SCHEME`: The storage scheme to which the offloaded data will be staged. For Google BigQuery the only recommended setting is `gs`. An ad hoc override is available with the `--offload-fs-scheme` option
- `OFFLOAD_FS_CONTAINER`: The name of the bucket or container to be used for offloads. An ad hoc override is available with the `--offload-fs-container` option
- `OFFLOAD_FS_PREFIX`: The storage subdirectory defined within the bucket/container (or can be an empty string if preferred). An ad hoc override is available with the `--offload-fs-prefix` option


### Google Cloud Platform Dataproc

Two flavours of Dataproc are supported by GOE:

- Dataproc Serverless
- Dataproc

`gcloud` is used as an interface for both services.

To use Dataproc Serverless, at a minimum, the following configurations should be defined:

- `GOOGLE_DATAPROC_PROJECT`
- `GOOGLE_DATAPROC_REGION`
- `GOOGLE_DATAPROC_SERVICE_ACCOUNT`
- `GOOGLE_DATAPROC_BATCHES_VERSION`

To use Dataproc, at a minimum, the following configurations should be defined:

- `GOOGLE_DATAPROC_CLUSTER`
- `GOOGLE_DATAPROC_PROJECT`
- `GOOGLE_DATAPROC_REGION`
- `GOOGLE_DATAPROC_SERVICE_ACCOUNT`

Please review your `offload.env` file to see other options.

The number of tasks in an offload transport job is defined by `OFFLOAD_TRANSPORT_PARALLELISM` or per offload with the `--offload-transport-parallelism` option. In Dataproc Serverless the requested configuration will be scaled up to match `OFFLOAD_TRANSPORT_PARALLELISM` automatically. It should be noted that in standard Dataproc defining more tasks than there are available Spark executors will result in queuing. Therefore, `OFFLOAD_TRANSPORT_PARALLELISM` should ideally be no more than the number of available executors.


### Apache Spark

It is possible to use any Spark service with GOE but please note that for Google BigQuery offloads, Dataproc Serverless is the recommended Spark service.

When using Apache Spark directly, two interfaces to Spark are supported: Spark Submit and Spark Thrift Server. The interface used by Offload is chosen automatically, based on configuration. If multiple interfaces are configured for use, the order of priority is Spark Thrift Server then Spark Submit. SparkSQL is used in all cases (via a PySpark script for Spark Submit, or as pure SQL for Spark Thrift Server) to extract the data to be offloaded from the source RDBMS table.

When offloading to cloud warehouses such as Google BigQuery, it is typical to use Spark Standalone (GOE includes a Transport package containing Spark Standalone components for this purpose), although an existing Spark cluster can be utilized if available.

Spark Submit is available for use by Offload if `OFFLOAD_TRANSPORT_CMD_HOST` and `OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE` are defined. When using a Spark Standalone cluster jobs will be submitted to the cluster defined in `OFFLOAD_TRANSPORT_SPARK_SUBMIT_MASTER_URL`.

Spark Thrift Server is available for use by Offload if `OFFLOAD_TRANSPORT_SPARK_THRIFT_HOST` and `OFFLOAD_TRANSPORT_SPARK_THRIFT_PORT` are defined. The Spark Thrift server can be configured to keep Spark executors alive between offloads, thereby removing process startup costs and providing low-latency offloads. At higher volumes this benefit becomes negligible.

The number of tasks in an offload transport Spark job is defined by `OFFLOAD_TRANSPORT_PARALLELISM` or per offload with the `--offload-transport-parallelism` option. It should be noted that defining more tasks than there are available Spark executors will result in queuing. Therefore, `OFFLOAD_TRANSPORT_PARALLELISM` should ideally be no more than the number of available executors.

Default configuration is appropriate for the majority of offloads but occasionally the nature of the source RDBMS table requires tuning of the following:

- `--offload-transport-fetch-size` can be used on a per offload basis to override `OFFLOAD_TRANSPORT_FETCH_SIZE`. This can be useful to manage memory requirements for tables with a large row size (such as when offloading Oracle LOB data), by reducing the fetch size
- `OFFLOAD_TRANSPORT_SPARK_OVERRIDES` or the per offload option `--offload-transport-jvm-overrides` can be used to inject JVM parameters into the Spark Submit command line. This has no effect for Spark Thrift Server because its configuration is managed independently
- `OFFLOAD_TRANSPORT_SPARK_PROPERTIES` or the per offload option `--offload-transport-spark-properties` can be used to modify Spark attributes. Note that because Spark Thrift Server configuration is managed independently, some attributes do not have any effect

### Query Import

Query Import is used for for low-volume offloads. Data is extracted and staged by Offload itself and not using an external tool. This avoids delays incurred when invoking Spark Submit. Non-partitioned tables with a source RDBMS size smaller than `--offload-transport-small-table-threshold` are eligible for Query Import. Query Import does not run in parallel, i.e. `OFFLOAD_TRANSPORT_PARALLELISM` is ignored.

### RDBMS Options

Connections to the source RDBMS are made to the address defined in `OFFLOAD_TRANSPORT_DSN`. This defaults to `ORA_CONN` but can be overridden if data needs to be extracted from an alternative address (such as from a replica database).

Concurrent offload transport processes open independent sessions in the source RDBMS. If a high value for `OFFLOAD_TRANSPORT_PARALLELISM` is required then consideration should be given to any session limits in the RDBMS. The `OFFLOAD_TRANSPORT_CONSISTENT_READ` parameter or the per offload option `--offload-transport-consistent-read` can be used to ensure that all concurrent extraction queries reference a specific point in time. When set to true, extraction queries will include an `AS OF SCN` clause. When the source RDBMS table or input partitions are known to be cold (i.e. not subject to any data modifications), this can be set to false to reduce resource consumption.


## Validate Staged Data

Once data has been staged, it is validated to ensure that the number of staged rows matches the number of rows read from the source RDBMS. While doing this Offload might also:

- Check for `NULL` values in any custom partition scheme defined using the `--partition-columns` option. A positive match results in a warning
- Check for `NULL` values in any column defined as mandatory (i.e. `NOT NULL`)
- Check for any NaN (Not a Number) special values if the source table has floating point numeric data types. This is because source RDBMS and target backend systems do not necessarily treat NaN values consistently. A positive match results in a warning
- Check for lossy decimal rounding as described in [Decimal Scale Rounding](decimal-scale-rounding)
- Check for source data that is invalid for the chosen backend partition scheme (if applicable), such as numeric data outside of any `--partition-lower-value`/`--partition-upper-value` range. A positive match results in a warning


## Validate Type Conversions

Data types used for staging data will rarely match those of the backend target table. Data is converted to the correct type when it is loaded into the final target table. This stage therefore verifies that there will be no invalid conversions when loading. While this is a duplication of type conversions in the [Load Staged Data](load-staged-data) phase, it provides the advantage of checking the data before the more compute-intensive data load and is able to report all columns with data issues in a single pass.

### Example 19: Catching Invalid Data Type Conversions

In the following example, the SH.SALES table is offloaded to a Hadoop cluster with an invalid data type for two columns: the data in the PROD_ID and CUST_ID columns is not compatible with the user requested single-byte integer data type.

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --integer-1-columns=PROD_ID,CUST_ID
```

This results in the following exception:

```
CAST() of load data will cause data loss due to lack of precision in target data type in 6887232 rows
Failing casts are:
    (`prod_id` IS NOT NULL AND CAST(`prod_id` AS TINYINT) IS NULL)
    (`cust_id` IS NOT NULL AND CAST(`cust_id` AS TINYINT) IS NULL)
The SQL below will assist identification of problem data:
SELECT PROD_ID
,      CUST_ID
FROM   `sh_load`.`sales`
WHERE  (`prod_id` IS NOT NULL AND CAST(`prod_id` AS TINYINT) IS NULL)
OR     (`cust_id` IS NOT NULL AND CAST(`cust_id` AS TINYINT) IS NULL)
LIMIT 50
```

The exception provides three important pieces of information:

- The number of rows with issues
- The columns/conversions with issues
- A SQL statement to use offline to review a sample of the problematic data


## Load Staged Data

In this phase of an offload staged data is converted to correct data types as described in [Validate Type Conversions](validate-type-conversions) and inserted into the target backend table. Where available, performance metrics from the backend system are recorded in the Offload log file written to `$OFFLOAD_HOME/log`.

## Offload Transport Chunks

The source RDBMS data is offloaded in “chunks”. A chunk can comprise an entire table or a set of one or more partitions. The sequence of four operations described above are executed for each chunk until the whole input set has been offloaded. A final task in this phase, for backend systems that support it, is to update statistics on the freshly offloaded data.

For partitioned RDBMS tables, data is offloaded in sets of partitions. Input partitions are grouped into chunks, based on either the number of partitions or cumulative partition size (including subpartitions where relevant). For non-partitioned tables the table itself is considered a single chunk.

The size limit for a partition chunk is defined by `MAX_OFFLOAD_CHUNK_SIZE` or per offload with the `--max-offload-chunk-size` option. The maximum number of partitions in a chunk is defined by `MAX_OFFLOAD_CHUNK_COUNT` or per offload with the `--max-offload-chunk-count` option. Both size and count thresholds are active at the same time, therefore the first threshold to be breached is the one that closes the chunk. If the first partition in a chunk breaches the size threshold then it will be included for offload and that chunk will be greater in size than the configured threshold.

Sets of partitions are an effective way of managing resource consumption. For example, if a source RDBMS typically has partitions of 8GB in size and the backend system can comfortably load 64GB of data at a time without impacting other users, `MAX_OFFLOAD_CHUNK_SIZE` can be set to `64G` and roughly 8 partitions will be offloaded per chunk.

When calculating the cumulative size of partitions Offload uses RDBMS segment sizes. This is important to note because compressed data in the RDBMS may increase in size when extracted and staged.

Partitions are considered for chunking in the logical order they are defined in the source RDBMS. For example, partitions from an Oracle Database range-partitioned table are split into chunks while maintaining the order of oldest to most recent partition. Offload does not attempt to optimize chunking by shuffling the order of partitions because that would offload partitions out of sequence and be a risk to atomicity.

# Managing Data Distribution

Offload also provides options to manage the offloaded data distribution:

- Backend partitioning: See [Managing Backend Partitioning](managing-backend-partitioning)
- Data distribution: See [Backend Data Distribution and Sorting/Clustering](backend-data-distribution-and-sorting-clustering)

## Managing Backend Partitioning

When offloading data, backend tables can be partitioned in several ways, depending on the backend platform and user preferences:

- [Inherited Partitioning](inherited-partitioning)
- [User-Defined Partitioning](user-defined-partitioning)

### Inherited Partitioning

Tables offloaded with Partition-Based Offload or Subpartition-Based Offload are automatically partitioned in the backend with the (sub)partition key of the source table, unless overridden by the user. The granularity of the inherited (sub)partitions can be different to the source RDBMS (sub)partitions if required. In some cases it is mandatory to specify the granularity of the backend partition scheme (see [Partition Granularity](partition-granularity) below for details)

For Google BigQuery, only the leading (sub)partition key column will be used. In some cases, the backend partitioning might be implemented by a synthetic partition key column, but not always. See [Synthetic Partitioning](synthetic-partitioning) below for details.

### User-Defined Partitioning

Tables can be offloaded with the `--partition-columns` option to define a custom partitioning scheme for the backend table. This enables non-partitioned RDBMS tables to be partitioned in the backend if required, in addition to allowing (sub)partitioned RDBMS tables to be offloaded with a different partitioning scheme in the backend. It is also possible (and in some cases mandatory) to specify the granularity of the backend partitions (see [Partition Granularity](partition-granularity) for details below) and user-defined partitioning supports more RDBMS data types than inherited partitioning.

#### Example 20: Offload with User-Defined Partitioning

```shell
$ $OFFLOAD_HOME/bin/offload -t SH.SALES -x --partition-columns=TIME_ID,PROD_ID --partition-granularity=Y,1000
```

When offloading to Google BigQuery, user-defined partitioning can include date/timestamp, numeric or string columns, but only one partition key column can be defined. In some cases, the backend partitioning will be implemented by a synthetic partition key column, but not always. See [Synthetic Partitioning](synthetic-partitioning) below for details.

### Synthetic Partitioning
Synthetic columns are used to partition backend tables instead of the corresponding natural columns. Depending on the data type and backend system, Offload will sometimes implement inherited and user-defined partitioning schemes with additional synthetic columns (one per source (sub)partition column). This is usually when:

- The backend system has no native partitioning support for the data type of the partition key column
- The backend system has no native partitioning support for the change in granularity requested for the offloaded partitions

Synthetic partition keys are used internally by Offload to ensure that the backend partition columns remain consistent across multiple Partition-Based Offload or Subpartition-Based Offload operations.

Synthetic partition key columns are named by Gluent Offload Engine as a derivative of the corresponding source column name (e.g. `GL_PART_M_TIME_ID` or `GL_PART_U0_SOURCE_CODE`).

When a table is offloaded with partitioning to Google BigQuery, a synthetic partition key will only be generated when the natural partition column is of a `NUMERIC`, `BIGNUMERIC` or `STRING` BigQuery data type, and the resulting synthetic partition key column will be created as an INT64 type (the integral magnitude of the source numeric data must not exceed the `INT64` limits). For `STRING` columns, or for extreme `[BIG]NUMERIC` data that cannot be reduced to INT64 values with `--partition-granularity` (i.e. the granularity itself would need to exceed `INT64` limits), a custom user-defined function (UDF) must be created and used to enable Gluent Offload Engine to create an INT64 synthetic partition key representation of the source data (see [Partition Functions](partition-functions)). Native BigQuery partitioning will be used when the natural partition column has a data type of `INT64`, `DATE`, `DATETIME` or `TIMESTAMP`.

Offload populates synthetic partition key columns with generated data when offloading based on the type and granularity of the data or based on the type and a custom partition function.





