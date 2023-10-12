.DEFAULT_GOAL := install

.PHONY: clean install

TARGET_DIR = ../../target/offload

install: $(TARGET_DIR)/conf/oracle-hadoop-offload.env.template \
         $(TARGET_DIR)/conf/oracle-bigquery-offload.env.template \
         $(TARGET_DIR)/conf/oracle-snowflake-offload.env.template \
         $(TARGET_DIR)/conf/oracle-synapse-offload.env.template \
         $(TARGET_DIR)/conf/teradata-hadoop-offload.env.template \
         $(TARGET_DIR)/conf/teradata-bigquery-offload.env.template \
         $(TARGET_DIR)/conf/teradata-snowflake-offload.env.template \
         $(TARGET_DIR)/conf/teradata-synapse-offload.env.template

$(TARGET_DIR)/conf/oracle-hadoop-offload.env.template: offload.env.template.common offload.env.template.oracle offload.env.template.hadoop offload.env.template.listener
	cat offload.env.template.common offload.env.template.oracle offload.env.template.hadoop offload.env.template.listener \
		> $(TARGET_DIR)/conf/oracle-hadoop-offload.env.template

$(TARGET_DIR)/conf/oracle-bigquery-offload.env.template: offload.env.template.common offload.env.template.oracle offload.env.template.bigquery offload.env.template.listener
	cat offload.env.template.common offload.env.template.oracle offload.env.template.bigquery offload.env.template.listener \
		> $(TARGET_DIR)/conf/oracle-bigquery-offload.env.template
	sed -i 's/^export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=/export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=spark-submit/' \
			$(TARGET_DIR)/conf/oracle-bigquery-offload.env.template

$(TARGET_DIR)/conf/oracle-snowflake-offload.env.template: offload.env.template.common offload.env.template.oracle offload.env.template.snowflake offload.env.template.listener
	cat offload.env.template.common offload.env.template.oracle offload.env.template.snowflake offload.env.template.listener \
		> $(TARGET_DIR)/conf/oracle-snowflake-offload.env.template
	sed -i 's/^export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=/export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=spark-submit/' \
			$(TARGET_DIR)/conf/oracle-snowflake-offload.env.template

$(TARGET_DIR)/conf/oracle-synapse-offload.env.template: offload.env.template.common offload.env.template.oracle offload.env.template.synapse offload.env.template.listener
	cat offload.env.template.common offload.env.template.oracle offload.env.template.synapse offload.env.template.listener \
		> $(TARGET_DIR)/conf/oracle-synapse-offload.env.template

$(TARGET_DIR)/conf/mssql-hadoop-offload.env.template: offload.env.template.common offload.env.template.mssql offload.env.template.hadoop offload.env.template.listener
	cat offload.env.template.common offload.env.template.mssql offload.env.template.hadoop offload.env.template.listener \
		> $(TARGET_DIR)/conf/mssql-hadoop-offload.env.template

$(TARGET_DIR)/conf/mssql-bigquery-offload.env.template: offload.env.template.common offload.env.template.mssql offload.env.template.bigquery offload.env.template.listener
	cat offload.env.template.common offload.env.template.mssql offload.env.template.bigquery offload.env.template.listener \
		> $(TARGET_DIR)/conf/mssql-bigquery-offload.env.template

$(TARGET_DIR)/conf/mssql-snowflake-offload.env.template: offload.env.template.common offload.env.template.mssql offload.env.template.snowflake offload.env.template.listener
	cat offload.env.template.common offload.env.template.mssql offload.env.template.snowflake offload.env.template.listener \
		> $(TARGET_DIR)/conf/mssql-snowflake-offload.env.template

$(TARGET_DIR)/conf/mssql-synapse-offload.env.template: offload.env.template.common offload.env.template.mssql offload.env.template.synapse offload.env.template.listener
	cat offload.env.template.common offload.env.template.mssql offload.env.template.synapse offload.env.template.listener \
		> $(TARGET_DIR)/conf/mssql-synapse-offload.env.template

$(TARGET_DIR)/conf/netezza-hadoop-offload.env.template: offload.env.template.common offload.env.template.netezza offload.env.template.hadoop offload.env.template.listener
	cat offload.env.template.common offload.env.template.netezza offload.env.template.hadoop offload.env.template.listener \
		> $(TARGET_DIR)/conf/netezza-hadoop-offload.env.template

$(TARGET_DIR)/conf/netezza-bigquery-offload.env.template: offload.env.template.common offload.env.template.netezza offload.env.template.bigquery offload.env.template.listener
	cat offload.env.template.common offload.env.template.netezza offload.env.template.bigquery offload.env.template.listener \
		> $(TARGET_DIR)/conf/netezza-bigquery-offload.env.template

$(TARGET_DIR)/conf/netezza-snowflake-offload.env.template: offload.env.template.common offload.env.template.netezza offload.env.template.snowflake offload.env.template.listener
	cat offload.env.template.common offload.env.template.netezza offload.env.template.snowflake offload.env.template.listener \
		> $(TARGET_DIR)/conf/netezza-snowflake-offload.env.template

$(TARGET_DIR)/conf/netezza-synapse-offload.env.template: offload.env.template.common offload.env.template.netezza offload.env.template.synapse
	cat offload.env.template.common offload.env.template.netezza offload.env.template.synapse \
		> $(TARGET_DIR)/conf/netezza-synapse-offload.env.template

$(TARGET_DIR)/conf/teradata-hadoop-offload.env.template: offload.env.template.common offload.env.template.teradata offload.env.template.hadoop
	cat offload.env.template.common offload.env.template.teradata offload.env.template.hadoop \
		> $(TARGET_DIR)/conf/teradata-hadoop-offload.env.template

$(TARGET_DIR)/conf/teradata-bigquery-offload.env.template: offload.env.template.common offload.env.template.teradata offload.env.template.bigquery
	cat offload.env.template.common offload.env.template.teradata offload.env.template.bigquery \
		> $(TARGET_DIR)/conf/teradata-bigquery-offload.env.template
	sed -i 's/^export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=/export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=spark-submit/' \
			$(TARGET_DIR)/conf/teradata-bigquery-offload.env.template

$(TARGET_DIR)/conf/teradata-snowflake-offload.env.template: offload.env.template.common offload.env.template.teradata offload.env.template.snowflake
	cat offload.env.template.common offload.env.template.teradata offload.env.template.snowflake \
		> $(TARGET_DIR)/conf/teradata-snowflake-offload.env.template
	sed -i 's/^export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=/export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=spark-submit/' \
			$(TARGET_DIR)/conf/teradata-snowflake-offload.env.template

$(TARGET_DIR)/conf/teradata-synapse-offload.env.template: offload.env.template.common offload.env.template.teradata offload.env.template.synapse
	cat offload.env.template.common offload.env.template.teradata offload.env.template.synapse \
		> $(TARGET_DIR)/conf/teradata-synapse-offload.env.template

clean:
	rm -f $(TARGET_DIR)/conf/oracle-hadoop-offload.env.template \
				$(TARGET_DIR)/conf/oracle-bigquery-offload.env.template \
				$(TARGET_DIR)/conf/oracle-snowflake-offload.env.template \
				$(TARGET_DIR)/conf/oracle-synapse-offload.env.template \
				$(TARGET_DIR)/conf/teradata-hadoop-offload.env.template \
				$(TARGET_DIR)/conf/teradata-bigquery-offload.env.template \
				$(TARGET_DIR)/conf/teradata-snowflake-offload.env.template \
				$(TARGET_DIR)/conf/teradata-synapse-offload.env.template
