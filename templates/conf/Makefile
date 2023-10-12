.DEFAULT_GOAL := install

.PHONY: clean install

TARGET_DIR = ../../target/offload
TARGET_CONF_DIR = $(TARGET_DIR)/conf

install: $(TARGET_CONF_DIR)/oracle-hadoop-offload.env.template \
         $(TARGET_CONF_DIR)/oracle-bigquery-offload.env.template \
         $(TARGET_CONF_DIR)/oracle-snowflake-offload.env.template \
         $(TARGET_CONF_DIR)/oracle-synapse-offload.env.template \
         $(TARGET_CONF_DIR)/teradata-hadoop-offload.env.template \
         $(TARGET_CONF_DIR)/teradata-bigquery-offload.env.template \
         $(TARGET_CONF_DIR)/teradata-snowflake-offload.env.template \
         $(TARGET_CONF_DIR)/teradata-synapse-offload.env.template

$(TARGET_CONF_DIR)/oracle-hadoop-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.oracle offload.env.template.hadoop offload.env.template.listener
	cat offload.env.template.common offload.env.template.oracle offload.env.template.hadoop offload.env.template.listener \
		> $(TARGET_CONF_DIR)/oracle-hadoop-offload.env.template

$(TARGET_CONF_DIR)/oracle-bigquery-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.oracle offload.env.template.bigquery offload.env.template.listener
	cat offload.env.template.common offload.env.template.oracle offload.env.template.bigquery offload.env.template.listener \
		> $(TARGET_CONF_DIR)/oracle-bigquery-offload.env.template
	sed -i 's/^export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=/export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=spark-submit/' \
			$(TARGET_CONF_DIR)/oracle-bigquery-offload.env.template

$(TARGET_CONF_DIR)/oracle-snowflake-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.oracle offload.env.template.snowflake offload.env.template.listener
	cat offload.env.template.common offload.env.template.oracle offload.env.template.snowflake offload.env.template.listener \
		> $(TARGET_CONF_DIR)/oracle-snowflake-offload.env.template
	sed -i 's/^export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=/export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=spark-submit/' \
			$(TARGET_CONF_DIR)/oracle-snowflake-offload.env.template

$(TARGET_CONF_DIR)/oracle-synapse-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.oracle offload.env.template.synapse offload.env.template.listener
	cat offload.env.template.common offload.env.template.oracle offload.env.template.synapse offload.env.template.listener \
		> $(TARGET_CONF_DIR)/oracle-synapse-offload.env.template

$(TARGET_CONF_DIR)/mssql-hadoop-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.mssql offload.env.template.hadoop offload.env.template.listener
	cat offload.env.template.common offload.env.template.mssql offload.env.template.hadoop offload.env.template.listener \
		> $(TARGET_CONF_DIR)/mssql-hadoop-offload.env.template

$(TARGET_CONF_DIR)/mssql-bigquery-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.mssql offload.env.template.bigquery offload.env.template.listener
	cat offload.env.template.common offload.env.template.mssql offload.env.template.bigquery offload.env.template.listener \
		> $(TARGET_CONF_DIR)/mssql-bigquery-offload.env.template

$(TARGET_CONF_DIR)/mssql-snowflake-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.mssql offload.env.template.snowflake offload.env.template.listener
	cat offload.env.template.common offload.env.template.mssql offload.env.template.snowflake offload.env.template.listener \
		> $(TARGET_CONF_DIR)/mssql-snowflake-offload.env.template

$(TARGET_CONF_DIR)/mssql-synapse-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.mssql offload.env.template.synapse offload.env.template.listener
	cat offload.env.template.common offload.env.template.mssql offload.env.template.synapse offload.env.template.listener \
		> $(TARGET_CONF_DIR)/mssql-synapse-offload.env.template

$(TARGET_CONF_DIR)/netezza-hadoop-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.netezza offload.env.template.hadoop offload.env.template.listener
	cat offload.env.template.common offload.env.template.netezza offload.env.template.hadoop offload.env.template.listener \
		> $(TARGET_CONF_DIR)/netezza-hadoop-offload.env.template

$(TARGET_CONF_DIR)/netezza-bigquery-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.netezza offload.env.template.bigquery offload.env.template.listener
	cat offload.env.template.common offload.env.template.netezza offload.env.template.bigquery offload.env.template.listener \
		> $(TARGET_CONF_DIR)/netezza-bigquery-offload.env.template

$(TARGET_CONF_DIR)/netezza-snowflake-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.netezza offload.env.template.snowflake offload.env.template.listener
	cat offload.env.template.common offload.env.template.netezza offload.env.template.snowflake offload.env.template.listener \
		> $(TARGET_CONF_DIR)/netezza-snowflake-offload.env.template

$(TARGET_CONF_DIR)/netezza-synapse-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.netezza offload.env.template.synapse
	cat offload.env.template.common offload.env.template.netezza offload.env.template.synapse \
		> $(TARGET_CONF_DIR)/netezza-synapse-offload.env.template

$(TARGET_CONF_DIR)/teradata-hadoop-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.teradata offload.env.template.hadoop
	cat offload.env.template.common offload.env.template.teradata offload.env.template.hadoop \
		> $(TARGET_CONF_DIR)/teradata-hadoop-offload.env.template

$(TARGET_CONF_DIR)/teradata-bigquery-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.teradata offload.env.template.bigquery
	cat offload.env.template.common offload.env.template.teradata offload.env.template.bigquery \
		> $(TARGET_CONF_DIR)/teradata-bigquery-offload.env.template
	sed -i 's/^export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=/export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=spark-submit/' \
			$(TARGET_CONF_DIR)/teradata-bigquery-offload.env.template

$(TARGET_CONF_DIR)/teradata-snowflake-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.teradata offload.env.template.snowflake
	cat offload.env.template.common offload.env.template.teradata offload.env.template.snowflake \
		> $(TARGET_CONF_DIR)/teradata-snowflake-offload.env.template
	sed -i 's/^export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=/export OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE=spark-submit/' \
			$(TARGET_CONF_DIR)/teradata-snowflake-offload.env.template

$(TARGET_CONF_DIR)/teradata-synapse-offload.env.template: $(TARGET_CONF_DIR) offload.env.template.common offload.env.template.teradata offload.env.template.synapse
	cat offload.env.template.common offload.env.template.teradata offload.env.template.synapse \
		> $(TARGET_CONF_DIR)/teradata-synapse-offload.env.template

$(TARGET_CONF_DIR):
	mkdir -p $(TARGET_CONF_DIR)

clean:
	rm -f $(TARGET_CONF_DIR)/oracle-hadoop-offload.env.template \
				$(TARGET_CONF_DIR)/oracle-bigquery-offload.env.template \
				$(TARGET_CONF_DIR)/oracle-snowflake-offload.env.template \
				$(TARGET_CONF_DIR)/oracle-synapse-offload.env.template \
				$(TARGET_CONF_DIR)/teradata-hadoop-offload.env.template \
				$(TARGET_CONF_DIR)/teradata-bigquery-offload.env.template \
				$(TARGET_CONF_DIR)/teradata-snowflake-offload.env.template \
				$(TARGET_CONF_DIR)/teradata-synapse-offload.env.template
