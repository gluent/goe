# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SPARK_TARGET_DIR=../../target/transport
SPARK_VER=3.2.4
SPARK_HADOOP_VER=hadoop3.2
SPARK_REL_DIR=spark-$(SPARK_VER)-bin-$(SPARK_HADOOP_VER)
SPARK_TARBALL=$(SPARK_REL_DIR).tgz
JAR_DIR=$(SPARK_TARGET_DIR)/$(SPARK_REL_DIR)/jars
AVRO_VER=2.12
AVRO_JAR=spark-avro_$(AVRO_VER)-$(SPARK_VER).jar
GCS_HADOOP_VER=hadoop3-2
GCS_JAR=gcs-connector-$(GCS_HADOOP_VER).2.11-shaded.jar

.PHONY: spark-target spark-source spark-config

spark-target: spark-source spark-config spark-jars

spark-config: spark-source
	cp ../../templates/spark/spark-standalone-defaults.conf $(SPARK_TARGET_DIR)/spark/conf/spark-defaults.conf
	cp ../../templates/spark/spark-standalone-hive-site.xml $(SPARK_TARGET_DIR)/spark/conf/hive-site.xml
	cp ../../templates/spark/spark-standalone-spark-env.sh $(SPARK_TARGET_DIR)/spark/conf/spark-env.sh
	mkdir -p $(SPARK_TARGET_DIR)/spark-events $(SPARK_TARGET_DIR)/derby $(SPARK_TARGET_DIR)/warehouse $(SPARK_TARGET_DIR)/spark/run
	echo "export SPARK_PID_DIR=\$$SPARK_HOME/run" >> $(SPARK_TARGET_DIR)/spark/conf/spark-env.sh
	chmod 600 $(SPARK_TARGET_DIR)/spark/conf/spark-defaults.conf

spark-source: $(SPARK_TARGET_DIR)/$(SPARK_TARBALL)
	rm -rf $(SPARK_TARGET_DIR)/spark-?.?.?-bin-hadoop*/
	tar xf $(SPARK_TARGET_DIR)/$(SPARK_TARBALL) --directory=$(SPARK_TARGET_DIR)
	cd $(SPARK_TARGET_DIR) && ln -f -s $(SPARK_REL_DIR) spark

spark-jars: $(JAR_DIR)/$(GCS_JAR) $(JAR_DIR)/$(AVRO_JAR)
#	cp ../spark-basic-auth/build/libs/spark-basic-auth-1.0-SNAPSHOT.jar $(SPARK_TARGET_DIR)/spark/jars/spark-basic-auth.jar

$(SPARK_TARGET_DIR)/$(SPARK_TARBALL): $(SPARK_TARGET_DIR)
	test -f $(SPARK_TARGET_DIR)/$(SPARK_TARBALL) || wget --directory-prefix $(SPARK_TARGET_DIR)/ https://archive.apache.org/dist/spark/spark-$(SPARK_VER)/$(SPARK_TARBALL)

$(JAR_DIR)/$(AVRO_JAR): spark-source
	test -f $(JAR_DIR)/$(AVRO_JAR) || wget --directory-prefix $(JAR_DIR)/ https://repo1.maven.org/maven2/org/apache/spark/spark-avro_$(AVRO_VER)/$(SPARK_VER)/$(AVRO_JAR)

$(JAR_DIR)/$(GCS_JAR): spark-source
	test -f $(JAR_DIR)/$(GCS_JAR) || wget --directory-prefix $(JAR_DIR)/ https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/$(GCS_HADOOP_VER).2.11/$(GCS_JAR)

$(SPARK_TARGET_DIR):
	test -d $(SPARK_TARGET_DIR) || mkdir $(SPARK_TARGET_DIR)

clean:
	rm -fr $(SPARK_TARGET_DIR)/*

