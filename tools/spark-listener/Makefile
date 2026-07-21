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

JAR_FOR_SPARK_301=spark-3.0.1-listener_2.12-1.0.jar
JAR_FOR_SPARK_312=spark-3.1.2-listener_2.12-1.0.jar
JAR_FOR_SPARK_320=spark-3.2.0-listener_2.12-1.0.jar
JAR_FOR_SPARK_330=spark-3.3.0-listener_2.12-1.0.jar
JAR_FOR_SPARK_351_212=spark-3.5.1-listener_2.12-1.0.jar
JAR_FOR_SPARK_351_213=spark-3.5.1-listener_2.13-1.0.jar
JAR_FOR_SPARK_353_213=spark-3.5.3-listener_2.13-1.0.jar
TARGET_JAR_FOR_SPARK_301=target/scala-2.12/$(JAR_FOR_SPARK_301)
TARGET_JAR_FOR_SPARK_312=target/scala-2.12/$(JAR_FOR_SPARK_312)
TARGET_JAR_FOR_SPARK_320=target/scala-2.12/$(JAR_FOR_SPARK_320)
TARGET_JAR_FOR_SPARK_330=target/scala-2.12/$(JAR_FOR_SPARK_330)
TARGET_JAR_FOR_SPARK_351_212=target/scala-2.12/$(JAR_FOR_SPARK_351_212)
TARGET_JAR_FOR_SPARK_351_213=target/scala-2.13/$(JAR_FOR_SPARK_351_213)
TARGET_JAR_FOR_SPARK_353_213=target/scala-2.13/$(JAR_FOR_SPARK_353_213)
INSTALL_DIR=../../target/offload/lib
INSTALL_JAR=$(INSTALL_DIR)/goe-spark-listener.jar

all: spark-listener

.PHONY: spark-listener
spark-listener: $(TARGET_JAR_FOR_SPARK_330) $(TARGET_JAR_FOR_SPARK_320) $(TARGET_JAR_FOR_SPARK_312) $(TARGET_JAR_FOR_SPARK_301) $(TARGET_JAR_FOR_SPARK_351_212) $(TARGET_JAR_FOR_SPARK_351_213) $(TARGET_JAR_FOR_SPARK_353_213)

$(TARGET_JAR_FOR_SPARK_301):
	sbt -DsparkVersion=3.0.1 "reload; package"

$(TARGET_JAR_FOR_SPARK_312):
	sbt -DsparkVersion=3.1.2 "reload; package"

$(TARGET_JAR_FOR_SPARK_320):
	sbt -DsparkVersion=3.2.0 "reload; package"

$(TARGET_JAR_FOR_SPARK_330):
	sbt -DsparkVersion=3.3.0 "reload; package"

$(TARGET_JAR_FOR_SPARK_351_212):
	sbt -DsparkVersion=3.5.1 -DscalaVersion=2.12.18 "reload; package"

$(TARGET_JAR_FOR_SPARK_351_213):
	sbt -DsparkVersion=3.5.1 -DscalaVersion=2.13.11 "reload; package"

$(TARGET_JAR_FOR_SPARK_353_213):
	sbt -DsparkVersion=3.5.3 -DscalaVersion=2.13.11 "reload; package"

target: $(INSTALL_DIR) spark-listener
	cp $(TARGET_JAR_FOR_SPARK_301) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_301)
	cp $(TARGET_JAR_FOR_SPARK_312) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_312)
	cp $(TARGET_JAR_FOR_SPARK_320) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_320)
	cp $(TARGET_JAR_FOR_SPARK_330) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_330)
	cp $(TARGET_JAR_FOR_SPARK_351_212) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_351_212)
	cp $(TARGET_JAR_FOR_SPARK_351_213) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_351_213)
	cp $(TARGET_JAR_FOR_SPARK_353_213) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_353_213)
	# Default to Spark 3.5.1 2.12 compatible Spark Listener because that is the
	# current Google Managed Spark serverless default.
	cp $(TARGET_JAR_FOR_SPARK_351_212) $(INSTALL_JAR)

$(INSTALL_DIR):
	mkdir -p $(INSTALL_DIR)

.PHONY: clean
clean:
	sbt clean
	rm -f $(TARGET_JAR_FOR_SPARK_330) $(TARGET_JAR_FOR_SPARK_320) $(TARGET_JAR_FOR_SPARK_312) $(TARGET_JAR_FOR_SPARK_301) $(TARGET_JAR_FOR_SPARK_351_212) $(TARGET_JAR_FOR_SPARK_351_213) $(TARGET_JAR_FOR_SPARK_353_213)
	rm -f $(INSTALL_JAR) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_330) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_320) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_312) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_301) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_351_212) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_351_213) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_353_213)
