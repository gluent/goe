JAR_FOR_SPARK_330=spark-3.3.0-listener_2.12-1.0.jar
JAR_FOR_SPARK_320=spark-3.2.0-listener_2.12-1.0.jar
JAR_FOR_SPARK_312=spark-3.1.2-listener_2.12-1.0.jar
JAR_FOR_SPARK_301=spark-3.0.1-listener_2.12-1.0.jar
TARGET_JAR_FOR_SPARK_330=target/scala-2.12/$(JAR_FOR_SPARK_330)
TARGET_JAR_FOR_SPARK_320=target/scala-2.12/$(JAR_FOR_SPARK_320)
TARGET_JAR_FOR_SPARK_312=target/scala-2.12/$(JAR_FOR_SPARK_312)
TARGET_JAR_FOR_SPARK_301=target/scala-2.12/$(JAR_FOR_SPARK_301)
INSTALL_DIR=../../target/offload/lib
INSTALL_JAR=$(INSTALL_DIR)/goe-spark-listener.jar

all: spark-listener

.PHONY: spark-listener
spark-listener: $(TARGET_JAR_FOR_SPARK_330) $(TARGET_JAR_FOR_SPARK_320) $(TARGET_JAR_FOR_SPARK_312) $(TARGET_JAR_FOR_SPARK_301)

$(TARGET_JAR_FOR_SPARK_330):
	sbt -DsparkVersion=3.3.0 package

$(TARGET_JAR_FOR_SPARK_320):
	sbt -DsparkVersion=3.2.0 package

$(TARGET_JAR_FOR_SPARK_312):
	sbt -DsparkVersion=3.1.2 package

$(TARGET_JAR_FOR_SPARK_301):
	sbt -DsparkVersion=3.0.1 package

target: $(INSTALL_DIR) spark-listener
	cp $(TARGET_JAR_FOR_SPARK_330) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_330)
	cp $(TARGET_JAR_FOR_SPARK_320) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_320)
	cp $(TARGET_JAR_FOR_SPARK_312) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_312)
	cp $(TARGET_JAR_FOR_SPARK_301) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_301)
	# Default to Spark 3.2.0 compatible Spark Listener because that is the current bundled version.
	cp $(TARGET_JAR_FOR_SPARK_320) $(INSTALL_JAR)

$(INSTALL_DIR):
	mkdir -p $(INSTALL_DIR)

.PHONY: clean
clean:
	sbt clean
	rm -f $(TARGET_JAR_FOR_SPARK_330) $(TARGET_JAR_FOR_SPARK_320) $(TARGET_JAR_FOR_SPARK_312) $(TARGET_JAR_FOR_SPARK_301)
	rm -f $(INSTALL_JAR) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_330) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_320) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_312) $(INSTALL_DIR)/goe-$(JAR_FOR_SPARK_301)
