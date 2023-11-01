SPARK_TARGET_DIR=../target/transport
SPARK_VER=3.2.4
SPARK_REL_DIR=spark-$(SPARK_VER)-bin-hadoop3.2
SPARK_TARBALL=$(SPARK_REL_DIR).tgz
#SPARK_AVRO_VER=2.12
#SPARK_AVRO_JAR=$(SPARK_TARGET_DIR)/spark/jars/spark-avro_$(SPARK_AVRO_VER)-$(SPARK_VER).jar

.PHONY: spark-target spark-source spark-config

spark-target: spark-config

spark-config: spark-source
	cp ../templates/spark/spark-standalone-defaults.conf $(SPARK_TARGET_DIR)/spark/conf/spark-defaults.conf
	cp ../templates/spark/spark-standalone-hive-site.xml $(SPARK_TARGET_DIR)/spark/conf/hive-site.xml
	cp ../templates/spark/spark-standalone-spark-env.sh $(SPARK_TARGET_DIR)/spark/conf/spark-env.sh
	mkdir -p $(SPARK_TARGET_DIR)/spark-events $(SPARK_TARGET_DIR)/derby $(SPARK_TARGET_DIR)/warehouse $(SPARK_TARGET_DIR)/spark/run
	echo "export SPARK_PID_DIR=\$$SPARK_HOME/run" >> $(SPARK_TARGET_DIR)/spark/conf/spark-env.sh
	chmod 600 $(SPARK_TARGET_DIR)/spark/conf/spark-defaults.conf

spark-source: $(SPARK_TARGET_DIR)/$(SPARK_TARBALL)
	rm -rf $(SPARK_TARGET_DIR)/spark-?.?.?-bin-hadoop*/
	tar xf $(SPARK_TARGET_DIR)/$(SPARK_TARBALL) --directory=$(SPARK_TARGET_DIR)
	cd $(SPARK_TARGET_DIR) && ln -f -s $(SPARK_REL_DIR) spark

#spark-jars:
#	cp ../spark-basic-auth/build/libs/spark-basic-auth-1.0-SNAPSHOT.jar $(SPARK_TARGET_DIR)/spark/jars/spark-basic-auth.jar

$(SPARK_TARGET_DIR)/$(SPARK_TARBALL): $(SPARK_TARGET_DIR)
	test -f $(SPARK_TARGET_DIR)/$(SPARK_TARBALL) || wget --directory-prefix $(SPARK_TARGET_DIR)/ https://archive.apache.org/dist/spark/spark-$(SPARK_VER)/$(SPARK_TARBALL)

$(SPARK_TARGET_DIR):
	test -d $(SPARK_TARGET_DIR) || mkdir $(SPARK_TARGET_DIR)

clean:
	rm -fr $(SPARK_TARGET_DIR)/*

