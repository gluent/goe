OFFLOAD_VERSION=$(shell cat ../version)
OS_VERSION=$(shell lsb_release -r | grep -oP "(?<=\s)[5-8](?=\.)")
PACKAGE_OS_ID=el$(OS_VERSION)
INTEGRATION_PACKAGE=gluent_integration_$(OFFLOAD_VERSION)_$(PACKAGE_OS_ID).tar
TARGET_DIR=target/integration
PYTHON_VERSION=3.8

.PHONY: clean target package clean-integration-python integration-python

clean:
	rm -rf $(TARGET_DIR)
	rm -f ../$(INTEGRATION_PACKAGE).bz2

target: integration-python
	mkdir -p $(TARGET_DIR)/bin
	mkdir -p $(TARGET_DIR)/sql/oracle/environments
	mkdir -p $(TARGET_DIR)/sql/oracle/tests
	mkdir -p $(TARGET_DIR)/sql/teradata/environments
	mkdir -p $(TARGET_DIR)/sql/teradata/tests
	mkdir -p $(TARGET_DIR)/config
	# This directory structure is for backwards compatibility when Oracle was the only frontend
	mkdir -p $(TARGET_DIR)/sql/environments/test
	cp {test,test_results,test_root_causes.yaml} $(TARGET_DIR)/bin
	cp -r test_sets/ $(TARGET_DIR)/bin
	cp ../gluentlib/scripts/{exec_in_hadoop_db,exec_in_hdfs} $(TARGET_DIR)/bin
	cp -r ../gluentlib/tests $(TARGET_DIR)/bin
	cp -r testlib $(TARGET_DIR)/bin
	#cp ../connector/build-release/unit_tests $(TARGET_DIR)/bin
	#cp -r ../connector/test_profiles $(TARGET_DIR)/bin
	cp {step_setup.sh,test_functions.sh,color_definitions.sh,geckodriver*,infra_check.sh,cleanup_cloud_sync_tests.sh,clean_old_tc_schemas.py,restart_llap,start_hdp,emr_init.sh,emr_terminate.sh,generate_sql_disco.sh,generate_table_disco.sh,cdp_cli.py,prepare_manual_test.sh,test_setup,test_runner} $(TARGET_DIR)/bin
	cp -r config/tests $(TARGET_DIR)/config
	cp -r ste $(TARGET_DIR)/sql/oracle/tests
	cp -r ../sql/oracle/source $(TARGET_DIR)/sql/oracle
	cp -r environments/oracle/test $(TARGET_DIR)/sql/oracle/environments
	cp -r environments/oracle/ste $(TARGET_DIR)/sql/oracle/environments
	cp -r environments/teradata/test $(TARGET_DIR)/sql/teradata/environments
	cp environments/build_test_schemas.sh $(TARGET_DIR)/sql/environments/test

clean-integration-python:
	rm -fr target/integration/bin/py*
	rm -fr target/integration/lib/py*

integration-python: clean-integration-python
	# Integration Python
	test -d target/integration/bin || mkdir -p target/integration/bin
	test -f target/integration/bin/python$(PYTHON_VERSION) || cp ../thirdparty/target/integration/bin/python$(PYTHON_VERSION) target/integration/bin/
	test -f target/integration/bin/python3 || cp -P ../thirdparty/target/integration/bin/*3* target/integration/bin/
	test -f target/integration/bin/pylint || cp -P ../thirdparty/target/integration/bin/pylint target/integration/bin/
	test -d target/integration/lib || mkdir -p target/integration/lib
	cp -rP ../thirdparty/target/integration/lib/python$(PYTHON_VERSION)/ target/integration/lib/

package: target
	tar -C $(TARGET_DIR) --exclude "lib/python$(PYTHON_VERSION)/site-packages/easy_install.*" --exclude "lib/python$(PYTHON_VERSION)/site-packages/pip*" --exclude "lib/python$(PYTHON_VERSION)/site-packages/pkg_resources" --exclude "lib/python$(PYTHON_VERSION)/site-packages/wheel*" --exclude "lib/python$(PYTHON_VERSION)/site-packages/setuptools*" --exclude="bin/python3*" -cjf ../$(INTEGRATION_PACKAGE).bz2 bin sql config lib/python$(PYTHON_VERSION)/site-packages
