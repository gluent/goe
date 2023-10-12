.PHONY: cx_Oracle lint license-placement

TARGET_DIR=../target/offload
INTEGRATION_DIR=../target/integration
THIRDPARTY_DIR=../thirdparty

ifneq ($(filter '$(MAKECMDGOALS)', 'install' 'test' 'lint' 'clean'),)
  $(info "Target Directory not supplied to make. Using default: $(TARGET_DIR)")
else
  TARGET_DIR=$(filter-out $@,$(MAKECMDGOALS))
  $(info "Install Directory: $(TARGET_DIR)")
endif

LIB_TARGET_DIR=$(TARGET_DIR)/lib/python3.8/site-packages
PYTHONPATH=$(TARGET_DIR)/bin
PYTHON=$(TARGET_DIR)/bin/python3
INTEGRATION_PYTHON=$(INTEGRATION_DIR)/bin/python3
EASY_INSTALL=$(LIB_TARGET_DIR)/easy_install.py
PIP=$(PYTHON) -m pip
PYLINT_COMMAND=$(INTEGRATION_PYTHON) -m pylint --init-hook='import sys; sys.path.append("$(LIB_TARGET_DIR)")' -E
LICENSE_YEAR=$(shell date +"%Y")
LICENSE_TEXT=Copyright 2015-$(LICENSE_YEAR) Gluent Inc. All rights reserved.
BUILD_SOURCE=build/src

test:
	echo "##teamcity[testSuiteStarted name='gluentlib']"
	$(PYTHON) -m pytest tests
	echo "##teamcity[testSuiteFinished name='gluentlib']"

install: license-placement
	LD_LIBRARY_PATH=$(TARGET_DIR)/lib $(PYTHON) setup.py bdist_egg --exclude-source-files
	LD_LIBRARY_PATH=$(TARGET_DIR)/lib $(PYTHON) $(EASY_INSTALL) --no-deps --exclude-scripts -d $(LIB_TARGET_DIR) dist/*egg
	mv $(LIB_TARGET_DIR)/easy-install.pth $(LIB_TARGET_DIR)/easy-install.pth.orig
	cat $(LIB_TARGET_DIR)/easy-install.pth.orig | grep -v 'import sys' > $(LIB_TARGET_DIR)/easy-install.pth

license-placement: copy-gluentlib-to-src
	find $(BUILD_SOURCE)/gluentlib -type f -not -path '*/\.*' -exec sed -i 's/LICENSE_TEXT/$(LICENSE_TEXT)/g' {} +
	sed -i 's/LICENSE_TEXT/$(LICENSE_TEXT)/g' $(BUILD_SOURCE)/scripts/*

copy-gluentlib-to-src:
	test -d $(BUILD_SOURCE) || mkdir -p $(BUILD_SOURCE)
	rm -fr $(BUILD_SOURCE)/gluentlib $(BUILD_SOURCE)/scripts
	cp -r gluentlib scripts $(BUILD_SOURCE)

clean:
	rm -fr build gluentlib.egg-info dist $(LIB_TARGET_DIR)/gluentlib*.egg

lint:
	$(PYLINT_COMMAND) ./gluentlib/connect
	# pylint reports import-error due to gluent.py not being utf8 encoded, works in operation so ignoring
	$(PYLINT_COMMAND) --disable=import-error ./gluentlib/conductor
	$(PYLINT_COMMAND) ./gluentlib/config
	# pylint can't understand requests.codes.ok so disabling no-member
	$(PYLINT_COMMAND) --disable=no-member ./gluentlib/data_governance
	$(PYLINT_COMMAND) ./gluentlib/filesystem
	$(PYLINT_COMMAND) --disable=import-error ./gluentlib/filesystem/web_hdfs.py
	# pylint can't import gluent.py, the requirement should go away after BigQuery for IU
	$(PYLINT_COMMAND) --disable=import-error,no-value-for-parameter,too-many-function-args ./gluentlib/incremental
	# Several gluentlib/offload modules being ignored until GOE-1495 is complete or because of subsequent commands
	$(PYLINT_COMMAND) --ignore=predicate_offload.py,offload_transport.py,offload_source_data.py,offload_source_table.py,offload_transport_livy_requests.py ./gluentlib/offload/*.py
	# pylint not understanding syntax np.datetime_as_string returns a str if input is str
	$(PYLINT_COMMAND) --disable=no-member ./gluentlib/offload/predicate_offload.py ./gluentlib/offload/offload_source_data.py
	$(PYLINT_COMMAND) --disable=no-method-argument,no-self-argument,too-many-function-args ./gluentlib/offload/offload_source_table.py
	$(PYLINT_COMMAND) --disable=no-member ./gluentlib/offload/offload_transport.py
	$(PYLINT_COMMAND) --disable=import-error ./gluentlib/offload/offload_transport_livy_requests.py
	$(PYLINT_COMMAND) --ignore=bigquery_backend_api.py ./gluentlib/offload/bigquery
	$(PYLINT_COMMAND) --disable=no-member,unexpected-keyword-arg ./gluentlib/offload/bigquery/bigquery_backend_api.py
	$(PYLINT_COMMAND) ./gluentlib/offload/factory
	$(PYLINT_COMMAND) ./gluentlib/offload/hadoop
	$(PYLINT_COMMAND) --ignore=mssql_frontend_api.py ./gluentlib/offload/microsoft
	# Due to Module 'pymssql' has no 'connect' member
	$(PYLINT_COMMAND) --disable=no-member ./gluentlib/offload/microsoft/mssql_frontend_api.py
	$(PYLINT_COMMAND) ./gluentlib/offload/netezza
	$(PYLINT_COMMAND) --ignore=oracle_offload_source_table.py ./gluentlib/offload/oracle
	# cx-Oracle lint error No name 'DatabaseError' in module 'cx_Oracle'
	$(PYLINT_COMMAND) --disable=no-name-in-module ./gluentlib/offload/oracle/oracle_offload_source_table.py
	$(PYLINT_COMMAND) ./gluentlib/offload/snowflake
	$(PYLINT_COMMAND) --disable=bad-super-call ./gluentlib/offload/spark
	$(PYLINT_COMMAND) ./gluentlib/offload/staging
	$(PYLINT_COMMAND) ./gluentlib/persistence
	# gluentlib/schema_sync ignoring import-error because pylint can't import gluent.py
	# Ignoring schema_sync_step.py until GOE-1495 is complete
	$(PYLINT_COMMAND) --disable=import-error --ignore=schema_sync_step.py ./gluentlib/schema_sync
	$(PYLINT_COMMAND) --ignore=consistency_check.py,diagnostics.py ./gluentlib/util
	# gluentlib/util/diagnostics.py ignoring import-error because pylint can't import gluent.py
	$(PYLINT_COMMAND) --disable=invalid-unary-operand-type,import-error ./gluentlib/util/diagnostics.py
	$(PYLINT_COMMAND) ../scripts/diagnose.py ../scripts/schema_sync.py ../scripts/offload_status_report.py
	# pylint is unable to import DatabaseError from cx_Oracle. Also commented because of quasi global variable issue.
	$(PYLINT_COMMAND) --disable=no-name-in-module,undefined-variable ../scripts/connect.py
	# gluent.py has some Objects built using kwargs which prevents correct member checking
	$(PYLINT_COMMAND) --disable=no-member,access-member-before-definition,unsubscriptable-object ../scripts/gluent.py
	# test uses gluent.py Objects built using kwargs which prevents correct member checking.
	# test has an exception, TestFailure, which does not inherit from Exception. I tried changing this and tests stopped
	# working. Hence catching-non-exception below.
	$(PYLINT_COMMAND) --disable=no-member,catching-non-exception,raising-non-exception ../scripts/test
	$(PYLINT_COMMAND) ../scripts/test_stories
	$(PYLINT_COMMAND) tests/conductor
	$(PYLINT_COMMAND) tests/connect
	$(PYLINT_COMMAND) tests/filesystem
	$(PYLINT_COMMAND) tests/offload
	$(PYLINT_COMMAND) --ignore=test_linux_cmd.py tests/util

python-integrity:
	# Ensure Python is correctly built
	$(PYTHON) -c "import ssl"
	$(PYTHON) -c "import ctypes"
