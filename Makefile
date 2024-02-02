
TARGET_DIR=target/offload


OFFLOAD_VERSION=$(shell cat version)
LICENSE_YEAR=$(shell date +"%Y")
LICENSE_TEXT=Copyright 2015-$(LICENSE_YEAR) Gluent Inc. All rights reserved.

BUILD=$(strip $(shell git rev-parse --short HEAD))

VENV_EXISTS=$(shell python3 -c "if __import__('pathlib').Path('.venv/bin/activate').exists(): print('yes')")
VENV_PREFIX=.venv


.PHONY: package
package: target
	cd target && make package


.PHONY: install
install: offload-home-check package
	# Install package into a target OFFLOAD_HOME, usually for local testing.
	@echo -e "=> \e[92m Installing to directory: $(OFFLOAD_HOME)...\e[0m"
	test -f goe_$(OFFLOAD_VERSION).tar.gz
	# Remove everything but the conf directory
	rm -fr $(OFFLOAD_HOME)/[blrstvL]* $(OFFLOAD_HOME)/cache
	tar --directory=${OFFLOAD_HOME}/../ -xf goe_$(OFFLOAD_VERSION).tar.gz


.PHONY: install-dev
install-dev:
	# Recreate virtual environment and install requirements for development.
	@if [ "$(VENV_EXISTS)" ]; then echo "=> Removing existing virtual environment"; fi
	if [ "$(VENV_EXISTS)" ]; then $(MAKE) python-goe-destroy; fi
	if [ "$(VENV_EXISTS)" ]; then $(MAKE) python-goe-clean; fi
	python3 -m venv $(VENV_PREFIX)
	. $(VENV_PREFIX)/bin/activate && python3 -m pip install --quiet --upgrade pip build setuptools wheel
	. $(VENV_PREFIX)/bin/activate && python3 -m pip install .[dev]
	$(MAKE) python-goe


.PHONY: install-dev-extras
install-dev-extras:
	. $(VENV_PREFIX)/bin/activate && python3 -m pip install .[hadoop,snowflake,sql_server,teradata]


.PHONY: target
target: python-goe spark-listener license-txt offload-env
	@echo -e "=> \e[92m Building target: $(TARGET_DIR)...\e[0m"
	mkdir -p $(TARGET_DIR)/bin
	cp bin/{offload,connect,logmgr,display_goe_env,clean_goe_env,listener,agg_validate} $(TARGET_DIR)/bin
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/connect
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/offload
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/logmgr
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/display_goe_env
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/clean_goe_env
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/listener
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/agg_validate
	mkdir -p $(TARGET_DIR)/tools
	cp tools/goe-shell-functions.sh $(TARGET_DIR)/tools
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/tools/goe-shell-functions.sh
	mkdir -p $(TARGET_DIR)/tools/listener
	cp tools/goe-listener{.sh,.service} $(TARGET_DIR)/tools/listener
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/tools/listener/goe-listener.sh
	chmod 0755 $(TARGET_DIR)/tools/listener/goe-listener.sh
	chmod 0755 $(TARGET_DIR)/bin/listener
	rm -rf $(TARGET_DIR)/setup/sql $(TARGET_DIR)/setup/python
	mkdir -p $(TARGET_DIR)/cache
	mkdir -p $(TARGET_DIR)/setup/sql && cp -a sql/oracle/source/* $(TARGET_DIR)/setup
	mkdir -p $(TARGET_DIR)/lib && cp dist/goe-*.whl $(TARGET_DIR)/lib
	cp version $(TARGET_DIR)
	cp LICENSE.txt $(TARGET_DIR)
	echo "$(OFFLOAD_VERSION) ($(BUILD))" > $(TARGET_DIR)/version_build
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/setup/*.sql $(TARGET_DIR)/setup/sql/*.sql
	sed -i -e "s/VERSION/$(OFFLOAD_VERSION)/" -e "s/BUILD/$(BUILD)/" $(TARGET_DIR)/setup/sql/{install,upgrade}_env.sql
	sed -i "s/'%s-SNAPSHOT'/'$(OFFLOAD_VERSION) ($(BUILD))'/" $(TARGET_DIR)/setup/sql/create_offload*_package_spec.sql
	mkdir -p $(TARGET_DIR)/templates
	cp -r templates/goe_base.html templates/offload_status_report $(TARGET_DIR)/templates/
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/templates/goe_base.html
	mkdir -p $(TARGET_DIR)/run $(TARGET_DIR)/log
	chmod 775 $(TARGET_DIR)/run $(TARGET_DIR)/log
	chmod 640 $(TARGET_DIR)/conf/*offload.env.template

.PHONY: spark-basic-auth
spark-basic-auth:
	cd spark-basic-auth && make

.PHONY: spark-listener
spark-listener:
	cd tools/spark-listener && make target

#package-spark-standalone: spark-basic-auth spark-listener license-txt
.PHONY: package-spark-standalone
package-spark-standalone: spark-listener
	cd tools/transport && make spark-target
	cd target && make package-spark


.PHONY: python-goe
python-goe: python-goe-clean
	sed -i "s/^version = .*/\Lversion = \"$(OFFLOAD_VERSION)\"/" pyproject.toml
	. $(VENV_PREFIX)/bin/activate && python3 -m build


.PHONY: offload-env
offload-env:
	cd templates/conf && make


.PHONY: license-txt
license-txt:
	echo "$(LICENSE_TEXT)" > LICENSE.txt


.PHONY: offload-home-check
offload-home-check:
	@[ $(OFFLOAD_HOME) ] || echo OFFLOAD_HOME is not set
	@[ $(OFFLOAD_HOME) ]


### CLEANUP ###
.PHONY: clean
clean:
	cd templates/conf && make clean
	cd tools/spark-listener && make clean
	cd target && make clean
	rm -f goe_[0-9]*.[0-9]*.*.tar.gz


.PHONY: python-goe-destroy
python-goe-destroy:
	@rm -rf .venv


.PHONY: python-goe-clean
python-goe-clean:
	@rm -rf .pytest_cache build/ dist/ .eggs/
	@find . -name '*.egg-info' -exec rm -rf {} +
	@find . -name '*.egg' -exec rm -f {} +
	@find . -name '*.pyc' -exec rm -f {} +
	@find . -name '*.pyo' -exec rm -f {} +
	@find . -name '__pycache__' -exec rm -rf {} +
