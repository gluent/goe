# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SHELL := /bin/bash

TARGET_DIR=target/offload

OFFLOAD_VERSION=$(shell cat version)
GOE_WHEEL="goe-$(shell echo $(OFFLOAD_VERSION) | tr 'A-Z-' 'a-z.')0-py3-none-any.whl"

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
	rm -fr $(OFFLOAD_HOME)/[blrstvL]* $(OFFLOAD_HOME)/cache $(OFFLOAD_HOME)/.venv
	tar --directory=${OFFLOAD_HOME}/../ -xf goe_$(OFFLOAD_VERSION).tar.gz
	python3 -m venv $(OFFLOAD_HOME)/$(VENV_PREFIX)
	# TODO Eventually this will come from PyPi:
	cd $(OFFLOAD_HOME) && . $(VENV_PREFIX)/bin/activate && python3 -m pip install lib/$(GOE_WHEEL)


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
target: python-goe spark-listener offload-env
	@echo -e "=> \e[92m Building target: $(TARGET_DIR)...\e[0m"
	mkdir -p $(TARGET_DIR)/bin
	cp bin/{offload,connect,logmgr,display_goe_env,clean_goe_env,agg_validate} $(TARGET_DIR)/bin
	mkdir -p $(TARGET_DIR)/tools
	cp tools/goe-shell-functions.sh $(TARGET_DIR)/tools
	rm -rf $(TARGET_DIR)/setup/sql $(TARGET_DIR)/setup/python
	mkdir -p $(TARGET_DIR)/cache
	mkdir -p $(TARGET_DIR)/setup/sql && cp -a sql/oracle/source/* $(TARGET_DIR)/setup
	mkdir -p $(TARGET_DIR)/lib && cp dist/goe-*.whl $(TARGET_DIR)/lib
	cp version $(TARGET_DIR)
	cp LICENSE $(TARGET_DIR)
	cp AUTHORS $(TARGET_DIR)
	echo "$(OFFLOAD_VERSION) ($(BUILD))" > $(TARGET_DIR)/version_build
	sed -i -e "s/VERSION/$(OFFLOAD_VERSION)/" -e "s/BUILD/$(BUILD)/" $(TARGET_DIR)/setup/sql/{install,upgrade}_env.sql
	sed -i "s/'%s-SNAPSHOT'/'$(OFFLOAD_VERSION) ($(BUILD))'/" $(TARGET_DIR)/setup/sql/create_offload*_package_spec.sql
	mkdir -p $(TARGET_DIR)/templates
	cp -r templates/goe_base.html templates/offload_status_report $(TARGET_DIR)/templates/
	mkdir -p $(TARGET_DIR)/run $(TARGET_DIR)/log
	chmod 775 $(TARGET_DIR)/run $(TARGET_DIR)/log
	chmod 640 $(TARGET_DIR)/conf/*offload.env.template


.PHONY: spark-basic-auth
spark-basic-auth:
	cd spark-basic-auth && make


.PHONY: spark-listener
spark-listener:
	cd tools/spark-listener && make target


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
