.PHONY: cx_Oracle lint license-placement

TARGET_DIR=../target/offload
INTEGRATION_DIR=../target/integration

ifneq ($(filter '$(MAKECMDGOALS)', 'install' 'test' 'lint' 'clean'),)
  $(info "Target Directory not supplied to make. Using default: $(TARGET_DIR)")
else
  TARGET_DIR=$(filter-out $@,$(MAKECMDGOALS))
  $(info "Install Directory: $(TARGET_DIR)")
endif

PYTHONPATH=$(TARGET_DIR)/bin
PYTHON=python3
INTEGRATION_PYTHON=$(INTEGRATION_DIR)/bin/python3
EASY_INSTALL=-m easy_install
PIP=$(PYTHON) -m pip
LICENSE_YEAR=$(shell date +"%Y")
LICENSE_TEXT=Copyright 2015-$(LICENSE_YEAR) Gluent Inc. All rights reserved.
BUILD_SOURCE=build/src

test:
	echo "##teamcity[testSuiteStarted name='gluentlib']"
	$(PYTHON) -m pytest tests
	echo "##teamcity[testSuiteFinished name='gluentlib']"

install: license-placement
	$(PYTHON) setup.py bdist_egg --exclude-source-files
	$(PYTHON) $(EASY_INSTALL) --no-deps --exclude-scripts dist/*egg

license-placement: copy-gluentlib-to-src
	find $(BUILD_SOURCE)/gluentlib -type f -not -path '*/\.*' -exec sed -i 's/LICENSE_TEXT/$(LICENSE_TEXT)/g' {} +
	sed -i 's/LICENSE_TEXT/$(LICENSE_TEXT)/g' $(BUILD_SOURCE)/scripts/*

copy-gluentlib-to-src:
	test -d $(BUILD_SOURCE) || mkdir -p $(BUILD_SOURCE)
	rm -fr $(BUILD_SOURCE)/gluentlib $(BUILD_SOURCE)/scripts
	cp -r gluentlib scripts $(BUILD_SOURCE)

clean:
	rm -fr build gluentlib.egg-info dist $(LIB_TARGET_DIR)/gluentlib*.egg
	find .. -name "gluentlib*.egg" | xargs rm -fr

python-integrity:
	# Ensure Python is correctly built
	$(PYTHON) -c "import ssl"
	$(PYTHON) -c "import ctypes"
