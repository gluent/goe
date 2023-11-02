.PHONY: license-placement

TARGET_DIR=../target/offload

ifneq ($(filter '$(MAKECMDGOALS)', 'install' 'clean' 'target'),)
  $(info "Target Directory not supplied to make. Using default: $(TARGET_DIR)")
else
  TARGET_DIR=$(filter-out $@,$(MAKECMDGOALS))
  $(info "Install Directory: $(TARGET_DIR)")
endif

PYTHON=python3
EASY_INSTALL=-m easy_install
OFFLOAD_VERSION=$(shell cat ../version)
LICENSE_YEAR=$(shell date +"%Y")
LICENSE_TEXT=Copyright 2015-$(LICENSE_YEAR) Gluent Inc. All rights reserved.
BUILD_SOURCE=build/src

install: license-placement
	# I (NJ) don't think we need this recipe.
	#	$(PYTHON) setup.py bdist_egg --exclude-source-files
	#	$(PYTHON) $(EASY_INSTALL) --no-deps --exclude-scripts dist/*egg

target: license-placement
	$(PYTHON) setup.py bdist_egg --exclude-source-files
	mkdir -p $(TARGET_DIR)/lib/ && cp dist/*egg $(TARGET_DIR)/lib/gluentlib_$(OFFLOAD_VERSION).egg

license-placement: copy-gluentlib-to-src
	find $(BUILD_SOURCE)/gluentlib -type f -not -path '*/\.*' -exec sed -i 's/LICENSE_TEXT/$(LICENSE_TEXT)/g' {} +
	sed -i 's/LICENSE_TEXT/$(LICENSE_TEXT)/g' $(BUILD_SOURCE)/scripts/*

copy-gluentlib-to-src:
	test -d $(BUILD_SOURCE) || mkdir -p $(BUILD_SOURCE)
	rm -fr $(BUILD_SOURCE)/gluentlib $(BUILD_SOURCE)/scripts
	cp -r gluentlib scripts $(BUILD_SOURCE)

clean:
	rm -fr build gluentlib.egg-info dist $(LIB_TARGET_DIR)/gluentlib*.egg

python-integrity:
	# Ensure Python is correctly built
	$(PYTHON) -c "import ssl"
	$(PYTHON) -c "import ctypes"
