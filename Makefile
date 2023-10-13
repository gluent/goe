
TARGET_DIR=target/offload


OFFLOAD_VERSION=$(shell cat version)
LICENSE_YEAR=$(shell date +"%Y")
LICENSE_TEXT=Copyright 2015-$(LICENSE_YEAR) Gluent Inc. All rights reserved.

BUILD=$(strip $(shell git rev-parse --short HEAD))

.PHONY: target package unit-test offload-env package-spark-standalone

package: target
	cd target && make package

install: target integration-target
ifeq ('$(MAKECMDGOALS)','install')
	$(error "Install Directory not supplied to make. Should be: make install ~/offload")
else
	$(info "Install Directory: $(filter-out $@,$(MAKECMDGOALS))")
	mkdir -p $(filter-out $@,$(MAKECMDGOALS))
	cp -a target/offload/* $(filter-out $@,$(MAKECMDGOALS))
endif


integration-target:
	cd testing && make target


target: python-gluentlib license-txt offload-env
	@echo -e "=> \e[92m Building target in $(TARGET_DIR)...\e[0m"
	mkdir -p $(TARGET_DIR)/bin
	cp scripts/{offload,connect,logmgr,display_gluent_env,clean_gluent_env,schema_sync,diagnose,offload_status_report,listener} scripts/{gluent,connect,schema_sync,diagnose,offload_status_report}.py $(TARGET_DIR)/bin
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/connect $(TARGET_DIR)/bin/schema_sync
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/offload
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/connect
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/logmgr
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/display_gluent_env
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/clean_gluent_env
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/schema_sync
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/diagnose
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/offload_status_report
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/connect.py
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/schema_sync.py
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/diagnose.py
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/offload_status_report.py
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/listener
	mkdir -p $(TARGET_DIR)/scripts
	cp scripts/gluent-shell-functions.sh $(TARGET_DIR)/scripts
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/scripts/gluent-shell-functions.sh
	mkdir -p $(TARGET_DIR)/scripts/listener
	cp scripts/gluent-listener{.sh,.service} $(TARGET_DIR)/scripts/listener
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/scripts/listener/gluent-listener.sh
	chmod 0755 $(TARGET_DIR)/scripts/listener/gluent-listener.sh
	chmod 0755 $(TARGET_DIR)/bin/listener
	cp gluentlib/scripts/agg_validate $(TARGET_DIR)/bin
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/agg_validate
	rm -rf $(TARGET_DIR)/setup/sql
	mkdir -p $(TARGET_DIR)/setup/sql
	mkdir -p $(TARGET_DIR)/cache
	cp -a sql/oracle/source/* $(TARGET_DIR)/setup
	cp version $(TARGET_DIR)
	cp LICENSE.txt $(TARGET_DIR)
	sed -e "s/return '%s-RC'.*/return '$(OFFLOAD_VERSION) ($(BUILD))'/" -e "s/LICENSE_TEXT/$(LICENSE_TEXT)/" scripts/gluent.py > $(TARGET_DIR)/bin/gluent.py
	sed -i "s/LICENSE_TEXT_HEADER/$(LICENSE_TEXT)/" $(TARGET_DIR)/bin/gluent.py
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/setup/*.sql $(TARGET_DIR)/setup/sql/*.sql
	sed -i -e "s/VERSION/$(OFFLOAD_VERSION)/" -e "s/BUILD/$(BUILD)/" $(TARGET_DIR)/setup/sql/{install,upgrade}_env.sql
	sed -i "s/'%s-SNAPSHOT'/'$(OFFLOAD_VERSION) ($(BUILD))'/" $(TARGET_DIR)/setup/sql/create_offload*_package_spec.sql
	mkdir -p $(TARGET_DIR)/templates
	cp -r templates/gl_base.html templates/offload_status_report $(TARGET_DIR)/templates/
	sed -i "s/LICENSE_TEXT/$(LICENSE_TEXT)/" $(TARGET_DIR)/templates/gl_base.html
	mkdir -p $(TARGET_DIR)/run $(TARGET_DIR)/log
	chmod 775 $(TARGET_DIR)/run $(TARGET_DIR)/log
	chmod 640 $(TARGET_DIR)/conf/*offload.env.template

.PHONY: spark-basic-auth
spark-basic-auth:
	cd spark-basic-auth && make

.PHONY: spark-listener
spark-listener:
	cd spark-listener && make install

package-spark-standalone: spark-basic-auth spark-listener license-txt
	cd thirdparty && make spark-standalone
	cd target && make package-spark-standalone

python-gluentlib:
	cd gluentlib && make install

offload-env:
	cd templates/conf && make

license-txt:
	echo "$(LICENSE_TEXT)" > LICENSE.txt


### CLEANUP ###
clean:
	cd templates/conf && make clean
	cd target && make clean


%:
	@: # magic to make install directory work
