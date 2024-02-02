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

.PHONY: clean package

OFFLOAD_VERSION=$(shell cat ../version)
GOE_PACKAGE=goe_$(OFFLOAD_VERSION).tar.gz
SPARK_PACKAGE=goe_transport_$(OFFLOAD_VERSION).tar.gz

package:
	rm -f ../$(GOE_PACKAGE)
	tar --exclude=".*" -zcf ../$(GOE_PACKAGE) offload

package-spark:
	rm -f ../$(SPARK_PACKAGE)
	tar --exclude="spark-*.tgz" -zcf ../$(SPARK_PACKAGE) transport

clean:
	rm -rf offload
	rm -rf transport
