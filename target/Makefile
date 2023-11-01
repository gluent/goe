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
