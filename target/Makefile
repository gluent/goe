.PHONY: clean package

OFFLOAD_VERSION=$(shell cat ../version)
GOE_PACKAGE=goe_$(OFFLOAD_VERSION).tar.gz

package:
	rm -f ../$(GOE_PACKAGE)
	tar --exclude=".*" -zcf ../$(GOE_PACKAGE) offload

clean:
	rm -rf offload
