#
# Makefile for CEPH filesystem.
#
EXTRA_CFLAGS := -I/root/raju/accelio/include -DENABLE_XIO -O0  -ggdb -g3 -gdwarf-4

KBUILD_EXTRA_SYMBOLS="/root/raju//accelio/src/kernel/xio/Module.symvers /root/raju/ceph-client-repo/net/ceph/Module.symvers"

obj-$(CONFIG_CEPH_LIB) += xio_msgr.o

xio_msgr-y := xio_messenger.o

all:
	make -C /usr/src/linux-headers-3.13.0-24-generic SUBDIRS=`pwd` KBUILD_EXTRA_SYMBOLS="/root/raju//accelio/src/kernel/xio/Module.symvers /root/raju/ceph-client-repo/net/ceph/Module.symvers" modules

clean:
	make -C /usr/src/linux-headers-3.13.0-24-generic SUBDIRS=`pwd` clean
