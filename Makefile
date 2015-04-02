# Copyright © SanDisk Corp. 2015 - All rights reserved.
#
# Makefile for CEPH XIO messenger.
#
EXTRA_CFLAGS := -I/root/raju/accelio/include -DENABLE_XIO -DXIO_PERF -DUSE_WQ -O0  -ggdb -g3 -gdwarf-4

obj-$(CONFIG_CEPH_LIB) += xio_msgr.o

xio_msgr-y := xio_messenger.o

all:
	make -C /usr/src/linux-headers-3.13.0-48-generic SUBDIRS=`pwd` KBUILD_EXTRA_SYMBOLS="/root/raju/accelio/src/kernel/xio/Module.symvers /root/raju/ceph-client/net/ceph/Module.symvers" modules

clean:
	make -C /usr/src/linux-headers-3.13.0-48-generic SUBDIRS=`pwd` clean
