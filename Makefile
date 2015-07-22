# Copyright © SanDisk Corp. 2015 - All rights reserved.
#
# Makefile for CEPH XIO messenger.
#
EXTRA_CFLAGS := -I/root/new/accelio/include -DXIO_PERF -DUSE_WQ -O0  -ggdb -g3 -gdwarf-4
#EXTRA_CFLAGS := -I/root/raju/accelio/include -DXIO_PERF -O0  -ggdb -g3 -gdwarf-4

obj-$(CONFIG_CEPH_LIB) += xio_msgr.o

xio_msgr-y := xio_messenger.o

all:
	make -C /usr/src/linux-headers-`uname -r` SUBDIRS=`pwd` KBUILD_EXTRA_SYMBOLS="/root/new/accelio/src/kernel/xio/Module.symvers /root/new/storm-ceph-client/src/net/ceph/Module.symvers" modules

clean:
	make -C /usr/src/linux-headers-`uname -r` SUBDIRS=`pwd` clean
