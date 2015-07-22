# Copyright © SanDisk Corp. 2015 - All rights reserved.
#
# Makefile for CEPH XIO messenger.
#
EXTRA_CFLAGS := -I$(XIO_SRC)/include -DXIO_PERF -DUSE_WQ -O0  -ggdb -g3 -gdwarf-4
#EXTRA_CFLAGS := -I$(XIO_SRC)/include -DXIO_PERF -O0  -ggdb -g3 -gdwarf-4

obj-$(CONFIG_CEPH_LIB) += xio_msgr.o

xio_msgr-y := xio_messenger.o

all:
	make -C /usr/src/linux-headers-`uname -r` SUBDIRS=`pwd` KBUILD_EXTRA_SYMBOLS="$(XIO_SRC)/src/kernel/xio/Module.symvers $(SRC_TREE)/kernel-ceph-client/src/net/ceph/Module.symvers" modules

clean:
	make -C /usr/src/linux-headers-`uname -r` SUBDIRS=`pwd` clean
