/*
 * Copyright © SanDisk Corp. 2015 - All rights reserved.
 */

#ifndef _XIO_MSGR_H_
#define _XIO_MSGR_H_

#include <linux/kref.h>
#include <linux/completion.h>
#include <libxio.h>

#define MAX_PORTAL_NAME     256

// This should always be the same value as XIO_MSGR_IOVLEN from ceph source
#define XIO_MSGR_IOVLEN 	(16)
#define XIO_MAX_SEND_MSGS	(512)	// Same as ceph
#define XIO_MAX_SEND_MSG_HIGH_WM	((XIO_MAX_SEND_MSGS * 4)/5)
//#define XIO_MAX_SEND_MSG_LOW_WM		(XIO_MAX_SEND_MSG_HIGH_WM/2)
#define XIO_MAX_SEND_MSG_LOW_WM		(XIO_MAX_SEND_MSG_HIGH_WM * 3/4)
#define XIO_MAX_SEND_BYTES	(2 << 28)	// same as ceph
#define MAX_XIO_BUF_SIZE 	(1044480)	// same as ceph
#define SGL_SZ	(XIO_MSGR_IOVLEN * sizeof(struct scatterlist))
#define XIO_HDR_LEN		(216)

#define CEPH_RDMA_BUFFER_ASSIGNED_FOR_MSG	(1 << 7)

struct libceph_rdma_session {
	struct list_head list; // in global session list
	struct xio_session *session;
	struct list_head conn_list;
	struct mutex conn_list_lock;
	atomic_t conn_count;
	char portal[MAX_PORTAL_NAME];
	struct kref kref;
	struct libceph_rdma_connection *rdma_conn;
	struct task_struct *sess_th;
	struct xio_context *ctx;
};

struct xio_rcvd_msg_hdlr {
	struct sg_table *tbl;
	struct scatterlist *sg;
	struct ceph_msg *msg;
	void *residual_buf;
	int residual_len;
	int sg_cnt;
	int total_msg_len;
	int cur_sg_off;
	int msg_chain_cnt;
	struct scatterlist *last_sg;
	void *last_buf;
};

struct libceph_rdma_connection {
	struct list_head list; // in conn list of session
	struct libceph_rdma_session *session;
	struct xio_connection  *xio_conn;
	struct sockaddr_storage *addr;
	struct ceph_connection *ceph_con;
	struct xio_context *ctx;
	struct task_struct *conn_th;
	struct list_head pending_msg_list;
	int queued_cnt;
	int pending_cnt;
	u64	last_sent_sn;
	bool close_initiated_locally;

	struct xio_rcvd_msg_hdlr hdlr;
	struct xio_rcvd_msg_hdlr inl_hdlr;
	u64 send_total_req;
	u64 send_total_time;
	u64 send_min_time;
	u64 send_max_time;
};

struct xio_processor {
	struct xio_msg *msg, *head, *tail;
	struct xio_vmsg *omsg;
	int msg_cnt;
	struct scatterlist *sg;
	int i;
	void *last_buf;
	struct scatterlist *last_sg;
	int total_msg_len;
	int remain_len;
};

struct xio_pending_msg {
	struct list_head list;
	struct xio_ev_data send_event;
	int msg_cnt;
};

static inline struct libceph_rdma_session *libceph_rdma_session_get(struct libceph_rdma_session *sess)
{
	kref_get(&sess->kref);
	return sess;
}
extern void libceph_rdma_session_last_put(struct kref *kref);
static inline void libceph_rdma_session_put(struct libceph_rdma_session *sess)
{
	kref_put(&sess->kref, libceph_rdma_session_last_put);
}
#endif
