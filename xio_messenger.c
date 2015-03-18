#include <linux/ceph/ceph_debug.h>
#include <linux/slab.h>
#include <linux/crc32c.h>
#include <linux/kthread.h>
#include <linux/module.h>
#include <linux/proc_fs.h>
#include <linux/seq_file.h>
#include <linux/reboot.h>
#include <linux/ceph/messenger.h>
#include <linux/ceph/libceph.h>

#include "xio_msgr.h"

// TODO - move these to some common header file
#define CON_STATE_CLOSED        1  /* -> PREOPEN */
#define CON_STATE_PREOPEN       2  /* -> CONNECTING, CLOSED */
#define CON_STATE_CONNECTING    3  /* -> NEGOTIATING, CLOSED */
#define CON_STATE_NEGOTIATING   4  /* -> OPEN, CLOSED */
#define CON_STATE_OPEN          5  /* -> STANDBY, CLOSED */
#define CON_STATE_STANDBY       6  /* -> PREOPEN, CLOSED */

#define NO_DEBUG		0
#define DEBUG_SLOW_PATH		1
#define DEBUG_DATA_PATH		2

#define DEBUG_LVL	(DEBUG_SLOW_PATH)

struct ceph_rdma_stats {
	atomic64_t	alloc_hdr;
	atomic64_t	alloc_hdr_failed;
	atomic64_t	free_hdr;
	atomic64_t	open_conn;
	atomic64_t	close_conn;

	atomic64_t	sess_create;
	atomic64_t	sess_free;
	atomic64_t	sess_tear;
	atomic64_t	sess_destroy;
	atomic64_t	sess_rej;
	atomic64_t	sess_err;
	atomic64_t	sess_unhandled_event;
	atomic64_t 	new_sess;
	atomic64_t 	sess_estab;
	atomic64_t	sess_new_conn;
	atomic64_t	sess_conn_est;
	atomic64_t	sess_conn_tear;
	atomic64_t	sess_conn_disconn;
	atomic64_t	sess_conn_close;
	atomic64_t	sess_conn_refused;
	atomic64_t	sess_conn_err;

	atomic64_t	remote_rsp;
	atomic64_t	msg_w_inline_data;
	atomic64_t	remote_rsp_last_msg;
	atomic64_t	assign_data_in_buf;
	atomic64_t	assign_data_in_buf_first_in_chain;
	atomic64_t	inline_data_first_in_chain;
	atomic64_t	assign_data_in_buf_w_chain;
	atomic64_t	msg_send_comp;
	atomic64_t	msg_delivered;
	atomic64_t	msg_error;
	atomic64_t	msg_cancel;
	atomic64_t	msg_cancel_req;

	atomic64_t	alloc_new_sg_list;
	atomic64_t	free_sg_list;
	atomic64_t	rcvd_msg_use_last_sg;
	atomic64_t	rcvd_msg_resid_buf;
	atomic64_t	rcvd_front_len;
	atomic64_t	rcvd_middle_len;
	atomic64_t	rcvd_data_len;

	atomic64_t	try_send;
	atomic64_t	send_err;
	atomic64_t	send_success;
	atomic64_t	add_to_pend_q;
	atomic64_t	rem_from_pend_q;
	atomic64_t	pend_q_cnt;
	atomic64_t	ow_send_comp;
	atomic64_t	ow_send_comp_last_in_chain;
	atomic64_t	send_msg_use_last_sg;
	atomic64_t	send_front_len;
	atomic64_t	send_middle_len;
	atomic64_t	send_data_len;

	atomic64_t	alloc_xio_msg;
	atomic64_t	free_xio_msg;

	atomic64_t	client_send_req;
	atomic64_t	client_send_comp;

} rdma_stats;

#define RDMA_STATS_INC(_field)	(atomic64_inc(&rdma_stats._field))
#define RDMA_STATS_DEC(_field)	(atomic64_dec(&rdma_stats._field))
#define RDMA_STATS_ADD(_field, _val)	(atomic64_add((_val), &rdma_stats._field))
#define RDMA_STATS_SUB(_field, _val)	(atomic64_sub((_val), &rdma_stats._field))
#define RDMA_STATS_GET(_field) (long long int)(atomic64_read(&rdma_stats._field))

static struct list_head g_libceph_sessions;
static struct mutex g_libceph_lock;

/* Slab caches for frequently-allocated structures */
static struct kmem_cache *ceph_rdma_msg_cache = NULL;
static struct kmem_cache *ceph_rdma_header_buf_cache = NULL;

static struct xio_ev_data trigger_event;

static inline int ceph_rcvd_msg_add_sg(struct xio_rcvd_msg_hdlr *hdlr, void *buf, int len);
static void ceph_rdma_trigger_send_on_conn(void *data);
static int ceph_rdma_xio_reboot(struct notifier_block *nb, unsigned long event, void *ptr);
static int ceph_rdma_on_new_msg(struct xio_session *session,
		struct xio_msg *msg, int more_in_batch, void *cb_user_context);
static void ceph_xio_unmap_data_pages(struct ceph_msg *m);
static int ceph_rdma_send(struct ceph_connection *con);
static inline void ceph_rdma_try_sending(struct libceph_rdma_connection *conn, int from_complete);

extern void con_fault(struct ceph_connection *con);
extern bool ceph_msg_data_advance(struct ceph_msg_data_cursor *cursor, size_t bytes);
extern int ceph_alloc_middle(struct ceph_connection *con, struct ceph_msg *msg);
extern void prepare_message_data(struct ceph_msg *msg, u32 data_len);
extern struct page *ceph_msg_data_next(struct ceph_msg_data_cursor *cursor,
					size_t *page_offset, size_t *length,
					bool *last_piece);
extern u32 ceph_crc32c_page(u32 crc, struct page *page,
				unsigned int page_offset,
				unsigned int length);
extern void ceph_msg_remove(struct ceph_msg *msg);

static struct notifier_block ceph_rdma_xio_reboot_notifier = {
	.notifier_call = ceph_rdma_xio_reboot,
	.priority = 0,
};

static int ceph_rdma_xio_reboot(struct notifier_block *nb, unsigned long event, void *ptr)
{
	printk("%s:%d: Reboot notifier called. event = %lu\n", __func__, __LINE__, event);
	return (NOTIFY_OK);
}

static void ceph_rdma_dump_vmsg_hdr(struct xio_iovec *hdr)
{
	trace_printk("HDR: base = %p, len = %zu\n", hdr->iov_base,
			hdr->iov_len);
}

static void ceph_rdma_dump_sg_tbl(struct sg_table *data_tbl)
{
	int i;
	struct scatterlist *sg;
	trace_printk("SG TBL: nents: %d, org nents: %d\n", data_tbl->nents, data_tbl->orig_nents);
        sg = data_tbl->sgl;
	for (i = 0; i < data_tbl->nents; i++) {
                trace_printk("%d: %p, %d\n", i, sg_virt(sg), sg->length);
		sg = sg_next(sg);
	}
}

static void ceph_rdma_dump_vmsg(char *str, struct xio_vmsg *vmsg)
{
	trace_printk("%s vmsg: %p\n", str, vmsg);
	ceph_rdma_dump_vmsg_hdr(&vmsg->header);
	trace_printk("sgl ty:%d, pad: %d\n", vmsg->sgl_type, vmsg->pad);
	ceph_rdma_dump_sg_tbl(&vmsg->data_tbl);
	trace_printk("USR CTX: %p\n", vmsg->user_context);
}

static void ceph_rdma_dump_xmsg(struct xio_msg *msg)
{
	trace_printk("======================================\n");
	trace_printk("XIO MSG: %p\n", msg);
	ceph_rdma_dump_vmsg("IN", &msg->in);
	ceph_rdma_dump_vmsg("OUT", &msg->out);
	trace_printk("SN: %lld, req:%p\n", msg->sn, msg->request);
	trace_printk("TY: %d, FL:0x%llx, rcpt: %d\n",
			msg->type, msg->flags, msg->receipt_res);
	trace_printk("TIME: %lld\n", msg->timestamp);
        trace_printk("CTX:%p, NEXT: %p\n", msg->user_context, msg->next);
	trace_printk("======================================\n");
	if (msg->next) {
		ceph_rdma_dump_xmsg(msg->next);
	}
}

static void *ceph_rdma_hdr_alloc(void)
{
	void * hdr;

	RDMA_STATS_INC(alloc_hdr);

	// return kzalloc(XIO_HDR_LEN, GFP_KERNEL);
	hdr = kmem_cache_zalloc(ceph_rdma_header_buf_cache, GFP_NOIO);
	if (unlikely(hdr == NULL)) {
		RDMA_STATS_INC(alloc_hdr_failed);
		return NULL;
	}
	return hdr;
}

static void ceph_rdma_hdr_free(void *hdr)
{
	BUG_ON(hdr == NULL);
	RDMA_STATS_INC(free_hdr);
	//kfree(hdr);
	kmem_cache_free(ceph_rdma_header_buf_cache, hdr);
}

static void print_connection(struct seq_file *m, struct libceph_rdma_connection *conn)
{
	struct xio_connection *xio_conn;
	// struct xio_nexus *nexus;
	// struct xio_rdma_transport *rdma_hndl;

	xio_conn = conn->xio_conn;
	seq_printf(m, "Connection: %p Q cnt: %d Pend cnt: %d\n", conn, conn->queued_cnt, conn->pending_cnt);
	seq_printf(m, "Last rcvd SN: %lld\n", conn->last_sent_sn);
	// xio_dump_rdma_hndl(xio_dump_connection(xio_conn, m), m);
}

static void print_sessions(struct seq_file *m)
{
	struct libceph_rdma_session *sess;
	struct libceph_rdma_connection *conn;
	struct xio_session *xio_sess;

	seq_printf(m, "\n\n");
	mutex_lock(&g_libceph_lock);
	list_for_each_entry(sess, &g_libceph_sessions, list) {
		xio_sess = sess->session;
		seq_printf(m, "Session: %p portal: %s conn count: %u\n", sess, sess->portal, atomic_read(&sess->conn_count));
//		seq_printf(m, "XIO sess: %p state: %d\n", xio_sess, xio_sess->state);
		mutex_lock(&sess->conn_list_lock);
		list_for_each_entry(conn, &sess->conn_list, list) {
			print_connection(m, conn);
		}
		mutex_unlock(&sess->conn_list_lock);
		seq_printf(m, "\n");
	}
	mutex_unlock(&g_libceph_lock);
}

static int xio_stats_proc_show(struct seq_file *m, void *v)
{
	seq_printf(m, "XIO stats: \n");
	seq_printf(m, "Alloc XIO hdr: %lld\n", RDMA_STATS_GET(alloc_hdr));
	seq_printf(m, "Alloc XIO hdr failed: %lld\n", RDMA_STATS_GET(alloc_hdr_failed));
	seq_printf(m, "Free XIO hdr: %lld\n", RDMA_STATS_GET(free_hdr));
	seq_printf(m, "Open connection: %lld\n", RDMA_STATS_GET(open_conn));
	seq_printf(m, "Close connection: %lld\n", RDMA_STATS_GET(close_conn));
	seq_printf(m, "Session create: %lld\n", RDMA_STATS_GET(sess_create));
	seq_printf(m, "Session create: %lld\n", RDMA_STATS_GET(sess_free));
	seq_printf(m, "Session teardown: %lld\n", RDMA_STATS_GET(sess_tear));
	seq_printf(m, "Session destroy: %lld\n", RDMA_STATS_GET(sess_destroy));
	seq_printf(m, "Session reject: %lld\n", RDMA_STATS_GET(sess_rej));
	seq_printf(m, "Session error: %lld\n", RDMA_STATS_GET(sess_err));
	seq_printf(m, "Session unhandled event: %lld\n", RDMA_STATS_GET(sess_unhandled_event));
	seq_printf(m, "New Session: %lld\n", RDMA_STATS_GET(new_sess));
	seq_printf(m, "Session established: %lld\n", RDMA_STATS_GET(sess_estab));
	seq_printf(m, "Session new connection: %lld\n", RDMA_STATS_GET(sess_new_conn));
	seq_printf(m, "Session connection established: %lld\n", RDMA_STATS_GET(sess_conn_est));
	seq_printf(m, "Session connection teardown: %lld\n", RDMA_STATS_GET(sess_conn_tear));
	seq_printf(m, "Session connection disconnected: %lld\n", RDMA_STATS_GET(sess_conn_disconn));
	seq_printf(m, "Session connection close: %lld\n", RDMA_STATS_GET(sess_conn_close));
	seq_printf(m, "Session connection refused: %lld\n", RDMA_STATS_GET(sess_conn_refused));
	seq_printf(m, "Session connection error: %lld\n", RDMA_STATS_GET(sess_conn_err));
	seq_printf(m, "Remote response: %lld\n", RDMA_STATS_GET(remote_rsp));
	seq_printf(m, "Remote response last msg: %lld\n", RDMA_STATS_GET(remote_rsp_last_msg));
	seq_printf(m, "Remote msg with inline data: %lld\n", RDMA_STATS_GET(msg_w_inline_data));
	seq_printf(m, "Assign data in buffer: %lld\n", RDMA_STATS_GET(assign_data_in_buf));
	seq_printf(m, "Assign data in buffer 1st in chain: %lld\n", RDMA_STATS_GET(assign_data_in_buf_first_in_chain));
	seq_printf(m, "Inline data 1st in chain: %lld\n", RDMA_STATS_GET(inline_data_first_in_chain));
	seq_printf(m, "Assign data in buffer with chain: %lld\n", RDMA_STATS_GET(assign_data_in_buf_w_chain));
	seq_printf(m, "Msg send complete: %lld\n", RDMA_STATS_GET(msg_send_comp));
	seq_printf(m, "Msg delivered: %lld\n", RDMA_STATS_GET(msg_delivered));
	seq_printf(m, "Msg error: %lld\n", RDMA_STATS_GET(msg_error));
	seq_printf(m, "Msg cancel: %lld\n", RDMA_STATS_GET(msg_cancel));
	seq_printf(m, "Msg cancel request: %lld\n", RDMA_STATS_GET(msg_cancel_req));
	seq_printf(m, "Alloc new SG list: %lld\n", RDMA_STATS_GET(alloc_new_sg_list));
	seq_printf(m, "Free SG list: %lld\n", RDMA_STATS_GET(free_sg_list));
	seq_printf(m, "Rcvd msg use last SG: %lld\n", RDMA_STATS_GET(rcvd_msg_use_last_sg));
	seq_printf(m, "Rcvd msg residual buffer: %lld\n", RDMA_STATS_GET(rcvd_msg_resid_buf));
	seq_printf(m, "Rcvd msg total front len: %lld\n", RDMA_STATS_GET(rcvd_front_len));
	seq_printf(m, "Rcvd msg total middle len: %lld\n", RDMA_STATS_GET(rcvd_middle_len));
	seq_printf(m, "Rcvd msg total data len: %lld\n", RDMA_STATS_GET(rcvd_data_len));
	seq_printf(m, "Try send: %lld\n", RDMA_STATS_GET(try_send));
	seq_printf(m, "Send error: %lld\n", RDMA_STATS_GET(send_err));
	seq_printf(m, "Send success: %lld\n", RDMA_STATS_GET(send_success));
	seq_printf(m, "Add to pending Q: %lld\n", RDMA_STATS_GET(add_to_pend_q));
	seq_printf(m, "Remove from pending Q: %lld\n", RDMA_STATS_GET(rem_from_pend_q));
	seq_printf(m, "Pending Q count: %lld\n", RDMA_STATS_GET(pend_q_cnt));
	seq_printf(m, "One way send complete: %lld\n", RDMA_STATS_GET(ow_send_comp));
	seq_printf(m, "One way send complete last in chain: %lld\n", RDMA_STATS_GET(ow_send_comp_last_in_chain));
	seq_printf(m, "Send msg use last SG: %lld\n", RDMA_STATS_GET(send_msg_use_last_sg));
	seq_printf(m, "Send msg total front len: %lld\n", RDMA_STATS_GET(send_front_len));
	seq_printf(m, "Send msg total middle len: %lld\n", RDMA_STATS_GET(send_middle_len));
	seq_printf(m, "Send msg total data len: %lld\n", RDMA_STATS_GET(send_data_len));
	seq_printf(m, "Alloc XIO msg: %lld\n", RDMA_STATS_GET(alloc_xio_msg));
	seq_printf(m, "Free XIO msg: %lld\n", RDMA_STATS_GET(free_xio_msg));
	seq_printf(m, "Client send requests: %lld\n", RDMA_STATS_GET(client_send_req));
	seq_printf(m, "Client send complete: %lld\n", RDMA_STATS_GET(client_send_comp));

	print_sessions(m);

	return 0;
}

static int xio_stats_proc_open(struct inode *inode, struct  file *file)
{
	return single_open(file, xio_stats_proc_show, NULL);
}

static ssize_t xio_proc_write(struct file *file, const char __user *user, size_t count, loff_t *data)
{
	char optstr[64], *str;
	char *token[2];
	int i;
	struct libceph_rdma_connection *conn;

	if (count == 0 || count > sizeof(optstr))
		return -EINVAL;
	if (copy_from_user(optstr, user, count))
		return -EFAULT;
	optstr[count - 1] = '\0';
	printk("%s: str: %s, count=%zu\n", __func__, optstr, count);
	str = optstr;
	for (i = 0; i < 2; i++) {
		token[i] = strsep(&str, " ");
	}
	if (strcmp(token[0], "trigger") == 0) {
		conn = (struct libceph_rdma_connection *)simple_strtoull(token[1], NULL, 0);
		printk("%s: conn_ptr = %p\n", __func__, conn);
		if (conn) {
			memset(&trigger_event, 0, sizeof(struct xio_ev_data));
			trigger_event.handler = ceph_rdma_trigger_send_on_conn;
			trigger_event.data = conn;
			xio_context_add_event(conn->session->ctx, &trigger_event);
		}
	}
	else {
		printk("%s: Unknown option: %s\n", __func__, token[0]);
	}
	
	return count;
}

static const struct file_operations xio_stats_proc_fops = {
	.owner = THIS_MODULE,
	.open = xio_stats_proc_open,
	.read = seq_read,
	.llseek = seq_lseek,
	.release = single_release,
	.write	= xio_proc_write,
};

static int ceph_rdma_alloc_msg(struct xio_processor *xp)
{
	struct xio_vmsg *omsg;

	RDMA_STATS_INC(alloc_xio_msg);
	BUG_ON(xp->msg);
#if 0
	xp->msg = kzalloc(sizeof(struct xio_msg), GFP_KERNEL);
#else
	xp->msg = kmem_cache_zalloc(ceph_rdma_msg_cache, GFP_KERNEL);
#endif
	if (unlikely(xp->msg == NULL)) {
		return -ENOMEM;
	}
	if (xp->head == NULL) {
		xp->head = xp->msg;
		xp->head->out.header.iov_base = ceph_rdma_hdr_alloc();
		if (unlikely(xp->head->out.header.iov_base == NULL)) {
			kmem_cache_free(ceph_rdma_msg_cache, xp->msg);
			xp->msg = NULL;
			return -ENOMEM;
		}
	}
	xp->msg->next = NULL;
	xp->msg->user_context = NULL;
	if (xp->tail != NULL) {
		xp->tail->next = xp->msg;
	}
	xp->tail = xp->msg;
	xp->msg_cnt++;
	xp->i = 0;
	omsg = &xp->msg->out;
	sg_alloc_table(&omsg->data_tbl, XIO_MSGR_IOVLEN, GFP_KERNEL);
	BUG_ON(omsg->data_tbl.sgl == NULL);
	RDMA_STATS_INC(alloc_new_sg_list);
	sg_init_table(omsg->data_tbl.sgl, XIO_MSGR_IOVLEN);
	xp->sg = omsg->data_tbl.sgl;
	omsg->sgl_type = XIO_SGL_TYPE_SCATTERLIST;
	xp->msg->type = XIO_MSG_TYPE_ONE_WAY;
	xp->omsg = omsg;

	return 0;
}

static void ceph_rdma_free_msg(struct xio_msg *msg)
{
	struct xio_vmsg *omsg;

	BUG_ON(msg == NULL);

	RDMA_STATS_INC(free_xio_msg);
	RDMA_STATS_INC(free_sg_list);
	omsg = &msg->out;
	sg_free_table(&omsg->data_tbl);
#if 0
	kfree(msg);
#else
	kmem_cache_free(ceph_rdma_msg_cache, msg);
#endif
	return;
}

static void ceph_xio_cleanup_msg(struct libceph_rdma_connection *conn, struct xio_msg *msg, int send_next_msg)
{
	struct ceph_msg *m = NULL;
	struct ceph_connection *con;

	// ceph_rdma_dump_xmsg(msg);
	if (msg->out.header.iov_base && msg->out.header.iov_len) {
		ceph_rdma_hdr_free(msg->out.header.iov_base);
		msg->out.header.iov_base = NULL;
		msg->out.header.iov_len = 0;
	}
	ceph_rdma_free_msg(msg);
	con = conn->ceph_con;

	/*
	 * In a chain of xio msgs, only the tail message will have pointer to
	 * ceph_msg.
	 */
	m = (struct ceph_msg *)msg->user_context;
	if (m) {
		RDMA_STATS_INC(ow_send_comp_last_in_chain);
		// kunmap() and pages that would have been mapped prior to send
		ceph_xio_unmap_data_pages(m);
		dout("got ack for seq %llu type %d at %p\n", m->hdr.seq,
				le16_to_cpu(m->hdr.type), m);
		//m->ack_stamp = jiffies;
		m->ack_stamp = 0xDEADBEEF;;
		dout("%s:%d: msg=%p, cnt=%d\n", __func__, __LINE__, m,
				atomic_read(&m->kref.refcount));
		BUG_ON(con != m->con);
		mutex_lock(&con->mutex);
		RDMA_STATS_INC(client_send_comp);
		m->msgr_ctx = NULL;
		ceph_msg_remove(m);
		mutex_unlock(&con->mutex);
	}
	if (send_next_msg) {
		ceph_rdma_try_sending(conn, 1);

		/* Try to send any CEPH messages that were not sent because of
		 * unavailabilty of XIO messages */
		ceph_rdma_send(con);
	}
}

int ceph_xio_close_conn(struct ceph_connection *ceph_conn)
{
	int ret = 0;
	struct libceph_rdma_connection *conn = NULL;

	RDMA_STATS_INC(close_conn);
#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("%s: close connection %p\n", __func__, ceph_conn);
#endif
	conn = (struct libceph_rdma_connection *)ceph_conn->msngr_pvt;
	BUG_ON(list_empty(&conn->pending_msg_list) == 0);
	BUG_ON(conn->queued_cnt);
	BUG_ON(conn->pending_cnt);
	conn->close_initiated_locally = true;
	ret = xio_disconnect(conn->xio_conn);
	//con_flag_clear(ceph_con, CON_FLAG_SOCK_CLOSED);

	//con_sock_state_closed(ceph_con);

	return ret;
}

/* XIO changes */
static inline void ceph_add_sg_ent(struct xio_processor *xp, void *buf, size_t len)
{
	int ret;

	if (xp->last_sg && xp->last_buf && (buf == xp->last_buf) && ((xp->last_sg->length + len) <= MAX_XIO_BUF_SIZE)) {
		RDMA_STATS_INC(send_msg_use_last_sg);
		xp->last_sg->length += len;
		xp->last_buf += len;
		return;
	}
	if (xp->msg == NULL) {
		ret = ceph_rdma_alloc_msg(xp);
		BUG_ON(ret);
	}

	sg_set_buf(xp->sg, buf, len);
	xp->last_sg = xp->sg;
	xp->last_buf = buf + len;
	xp->sg = sg_next(xp->sg);
	xp->i++;
	if (xp->i >= XIO_MSGR_IOVLEN) {
		xp->omsg->data_tbl.nents = xp->i;
		sg_mark_end(xp->last_sg);
		xp->msg = NULL;
	}
	return;
}

static void ceph_encode_headers(struct xio_iovec *encode, int msg_cnt, struct ceph_msg_header *hdr,
	struct ceph_msg_footer *footer)
{
	void *start = encode->iov_base;
	void *p = start;

	BUG_ON(!p);
	/* XIO header */
	ceph_encode_32(&p, cpu_to_le32(msg_cnt));
	ceph_encode_32(&p, cpu_to_le32(CEPH_ENTITY_TYPE_CLIENT));
	p += sizeof(struct ceph_entity_addr);

	/* CEPH msg header */
	ceph_encode_64(&p, hdr->seq);
	ceph_encode_64(&p, hdr->tid);
	ceph_encode_16(&p, hdr->type);
	ceph_encode_16(&p, hdr->priority);
	ceph_encode_16(&p, hdr->version);
	ceph_encode_32(&p, hdr->front_len);
	ceph_encode_32(&p, hdr->middle_len);
	ceph_encode_32(&p, hdr->data_len);
	ceph_encode_16(&p, hdr->data_off);
	ceph_encode_8(&p, hdr->src.type);
	ceph_encode_64(&p, hdr->src.num);
	p += sizeof(u16);
	ceph_encode_32(&p, hdr->crc);

	/* CEPH msg footer */
	ceph_encode_32(&p, footer->front_crc);
	ceph_encode_32(&p, footer->middle_crc);
	ceph_encode_32(&p, footer->data_crc);
	p += sizeof(u64); // kRBD does not support sig field
	ceph_encode_8(&p, footer->flags);
	encode->iov_len = p - start;

	return;
}

static inline int ceph_rdma_send_for_sure(struct xio_msg *msg, struct libceph_rdma_connection *conn, int msg_cnt)
{
	int ret;
	struct ceph_msg *m;
	struct xio_pending_msg *pend_msg;

	pend_msg = (struct xio_pending_msg *)&msg->in;
	memset(pend_msg, 0, sizeof(msg->in));
	m = msg->user_context;
	if (msg->next != NULL) {
		msg->user_context = NULL;
	}
	RDMA_STATS_INC(try_send);
	ret = xio_send_msg(conn->xio_conn, msg);
	if (likely((ret == 0) || ((ret == -1) && (xio_errno() == -1)))) {
		RDMA_STATS_INC(send_success);
		conn->queued_cnt += msg_cnt;
		return 0;
	}
	else {
		RDMA_STATS_INC(send_err);
		trace_printk("xio_send ret = %d(err: %d), Q cnt: %d, pend cnt: %d\n",
			ret, xio_errno(), conn->queued_cnt, conn->pending_cnt);
		trace_printk("adding msg %p/%p to pending q head\n", msg, m);
		RDMA_STATS_INC(add_to_pend_q);
		INIT_LIST_HEAD(&pend_msg->list);
		list_add(&pend_msg->list, &conn->pending_msg_list);
		pend_msg->msg_cnt = msg_cnt;
		msg->user_context = m;
		conn->pending_cnt += pend_msg->msg_cnt;
		RDMA_STATS_ADD(pend_q_cnt, pend_msg->msg_cnt);
		return 1;
	}
}

static inline void ceph_rdma_try_sending(struct libceph_rdma_connection *conn, int from_complete)
{
	struct xio_msg *msg;
	struct xio_pending_msg *pend_msg;
	struct xio_vmsg *vmsg;
	/* Peek into the list and see if we can send some messages */
	if (from_complete == 0) {
		printk("%s: trigger for conn %p from proc, pend list=%d, Q cnt=%d, pend cnt = %d\n", __func__, conn,
			list_empty(&conn->pending_msg_list), conn->queued_cnt, conn->pending_cnt);
	}
	// while ((list_empty(&conn->pending_msg_list) == 0) && (conn->queued_cnt <= XIO_MAX_SEND_MSG_LOW_WM)) {
	if ((list_empty(&conn->pending_msg_list) == 0) && (conn->queued_cnt <= XIO_MAX_SEND_MSG_LOW_WM)) {
		while (list_empty(&conn->pending_msg_list) == 0) {
			struct ceph_msg *m;
			pend_msg = list_first_entry(&conn->pending_msg_list,
					struct xio_pending_msg, list);
			vmsg = (struct xio_vmsg *)pend_msg;
			msg = container_of(vmsg, struct xio_msg, in);
			if (from_complete == 0) {
				printk("%s: trigger msg=%p, cnt=%d, Q cnt=%d\n", __func__,
					pend_msg, pend_msg->msg_cnt, conn->queued_cnt);
			}
			if ((pend_msg->msg_cnt + conn->queued_cnt) > XIO_MAX_SEND_MSG_HIGH_WM) {
				break;;
			}
			RDMA_STATS_INC(rem_from_pend_q);
			list_del(&pend_msg->list);
			conn->pending_cnt -= pend_msg->msg_cnt;
			RDMA_STATS_SUB(pend_q_cnt, pend_msg->msg_cnt);
			//m = (struct ceph_msg *)msg->user_context;
			// BUG_ON(con != m->con);
			m = (struct ceph_msg *)msg->user_context;
			if (from_complete == 0) {
				printk("%s:%d: Trying to sending msg %p/%p from pend Q. Q cnt=%d, pend cnt=%d(seq: %lld, tid: %lld)\n",
						__func__, __LINE__, msg, pend_msg, conn->queued_cnt, conn->pending_cnt,
						m->hdr.seq, m->hdr.tid);
			}
				trace_printk("Trying to sending msg %p/%p from pend Q. Q cnt=%d, pend cnt=%d\n",
						msg, pend_msg, conn->queued_cnt, conn->pending_cnt);
			if (m) {
				trace_printk("m=%p, seq: %lld, tid: %lld\n", m, m->hdr.seq, m->hdr.tid);
			}
			if (ceph_rdma_send_for_sure(msg, conn, pend_msg->msg_cnt)) {
				break;
			}
		}
	}
	return;
}

static void ceph_rdma_send_in_ctx(void *data)
{
	struct xio_msg *msg;
	struct ceph_msg *m;
	struct libceph_rdma_connection *conn;
	struct ceph_connection *con;
	struct xio_pending_msg *pend_msg;

	msg = (struct xio_msg *)data;
	pend_msg = (struct xio_pending_msg *)&msg->in;
	m = (struct ceph_msg *)msg->user_context;
	con = m->con;
	conn = (struct libceph_rdma_connection *)con->msngr_pvt;
	if ((list_empty(&conn->pending_msg_list) == 0) || 
			((pend_msg->msg_cnt + conn->queued_cnt) > XIO_MAX_SEND_MSG_HIGH_WM)){
		dout("%s:%d add to pend q, pend list=%d, mc=%d, qcnt=%d(seq: %lld, tid: %lld), m=%p/%p\n", __func__,
			__LINE__, list_empty(&conn->pending_msg_list), pend_msg->msg_cnt,
			conn->queued_cnt, m->hdr.seq, m->hdr.tid, msg, pend_msg);
		INIT_LIST_HEAD(&pend_msg->list);
		list_add_tail(&pend_msg->list, &conn->pending_msg_list);
		RDMA_STATS_INC(add_to_pend_q);
		conn->pending_cnt += pend_msg->msg_cnt;
		RDMA_STATS_ADD(pend_q_cnt, pend_msg->msg_cnt);
		// ceph_rdma_try_sending(conn, 0);
		return;

	}
	ceph_rdma_send_for_sure(msg, conn, pend_msg->msg_cnt);
	return;
}

static int ceph_rdma_send(struct ceph_connection *con)
{
	mutex_lock(&con->mutex);
	while (true) {
		u32 crc;
		struct libceph_rdma_connection *conn;
		struct xio_processor proc, *xp;
		struct ceph_msg *m;

#if 0
		if ((fault = con_sock_closed(con))) {
			dout("%s: con %p SOCK_CLOSED\n", __func__, con);
			break;
		}
#endif
		if (unlikely(con->state == CON_STATE_STANDBY)) {
			dout("%s: con %p STANDBY\n", __func__, con);
			break;
		}
		if (unlikely(con->state == CON_STATE_CLOSED)) {
			dout("%s: con %p CLOSED\n", __func__, con);
			BUG_ON(con->msngr_pvt);
			break;
		}
		if (list_empty(&con->out_queue)) {
			dout("%s:%d: Con Q empty. con state = %lu\n", __func__, __LINE__, con->state);
			break;
		}
		if (unlikely(con->state != CON_STATE_OPEN)) {
			dout("%s:%d con->state = %zu, qcnt=%d\n", __func__, __LINE__, con->state,
					list_empty(&con->out_queue));
			break;
		}

		xp = &proc;
		memset(xp, 0, sizeof(struct xio_processor));

		/*
		 * Try to allocate atleast one msg and its header buf before pulling
		 * a ceph msg from the Q.
		 */
		if (ceph_rdma_alloc_msg(xp)) {
#if DEBUG_LVL == DEBUG_DATA_PATH
			trace_printk("%s:%d Msg/hdr alloc failed\n", __func__, __LINE__);
#endif
			break;
		}
		RDMA_STATS_INC(client_send_req);
		m = list_first_entry(&con->out_queue, struct ceph_msg, list_head);
		BUG_ON(m->con != con);
		/* put message on sent list */
		ceph_msg_get(m);
		list_move_tail(&m->list_head, &con->out_sent);

		/*
		 * only assign outgoing seq # if we haven't sent this
		 * message yet.  if it is requeued, resend with it's
		 * original seq.
		 */
		if (m->needs_out_seq) {
			m->hdr.seq = cpu_to_le64(++con->out_seq);
			m->needs_out_seq = false;
		}
		if (m->data_length != le32_to_cpu(m->hdr.data_len)) {
			printk("%s:%d: DL: %zu != hdr DL: %d/%u\n", __func__, __LINE__,
				m->data_length, le32_to_cpu(m->hdr.data_len), m->hdr.data_len);
			printk("FL: %zu, hdr FL: %d/%d, type: %d/%d\n", m->front.iov_len,
				le32_to_cpu(m->hdr.front_len), m->hdr.front_len, le16_to_cpu(m->hdr.type),
				m->hdr.type);
			WARN_ON(1);
		}

		m->ack_stamp = 0x12345678;
		dout("%s:%d %p seq %lld type %d len %d+%d+%zd\n",
				__func__, __LINE__, m, con->out_seq,
				le16_to_cpu(m->hdr.type),
				le32_to_cpu(m->hdr.front_len),
				le32_to_cpu(m->hdr.middle_len), m->data_length);
		BUG_ON(le32_to_cpu(m->hdr.front_len) != m->front.iov_len);
		conn = (struct libceph_rdma_connection *)con->msngr_pvt;
		ceph_add_sg_ent(xp, m->front.iov_base, m->front.iov_len);
		RDMA_STATS_ADD(send_front_len, m->front.iov_len);
		if (m->middle) {
			BUG_ON(le32_to_cpu(m->hdr.middle_len) == 0);
			ceph_add_sg_ent(xp, m->middle->vec.iov_base, m->middle->vec.iov_len);
			RDMA_STATS_ADD(send_middle_len, m->middle->vec.iov_len);
		}
		crc = crc32c(0, &m->hdr, offsetof(struct ceph_msg_header, crc));
		m->hdr.crc = cpu_to_le32(crc);
		m->footer.flags = 0;
		crc = crc32c(0, m->front.iov_base, m->front.iov_len);
		m->footer.front_crc = cpu_to_le32(crc);
		if (m->middle) {
			crc = crc32c(0, m->middle->vec.iov_base,
					m->middle->vec.iov_len);
			m->footer.middle_crc = cpu_to_le32(crc);
		} else {
			m->footer.middle_crc = 0;
		}
		dout("%s:%d front_crc %u middle_crc %u\n", __func__, __LINE__, 
				le32_to_cpu(m->footer.front_crc),
				le32_to_cpu(m->footer.middle_crc));

		m->footer.data_crc = 0;
		m->footer.flags |= CEPH_MSG_FOOTER_COMPLETE;

		/* is there a data payload? */
		if (m->data_length) {
			struct ceph_msg_data_cursor *cursor;
			bool do_datacrc = !con->msgr->nocrc;

			BUG_ON(list_empty(&m->data));
			prepare_message_data(m, m->data_length);
			cursor = &m->cursor;
			crc = do_datacrc ? le32_to_cpu(m->footer.data_crc) : 0;
			while (cursor->resid) {
				struct page *page;
				size_t page_offset;
				size_t length;
				bool last_piece;
				bool need_crc;

				page = ceph_msg_data_next(&m->cursor,
						&page_offset, &length,
						&last_piece);
				// TODO
				// BUG_ON(page_count(page) >= 1);

				/* Page kunmap() will happen once the send has completed */
				ceph_add_sg_ent(xp, kmap(page) + page_offset, length);
				RDMA_STATS_ADD(send_data_len, length);
				if (do_datacrc && cursor->need_crc) {
					crc = ceph_crc32c_page(crc, page, page_offset, length);
				}
				need_crc = ceph_msg_data_advance(&m->cursor, (size_t)length);
			}
			if (do_datacrc) {
				m->footer.data_crc = cpu_to_le32(crc);
			}
			else {
				m->footer.flags |= CEPH_MSG_FOOTER_NOCRC;
			}
		}
		BUG_ON(!xp->omsg);
		xp->omsg->data_tbl.nents = xp->i;
		sg_mark_end(xp->last_sg);
		ceph_encode_headers(&xp->head->out.header, xp->msg_cnt, &m->hdr, &m->footer);
		dout("S: SN:%lld MC:%d T-%d F-%zu M-%zu D-%zu\n", m->hdr.seq, xp->msg_cnt, m->hdr.type, m->front.iov_len,
				(m->middle) ? m->middle->vec.iov_len : 0, m->data_length);
#if DEBUG_LVL == DEBUG_DATA_PATH
		trace_printk("S: SN:%lld MC:%d T-%d F-%zu M-%zu D-%zu\n", m->hdr.seq, xp->msg_cnt, m->hdr.type, m->front.iov_len,
				(m->middle) ? m->middle->vec.iov_len : 0, m->data_length);
#endif

#if 0
		/* TODO - xio ceph always expects assign data buffer to be invoked */
		msg->flags |= XIO_MSG_FLAG_SMALL_ZERO_COPY;
#endif

		/*
		 * This is temp, till the request is picked up by the context thread
		 * which should set it to NULL;
		 */
		xp->head->user_context = (void *)m;
		xp->tail->user_context = (void *)m;
		m->msgr_ctx = xp->head;
		{
			struct xio_ev_data *send_event;
			struct xio_pending_msg *pend_msg;
			int ret = 0;

			pend_msg = (struct xio_pending_msg *)&xp->head->in;
			send_event = (struct xio_ev_data *)&pend_msg->send_event;
			pend_msg->msg_cnt = xp->msg_cnt;
			memset(send_event, 0, sizeof(struct xio_ev_data));
			send_event->handler = ceph_rdma_send_in_ctx;
			send_event->data = (void *)xp->head;
			ret = xio_context_add_event(conn->session->ctx, send_event);
			BUG_ON(ret);
			dout("%s:%d: msg=%p, cnt=%d\n", __func__, __LINE__, m, atomic_read(&m->kref.refcount));
			ceph_msg_put(m);
		}
		// xio_context_run_loop(conn->ctx);
	}
	// TODO
	// Handle fault!!
	mutex_unlock(&con->mutex);
	return 0;
}

static int ceph_rdma_on_session_event(struct xio_session *session,
		struct xio_session_event_data *event_data,
		void *cb_user_context)
{
	struct libceph_rdma_session *sess;
	struct libceph_rdma_connection *conn;
	struct xio_connection *xio_conn;
	struct ceph_connection *ceph_con;

#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("%s:session event: %s. reason: %s\n", __func__,
			xio_session_event_str(event_data->event),
			xio_strerror(event_data->reason));
#endif

	sess = (struct libceph_rdma_session *)cb_user_context;
	xio_conn = (struct xio_connection *)event_data->conn;
	conn = (struct libceph_rdma_connection *)event_data->conn_user_context;
	switch (event_data->event) {
		case XIO_SESSION_REJECT_EVENT:
			RDMA_STATS_INC(sess_rej);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s: session %p reject event. uc=%p\n",
					__func__, session, cb_user_context);
#endif
			break;
		case XIO_SESSION_NEW_CONNECTION_EVENT:
			RDMA_STATS_INC(sess_new_conn);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s: new conn event. sess=%p,conn=%p, uc=%p\n",
					__func__, session, xio_conn, cb_user_context);
#endif
			break;
		case XIO_SESSION_CONNECTION_ESTABLISHED_EVENT:
			RDMA_STATS_INC(sess_conn_est);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s: conn %p/%p established, sess=%p/%p\n", __func__,
				xio_conn, conn, session, sess);
#endif
			// atomic_inc(&sess->conns_count);
			// atomic_dec(&sess->conn_init_count);
			ceph_con = (struct ceph_connection *)conn->ceph_con;
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s:%d: xio conn=%p, usr con=%p, ceph con=%p\n",
				__func__, __LINE__, xio_conn, conn, ceph_con);
#endif
			mutex_lock(&ceph_con->mutex);
			ceph_con->state = CON_STATE_OPEN;
			ceph_con->connect_seq++;
			mutex_unlock(&ceph_con->mutex);
			
			/* Send anything that was waiting when connection was being
				setup */
			ceph_rdma_send(ceph_con);
			break;

		case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
			RDMA_STATS_INC(sess_conn_tear);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s:%d conn %p/%p teardown. sess=%p/%p\n", __func__,
				__LINE__, xio_conn, conn, session, sess);
#endif
			xio_connection_destroy(xio_conn);
			conn->xio_conn = NULL;
			ceph_con = (struct ceph_connection *)conn->ceph_con;
			mutex_lock(&ceph_con->mutex);

			// con_fault(ceph_con);
			// con_flag_clear(con, CON_FLAG_SOCK_CLOSED);
			ceph_con->msngr_pvt = NULL;

			//con_sock_state_closed(con);
			ceph_con->ops->put(ceph_con);
			mutex_unlock(&ceph_con->mutex);
			BUG_ON(conn->session != sess);
			mutex_lock(&sess->conn_list_lock);
			list_del(&conn->list);
			atomic_dec(&sess->conn_count);
			mutex_unlock(&sess->conn_list_lock);
			kfree(conn);
			libceph_rdma_session_put(sess);
			break;

		case XIO_SESSION_TEARDOWN_EVENT:
			RDMA_STATS_INC(sess_tear);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s:%d: session %p/%p teardown event\n",
					__func__, __LINE__, session, sess);
#endif
			BUG_ON(list_empty(&sess->conn_list) == 0);
			xio_context_stop_loop(sess->ctx);
			/* Continue cleanup in ceph_rdma_session_work() */
			break;

		case XIO_SESSION_CONNECTION_DISCONNECTED_EVENT:
			RDMA_STATS_INC(sess_conn_disconn);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s: conn %p/%p discon, sess=%p/%p\n", 
					__func__, xio_conn, conn, session, sess);
#endif
			/* Freeup any pending XIO messeges that are yet to be sent. */
			while (list_empty(&conn->pending_msg_list) == 0) {
				struct xio_msg *msg, *msg_next;
				struct xio_pending_msg *pend_msg;
				struct xio_vmsg *vmsg;
				pend_msg = list_first_entry(&conn->pending_msg_list,
						struct xio_pending_msg, list);
				vmsg = (struct xio_vmsg *)pend_msg;
				msg = container_of(vmsg, struct xio_msg, in);
				RDMA_STATS_INC(rem_from_pend_q);
				list_del(&pend_msg->list);
				conn->pending_cnt -= pend_msg->msg_cnt;
				RDMA_STATS_SUB(pend_q_cnt, pend_msg->msg_cnt);
				if (msg->next) {
					msg->user_context = NULL;
				}
				for (; msg; msg = msg_next) {
					msg_next = msg->next;
					ceph_xio_cleanup_msg(conn, msg, 0);
				}
			}
			break;
		case XIO_SESSION_CONNECTION_CLOSED_EVENT:
			RDMA_STATS_INC(sess_conn_close);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s: conn %p/%p closed, sess=%p/%p\n",
					__func__, xio_conn, conn, session, sess);
#endif
			ceph_con = (struct ceph_connection *)conn->ceph_con;
			if (conn->close_initiated_locally == false) {
				ceph_con_close(ceph_con);
			}
			mutex_lock(&ceph_con->mutex);
			ceph_con->state = CON_STATE_CLOSED;
			mutex_unlock(&ceph_con->mutex);
			break;
		case XIO_SESSION_CONNECTION_REFUSED_EVENT:
			RDMA_STATS_INC(sess_conn_refused);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s:%d conn %p/%p refused, sess=%p/%p\n", 
					__func__, __LINE__, xio_conn, conn, 
					session, sess);
#endif
			break;
		case XIO_SESSION_CONNECTION_ERROR_EVENT:
			RDMA_STATS_INC(sess_conn_err);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s: conn %p/%p error, sess=%p/%p\n",
					__func__, xio_conn, conn, session, sess);
#endif
			break;
		case XIO_SESSION_ERROR_EVENT:
			RDMA_STATS_INC(sess_err);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s: session %p/%p error\n", __func__,
					session, sess);
#endif
			break;
		default:
			RDMA_STATS_INC(sess_unhandled_event);
#if DEBUG_LVL == DEBUG_SLOW_PATH
			printk("%s: Unhandled event %d\n", __func__,
					event_data->event);
#endif
			break;
	}

	return 0;
}

/*
 * kunmap any pages that we mapped either for send or receive
 */
static void ceph_xio_unmap_data_pages(struct ceph_msg *m)
{
	if (m->data_length) {
		struct ceph_msg_data_cursor *cursor;
		BUG_ON(list_empty(&m->data));
		prepare_message_data(m, m->data_length);
		cursor = &m->cursor;
		while (cursor->resid) {
			struct page *page;
			size_t page_offset;
			size_t length;
			bool last_piece;

			page = ceph_msg_data_next(&m->cursor, &page_offset, &length, &last_piece);
			kunmap(page);
			ceph_msg_data_advance(&m->cursor, (size_t)length);
		}
	}

	return;
}

static inline int ceph_rcvd_copy_buffers(struct xio_rcvd_msg_hdlr *hdlr,
		void *buf, int len)
{
	int cur_len;

	while (1) {
		cur_len = min((int)hdlr->sg->length - hdlr->cur_sg_off, len);
		dout("copying from %p to %p of len %d(off=%d)\n", sg_virt(hdlr->sg) + hdlr->cur_sg_off, buf,
			cur_len, hdlr->cur_sg_off);
		memcpy(buf, sg_virt(hdlr->sg) + hdlr->cur_sg_off, cur_len);
		len -= cur_len;
		buf += cur_len;
		hdlr->cur_sg_off += cur_len;
		if (hdlr->cur_sg_off == hdlr->sg->length) {
			hdlr->sg = sg_next(hdlr->sg);
			hdlr->sg_cnt++;
			hdlr->cur_sg_off = 0;
		}
		if (len == 0) {
			break;
		}
	}

	return 0;
}

static inline void ceph_rcvd_msg_finalize_sg(struct xio_rcvd_msg_hdlr *hdlr)
{
	if (hdlr->sg_cnt) {
		sg_mark_end(hdlr->last_sg);
	}
	hdlr->last_buf = hdlr->last_sg = NULL;
	hdlr->tbl->nents = hdlr->sg_cnt;
}

static inline void ceph_init_rcvd_msg_hdlr(struct xio_rcvd_msg_hdlr *hdlr,
		struct xio_msg *msg, int assign_buffers)
{
	struct scatterlist *sg;
	int i, ret = 0;
	int req_ents = 0;
	struct xio_vmsg *imsg, *omsg;

	imsg = &msg->in;
	omsg = &msg->out;
	imsg->user_context = NULL;
	hdlr->total_msg_len = 0;
	for_each_sg(imsg->data_tbl.sgl, sg, imsg->data_tbl.nents, i) {
		hdlr->total_msg_len += sg->length;
	}
	if (assign_buffers) {
		req_ents = hdlr->total_msg_len/4096;
		if (hdlr->total_msg_len%4096) {
			req_ents++;
		}
		req_ents++;
		if (req_ents > imsg->data_tbl.orig_nents) {
			RDMA_STATS_INC(alloc_new_sg_list);
			imsg->user_context = imsg->data_tbl.sgl;
			BUG_ON(imsg->pad);
			BUG_ON(omsg->pad);
			imsg->pad = imsg->data_tbl.nents;
			omsg->pad = imsg->data_tbl.orig_nents;
			memset(&imsg->data_tbl, 0, sizeof(imsg->data_tbl));
			ret = sg_alloc_table(&imsg->data_tbl, req_ents, GFP_KERNEL);
			if (ret) {
				printk("%s: SG tbl alloc returned %d\n", __func__, ret);
			}
			dout("%s:%d: allocating new SG list with %d entries(sg ent sz = %lu, sg tbl sz=%lu)\n",
					__func__, __LINE__, req_ents, sizeof(struct scatterlist), sizeof(struct sg_table));
		}
	}
	hdlr->tbl = &imsg->data_tbl;
	hdlr->sg = hdlr->tbl->sgl;
	hdlr->sg_cnt = 0;
	hdlr->cur_sg_off = 0;

	/*
	 * Handle any left over buffers from the previous messege in
	 * the same chain.
	 */
	if (hdlr->residual_len) {
		void *buf;
		int buf_len;

		buf = hdlr->residual_buf;
		buf_len = hdlr->residual_len;
		hdlr->residual_len = 0;
		hdlr->residual_buf = NULL;
		if (assign_buffers) {
			ret = ceph_rcvd_msg_add_sg(hdlr, buf, buf_len);
		}
		else {
			ceph_rcvd_copy_buffers(hdlr, buf, buf_len);
		}
	}
}

static inline int ceph_rcvd_msg_add_sg(struct xio_rcvd_msg_hdlr *hdlr, void *buf, int len)
{
	int cur_len;
	int ret = 0;

	cur_len = min(hdlr->total_msg_len, len);
	if (hdlr->last_sg && hdlr->last_buf && (hdlr->last_buf == buf)) {
		RDMA_STATS_INC(rcvd_msg_use_last_sg);
		hdlr->last_sg->length += cur_len;
		hdlr->last_buf += cur_len;
	}
	else {
		if (hdlr->sg_cnt >= hdlr->tbl->orig_nents) {
			if (hdlr->total_msg_len) {
				printk("%s:%d: Insufficient sg ents: sg=%d,nents=%d,tot=%d\n",
						__func__, __LINE__,
						hdlr->sg_cnt,
						hdlr->tbl->orig_nents,
						hdlr->total_msg_len);
				printk("l = %d\n", len);
				BUG_ON(0);
			}
			ceph_rcvd_msg_finalize_sg(hdlr);
			return 1;
		}
		else {
			sg_unmark_end(hdlr->sg);
			sg_set_buf(hdlr->sg, buf, cur_len);
			hdlr->last_sg = hdlr->sg;
			hdlr->last_buf = buf + cur_len;
			hdlr->sg = sg_next(hdlr->sg);
			hdlr->sg_cnt++;
		}
	}
	hdlr->total_msg_len -= cur_len;
	len -= cur_len;
	buf += cur_len;
	if (hdlr->total_msg_len == 0) {
		ret = 1;
		if (len) {
			RDMA_STATS_INC(rcvd_msg_resid_buf);
			hdlr->residual_buf = buf;
			hdlr->residual_len = len;
		}
		ceph_rcvd_msg_finalize_sg(hdlr);
	}

	return ret;
}

static int ceph_rdma_process_data(struct xio_msg *msg,
		struct libceph_rdma_connection *conn, int assign_buffers)
{
	struct xio_rcvd_msg_hdlr *hdlr;
	unsigned int data_len = 0;
	struct ceph_msg *m;
	int ret = 0;

	if (assign_buffers) {
		hdlr = &conn->hdlr;
	}
	else {
		hdlr = &conn->inl_hdlr;
	}
	BUG_ON(hdlr->msg == NULL);
	m = hdlr->msg;
	data_len = le32_to_cpu(m->hdr.data_len);
	/* (page) data */
	if (data_len) {
		struct ceph_msg_data_cursor *cursor = &m->cursor;
		struct page *page;
		size_t page_offset;
		size_t length;
		void *kaddr;

		if (list_empty(&m->data)) {
			return -EIO;
		}
		while (cursor->resid) {
			page = ceph_msg_data_next(&m->cursor, &page_offset, &length, NULL);
			BUG_ON(page_offset + length > PAGE_SIZE);
			kaddr = kmap(page);
			BUG_ON(!kaddr);
			(void) ceph_msg_data_advance(&m->cursor, (size_t)length);
			if (assign_buffers) {
				ret = ceph_rcvd_msg_add_sg(hdlr, kaddr + page_offset, length);
			}
			else {
				ceph_rcvd_copy_buffers(hdlr, kaddr + page_offset, length);
			}
			RDMA_STATS_ADD(rcvd_data_len, length);
			if (ret) {
				/*
				 * We have reached the end of the current
				 * input and/or output SG list. Wait for the
				 * next message in this chain to continue
				 * processing
				 */
				break;
			}
		}
	}

	return ret;
}

/* Assumes that the ceph connection mutex is held */
static int ceph_rdma_process_headers(struct xio_msg *msg,
		struct libceph_rdma_connection *conn, int assign_buffers)
{
	struct xio_vmsg *imsg;
	struct ceph_connection *con;
	struct ceph_msg *m;
	void *start, *p, *end;
	struct ceph_msg_header m_hdr;
	struct xio_rcvd_msg_hdlr *hdlr;
	unsigned int data_len = 0;
	u32 crc;
	u64 seq;
	int skip = 0;
	int ret = 0;

	if (assign_buffers) {
		hdlr = &conn->hdlr;
	}
	else {
		hdlr = &conn->inl_hdlr;
	}
	BUG_ON(hdlr->msg);
	imsg = &msg->in;
	start = imsg->header.iov_base;
	p = start;
	BUG_ON((p == NULL) || (imsg->header.iov_len == 0));
	end = p + imsg->header.iov_len;
	ceph_decode_need(&p, end, 2 * sizeof(32) + sizeof(struct ceph_entity_addr), bad);
	hdlr->msg_chain_cnt = ceph_decode_32(&p);
	p += sizeof(u32) + sizeof(struct ceph_entity_addr);
	ceph_decode_need(&p, end, 3 * sizeof(64) + 5 * sizeof(u32) + 3 * sizeof(16) + sizeof(u8), bad);
	m_hdr.seq = ceph_decode_64(&p);
	m_hdr.tid = ceph_decode_64(&p);
	m_hdr.type = ceph_decode_16(&p);
	m_hdr.priority = ceph_decode_16(&p);
	m_hdr.version = ceph_decode_16(&p);
	m_hdr.front_len = ceph_decode_32(&p);
	m_hdr.middle_len = ceph_decode_32(&p);
	data_len = m_hdr.data_len = ceph_decode_32(&p);
	m_hdr.data_off = ceph_decode_16(&p);
	m_hdr.src.type = ceph_decode_8(&p);
	m_hdr.src.num = ceph_decode_64(&p);
	p += sizeof(u16);
	m_hdr.crc = ceph_decode_32(&p);
	crc = crc32c(0, &m_hdr, offsetof(struct ceph_msg_header, crc));
	if (cpu_to_le32(crc) != m_hdr.crc) {
		// pr_err("%s:bad hdr crc %u != expected %u\n", __func__, crc,
		// 	m_hdr.crc);
		// return -EBADMSG;
	}
	if (m_hdr.front_len > CEPH_MSG_MAX_FRONT_LEN) {
		printk("%s:%d: front len %d > %d\n", __func__, __LINE__,
				m_hdr.front_len, CEPH_MSG_MAX_FRONT_LEN);
		return -EIO;
	}
	if (m_hdr.middle_len > CEPH_MSG_MAX_MIDDLE_LEN) {
		printk("%s:%d: middle len %d > %d\n", __func__, __LINE__,
				m_hdr.middle_len, CEPH_MSG_MAX_MIDDLE_LEN);
		return -EIO;
	}
	if (m_hdr.data_len > CEPH_MSG_MAX_DATA_LEN) {
		printk("%s:%d: data len %d > %d\n", __func__, __LINE__,
				m_hdr.data_len, CEPH_MSG_MAX_DATA_LEN);
		return -EIO;
	}
	if (hdlr->msg_chain_cnt > 1) {
		dout("%s:%d: Msg cnt %d, seq %lld, type %d\n", __func__, __LINE__, hdlr->msg_chain_cnt, m_hdr.seq, m_hdr.type);
		dout("FL:%d ML: %d DL: %d\n", m_hdr.front_len, m_hdr.middle_len, m_hdr.data_len);
	}
	dout("AD: M %p got hdr type %d seq %lld front %d data %d\n", msg, m_hdr.type, m_hdr.seq, m_hdr.front_len, m_hdr.data_len);
#if DEBUG_LVL == DEBUG_DATA_PATH
	trace_printk("AD: MC %d %p got hdr type %d seq %lld tid %lld front %d data %d\n", hdlr->msg_chain_cnt, msg, m_hdr.type, m_hdr.seq, m_hdr.tid, m_hdr.front_len, m_hdr.data_len);
#endif
	// TODO Check connection state
	// TODO - See if this can be moved to a seperate function to be used 
	// both socket & xio
	con = (struct ceph_connection *)conn->ceph_con;
	BUG_ON(con->state != CON_STATE_OPEN);
	BUG_ON(atomic_read(&con->msgr->stopping));
	BUG_ON(!con->ops->alloc_msg);
	mutex_unlock(&con->mutex);
	m = con->ops->alloc_msg(con, &m_hdr, &skip);
	mutex_lock(&con->mutex);
	//BUG_ON(!m);
	if (m) {
		BUG_ON(skip);
		m->con = con->ops->get(con);
		BUG_ON(m->con == NULL);
	}
	else {
		if (skip) {
			dout("%s: skip message\n", __func__);
			con->in_seq++;
			return 0;
		}
		con->error_msg = "error allocating memory for incoming message";
		mutex_unlock(&con->mutex);

		return -ENOMEM;
	}
	hdlr->msg = m;
	memcpy(&m->hdr, &m_hdr, sizeof(struct ceph_msg_header));
	/* footer */
	ceph_decode_need(&p, end, sizeof(64) + 3 * sizeof(u32) + sizeof(u8), bad);
	m->footer.front_crc = ceph_decode_32(&p);
	m->footer.middle_crc = ceph_decode_32(&p);
	m->footer.data_crc = ceph_decode_32(&p);
	p += sizeof(u64);	// Skip sig as it is not supported by kRBD
	m->footer.flags = ceph_decode_8(&p);
	dout("%s: got msg %p %d (%u) + %d (%u) + %d (%u)\n", __func__,
			m, m_hdr.front_len, m->footer.front_crc,
			m_hdr.middle_len, m->footer.middle_crc,
			m_hdr.data_len, m->footer.data_crc);
	if (m_hdr.middle_len && !m->middle) {
		dout("%s:%d: allocating middle iov\n", __func__,
				__LINE__);
		ret = ceph_alloc_middle(con, m);
		if (ret < 0) {
			ceph_msg_put(m);
			BUG_ON(0);
		}
	}
	if (m && m_hdr.data_len > m->data_length) {
		pr_warning("%s: long message (%u > %zd)\n", __func__,
				m_hdr.data_len, m->data_length);
		ceph_msg_put(m);
		BUG_ON(0);
	}

	/* verify seq# */
	// TODO - Ceph XioMessenger seems to send out of order messages
	// Disabling out of order message drops for now
	seq = le64_to_cpu(m_hdr.seq);
#if 0
	if ((s64)seq - (s64)con->in_seq < 1) {
		pr_info("%s: %s%lld %s seq %lld expected %lld\n",
				__func__, ENTITY_NAME(con->peer_name),
				ceph_pr_addr(&con->peer_addr.in_addr), seq,
				con->in_seq + 1);
		// BUG_ON(0);
	} else if ((s64)seq - (s64)con->in_seq > 1) {
		pr_err("%s: bad seq %lld expected %lld\n", __func__, seq,
				con->in_seq + 1);
		// con->error_msg = "bad message sequence # for incoming message";
		// return -EBADMSG;
	}
#endif
	if (m_hdr.data_len) {
		prepare_message_data(m, m_hdr.data_len);
	}
	/* front */
	BUG_ON(m->front.iov_base == NULL);
	BUG_ON(m->front.iov_len < m_hdr.front_len);
	m->front.iov_len = m_hdr.front_len;
	RDMA_STATS_ADD(rcvd_front_len, m_hdr.front_len);
	if (assign_buffers) {
		ret = ceph_rcvd_msg_add_sg(hdlr, m->front.iov_base, m_hdr.front_len);
	}
	else {
		ceph_rcvd_copy_buffers(hdlr, m->front.iov_base, m_hdr.front_len);
	}
	/* middle */
	if (m->middle) {
		BUG_ON(ret);
		BUG_ON(m->middle->vec.iov_base == NULL);
		BUG_ON(m->middle->vec.iov_len < m_hdr.middle_len);
		m->middle->vec.iov_len = m_hdr.middle_len;
		RDMA_STATS_ADD(rcvd_middle_len, m->middle->vec.iov_len);
		if (assign_buffers) {
			ret = ceph_rcvd_msg_add_sg(hdlr, m->middle->vec.iov_base, m->middle->vec.iov_len);
		}
		else {
			ceph_rcvd_copy_buffers(hdlr, m->middle->vec.iov_base, m->middle->vec.iov_len);
		}
		BUG_ON(ret);
	}
	con->in_seq++;
	/* if first message, set peer_name */
	if (con->peer_name.type == 0)
		con->peer_name = m->hdr.src;
bad:
	return ret;
}

static int ceph_rdma_process_incoming_msg(struct xio_msg *msg,
		struct libceph_rdma_connection *conn, int assign_buffers)
{
	struct xio_rcvd_msg_hdlr *hdlr;
	struct ceph_connection *con;
	struct ceph_msg *m;
	int ret = 0;

	if (assign_buffers) {
		hdlr = &conn->hdlr;
	}
	else {
		hdlr = &conn->inl_hdlr;
	}
	ceph_init_rcvd_msg_hdlr(hdlr, msg, assign_buffers);
	con = (struct ceph_connection *)conn->ceph_con;
	mutex_lock(&con->mutex);
	if (hdlr->msg == NULL) {
		ret = ceph_rdma_process_headers(msg, conn, assign_buffers);
		if (ret == -EBADMSG) {
			return ret;
		}
		if (hdlr->msg_chain_cnt > 1) {
			if (assign_buffers) {
				RDMA_STATS_INC(assign_data_in_buf_first_in_chain);
			}
			else {
				RDMA_STATS_INC(inline_data_first_in_chain);
			}
			dout("%s:%d: Msg cnt %d\n", __func__, __LINE__,
					hdlr->msg_chain_cnt);
		}
	}
	else {
		if (unlikely(assign_buffers == 0)) {
			struct xio_vmsg *imsg;
			printk("msg %p out of order?\n", msg);
			trace_printk("msg %p out of order?\n", msg);
			imsg = &msg->in;
			trace_printk("hdr. res l=%d, res b=%p, sgc=%d, tot=%d, sg off=%d\n", hdlr->residual_len,
					hdlr->residual_buf, hdlr->sg_cnt, hdlr->total_msg_len, hdlr->cur_sg_off);
			trace_printk("MC=%d, last sg=%p, last buf=%p, tbl=%p, sg=%p, m=%p\n", hdlr->msg_chain_cnt,
					hdlr->last_sg, hdlr->last_buf, hdlr->tbl, hdlr->sg, hdlr->msg);
			m = hdlr->msg;
			if (m) {
				trace_printk("DL=%zu, se=%lld, tid=%lld, type=%d, fl=%d, dl=%d\n", m->data_length, m->hdr.seq, m->hdr.tid,
						m->hdr.type, m->hdr.front_len, m->hdr.data_len);
			}
			m = msg->user_context;
			if (m) {
				trace_printk("DL=%zd, se=%lld, tid=%lld, type=%d, fl=%d, dl=%d\n", m->data_length, m->hdr.seq, m->hdr.tid,
						m->hdr.type, m->hdr.front_len, m->hdr.data_len);
			}
			ceph_rdma_dump_xmsg(msg);
			mutex_unlock(&con->mutex);
			BUG_ON(1);
			return 1;
		}
	}

	m = hdlr->msg;
	ceph_rdma_process_data(msg, conn, assign_buffers);
	mutex_unlock(&con->mutex);
	hdlr->msg_chain_cnt--;

	// Do this for the last message in the chain
	if (hdlr->msg_chain_cnt) {
		msg->user_context = NULL;
	}
	else {
		memset(hdlr, 0, sizeof(struct xio_rcvd_msg_hdlr));
		msg->user_context = (void *)m; // No need to lookup msg again
	}

	return ret;
}

static int ceph_rdma_on_new_msg(struct xio_session *session,
		struct xio_msg *msg, int more_in_batch, void *cb_user_context)
{
	struct libceph_rdma_connection *conn = cb_user_context;
	struct ceph_connection *con;
	struct ceph_msg *m;
	bool do_datacrc;
	u32 crc;
	u32 in_front_crc, in_middle_crc = 0, in_data_crc = 0;
	unsigned int data_len;
	struct xio_vmsg *imsg, *omsg;

	dout("%s: rcvd msg %p on sess %p. more = %d, uc=%p\n", __func__,
			msg, session, more_in_batch, cb_user_context);
	//ceph_rdma_dump_xmsg(msg);

	RDMA_STATS_INC(remote_rsp);
	if (msg->out.user_context == (void *)1) {
		m = (struct ceph_msg *)msg->user_context;

		msg->out.user_context = NULL;
		imsg = &msg->in;
		omsg = &msg->out;
		if (imsg->user_context != NULL) {
			sg_free_table(&imsg->data_tbl);
			imsg->data_tbl.sgl = imsg->user_context;
			imsg->data_tbl.nents = imsg->pad;
			imsg->pad = 0;
			imsg->data_tbl.orig_nents = omsg->pad;
			omsg->pad = 0;
		}
		// No processing for non-last message
		if (m == NULL) {
			/* acknowledge XIO that response is no longer needed */
			xio_release_msg(msg);
			return 0;
		}
		RDMA_STATS_INC(remote_rsp_last_msg);
	}
	else {
		RDMA_STATS_INC(msg_w_inline_data);
		if (ceph_rdma_process_incoming_msg(msg, conn, 0)) {
			trace_printk("Dropping message %p\n", msg);
			xio_release_msg(msg);
		}
		m = (struct ceph_msg *)msg->user_context;
	}

	// We have already verified the header CRC
	// Can skip this for perf
	crc = crc32c(0, &m->hdr, offsetof(struct ceph_msg_header, crc));
	if (cpu_to_le32(crc) != m->hdr.crc) {
		// pr_err("%s:bad hdr crc %u != expected %u\n", __func__, crc, m->hdr.crc);
		// BUG_ON(0);;
	}

	/* crc ok? */
	in_front_crc = crc32c(0, m->front.iov_base, m->front.iov_len);
	if (in_front_crc != le32_to_cpu(m->footer.front_crc)) {
		// pr_err("%s: %p front crc %u != exp. %u\n", __func__, m, in_front_crc, m->footer.front_crc);
		// BUG_ON(0);;
	}
	if (m->middle) {
		in_middle_crc = crc32c(0, m->middle->vec.iov_base,
				m->middle->vec.iov_len);
		if (in_middle_crc != le32_to_cpu(m->footer.middle_crc)) {
			// pr_err("%s: %p middle crc %u != exp %u\n", __func__, m, in_middle_crc, m->footer.middle_crc);
			// return -EBADMSG;
		}
	}
	// TODO Check connection state
	con = (struct ceph_connection *)conn->ceph_con;
	mutex_lock(&con->mutex);
	BUG_ON(con->state != CON_STATE_OPEN);
	do_datacrc = !con->msgr->nocrc;

	data_len = le32_to_cpu(m->hdr.data_len);
	if (data_len && do_datacrc && ((m->footer.flags & CEPH_MSG_FOOTER_NOCRC) == 0)) {
		u32 crc;
		struct ceph_msg_data_cursor *cursor;
		crc = in_data_crc = 0;
		in_data_crc = crc;
		BUG_ON(list_empty(&m->data));
		prepare_message_data(m, m->data_length);
		cursor = &m->cursor;
		while (cursor->resid) {
			struct page *page;
			size_t page_offset;
			size_t length;
			bool last_piece;

			page = ceph_msg_data_next(&m->cursor,
					&page_offset, &length,
					&last_piece);
			if (do_datacrc && cursor->need_crc) {
				crc = ceph_crc32c_page(crc, page, page_offset, length);
			}
			ceph_msg_data_advance(&m->cursor, (size_t)length);
		}
		if (in_data_crc != le32_to_cpu(m->footer.data_crc)) {
			// pr_err("%s: %p data crc %u != exp. %u\n", __func__, m, in_data_crc, le32_to_cpu(m->footer.data_crc));
			// return -EBADMSG;
		}
	}
	ceph_xio_unmap_data_pages(m);
	con->ops->put(con);
	mutex_unlock(&con->mutex);

	dout("===== %p %llu from %s%lld %d=%s len %d+%d (%u %u %u) =====\n",
	     m, le64_to_cpu(m->hdr.seq),
	     ENTITY_NAME(m->hdr.src),
	     le16_to_cpu(m->hdr.type),
	     ceph_msg_type_name(le16_to_cpu(m->hdr.type)),
	     le32_to_cpu(m->hdr.front_len),
	     le32_to_cpu(m->hdr.data_len),
	     in_front_crc, in_middle_crc, in_data_crc);
#if DEBUG_LVL == DEBUG_DATA_PATH
	trace_printk("===== %p %llu from %s%lld %d=%s len %d+%d (%u %u %u) =====\n",
	     m, le64_to_cpu(m->hdr.seq),
	     ENTITY_NAME(m->hdr.src),
	     le16_to_cpu(m->hdr.type),
	     ceph_msg_type_name(le16_to_cpu(m->hdr.type)),
	     le32_to_cpu(m->hdr.front_len),
	     le32_to_cpu(m->hdr.data_len),
	     in_front_crc, in_middle_crc, in_data_crc);
#endif

	/* acknowledge XIO that response is no longer needed */
	xio_release_msg(msg);

	con->ops->dispatch(con, m);

	return 0;
}

static int ceph_rdma_new_session(struct xio_session *session,
		struct xio_new_session_req *req, void *cb_user_context)
{
	struct libceph_rdma_session *sess;
	sess = (struct libceph_rdma_session *)cb_user_context;

	RDMA_STATS_INC(new_sess);
#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("%s: new session %p, req=%p, uc=%p\n", __func__, session,
			req, cb_user_context);
#endif

	return 0;
}

static int ceph_rdma_on_session_established(struct xio_session *session,
		struct xio_new_session_rsp *rsp, void *cb_user_context)
{
	struct libceph_rdma_session *sess;
	sess = (struct libceph_rdma_session *)cb_user_context;

	RDMA_STATS_INC(sess_estab);
#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("%s: sess %p/%p established rsp = %p\n", __func__,
			session, sess, rsp);
#endif

	return 0;
}

/* send completion notification */
static int ceph_rdma_on_msg_send_complete(struct xio_session *session,
		struct xio_msg *msg, void *conn_user_context)
{
	RDMA_STATS_INC(msg_send_comp);
#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("%s: msg %p send comp. sess %p, uc=%p\n", __func__, msg, session,
		conn_user_context);
#endif

	return 0;
}

static int ceph_rdma_on_msg_delivered(struct xio_session *session,
		struct xio_msg *msg, int more_in_batch,
		void *conn_user_context)
{
	RDMA_STATS_INC(msg_delivered);
#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("%s: msg %p delivered. sess %p, more = %d, uc=%p\n", __func__,
			msg, session, more_in_batch, conn_user_context);
#endif
	ceph_rdma_dump_xmsg(msg);
	return 0;
}

static int ceph_rdma_on_msg_error(struct xio_session *session, enum xio_status error,
		enum xio_msg_direction dir, struct xio_msg  *msg,
		void *conn_user_context)
{
	struct libceph_rdma_connection *conn = conn_user_context;

	RDMA_STATS_INC(msg_error);
	trace_printk("%s: msg %p error. sess %p, error = %d, dir = %d\n",
			__func__, msg, session, error, dir);
	trace_printk("conn %p, q cnt: %d, pend cnt: %d\n", conn,
			conn->queued_cnt, conn->pending_cnt);
	//ceph_rdma_dump_xmsg(msg);
	
	conn->queued_cnt--;
	ceph_xio_cleanup_msg(conn, msg, 0);
	return 0;
}

/* requester's message cancelation notification */
static int ceph_rdma_on_cancel(struct xio_session *session, struct xio_msg *msg,
	enum xio_status result, void *conn_user_context)
{
	RDMA_STATS_INC(msg_cancel);
#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("%s: msg %p cancel req. res = %d, sess %p, uc=%p\n", __func__, msg,
		result, session, conn_user_context);
#endif
	ceph_rdma_dump_xmsg(msg);
	return 0;
}

/* responder's message cancelation notification */
static int ceph_rdma_on_cancel_request(struct xio_session *session, struct xio_msg *msg,
	void *conn_user_context)
{
	RDMA_STATS_INC(msg_cancel_req);
#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("%s: msg %p cancel req. sess %p, uc=%p\n", __func__, msg, session,
		conn_user_context);
#endif
	ceph_rdma_dump_xmsg(msg);
	return 0;
}

static void ceph_rdma_validate_out_msg(struct xio_msg *msg, int tot_len,
	struct xio_rcvd_msg_hdlr *old_hdlr, struct xio_rcvd_msg_hdlr *hdlr)
{
	struct xio_vmsg *vmsg;
	struct sg_table *data_tbl;
	int i, calc_len = 0;
	struct scatterlist *sg;
	int err = 0;

	vmsg = &msg->in;
	data_tbl = &vmsg->data_tbl;
	if (data_tbl->nents == 0) {
		printk("%s: Err nexts == 0. m=%p\n", __func__, msg);
		err = 1;
	}
	else {
        	sg = data_tbl->sgl;
		calc_len = 0;
		for (i = 0; i < data_tbl->nents; i++) {
			if (sg->length == 0 || sg_virt(sg) == NULL) {
				printk("%s: Inv SG, sg = %p %d: %d %p\n", __func__, sg, i, sg->length, sg_virt(sg));
				err = 1;
				break;
			}
			calc_len += sg->length;
			sg = sg_next(sg);
		}
		if (tot_len != calc_len) {
			printk("%s: mismatch len. exp %d, act %d\n", __func__, tot_len,
				calc_len);
			err = 1;
		}
	}

	if (err) {
		printk("%s: tot len=%d\n", __func__, tot_len);
		printk("old hdr. res l=%d, res b=%p, sgc=%d, tot=%d, sg off=%d\n", old_hdlr->residual_len,
			old_hdlr->residual_buf, old_hdlr->sg_cnt, old_hdlr->total_msg_len, old_hdlr->cur_sg_off);
		printk("MC=%d, last sg=%p, last buf=%p, tbl=%p, sg=%p, m=%p\n", old_hdlr->msg_chain_cnt, old_hdlr->last_sg,
			old_hdlr->last_buf, old_hdlr->tbl, old_hdlr->sg, old_hdlr->msg);
		printk("new hdr. res l=%d, res b=%p, sgc=%d, tot=%d, sg off=%d\n", hdlr->residual_len,
			hdlr->residual_buf, hdlr->sg_cnt, hdlr->total_msg_len, hdlr->cur_sg_off);
		printk("MC=%d, last sg=%p, last buf=%p, tbl=%p, sg=%p, m=%p\n", hdlr->msg_chain_cnt, hdlr->last_sg,
			hdlr->last_buf, hdlr->tbl, hdlr->sg, hdlr->msg);
	}
	return;
}

/* notify the user to assign a data buffer for incoming read */
static int ceph_rdma_assign_data_in_buf(struct xio_msg *msg,
		void *cb_user_context)
{
	struct libceph_rdma_connection *conn = cb_user_context;
	void *prev_ctx = NULL;
	int tot_len = 0, i;
	struct xio_vmsg *imsg;
	struct scatterlist *sg;
	struct xio_rcvd_msg_hdlr old_hdlr;
	int ret = 0;

	dout("%s: data buf req for msg %p, uc=%p\n", __func__, msg,
			cb_user_context);
	//ceph_rdma_dump_xmsg(msg);
	RDMA_STATS_INC(assign_data_in_buf);

	imsg = &msg->in;
	tot_len = 0;
	for_each_sg(imsg->data_tbl.sgl, sg, imsg->data_tbl.nents, i) {
		tot_len += sg->length;
	}
	old_hdlr = conn->hdlr;
	ret = ceph_rdma_process_incoming_msg(msg, conn, 1);

	prev_ctx = msg->out.user_context;
	msg->out.user_context = (void *)1;
	ceph_rdma_validate_out_msg(msg, tot_len, &old_hdlr, &conn->hdlr);

	//printk("AD: M-%p ONE-%d MNE=%d NNE-%d T=%d F-%d M-%d D-%d\n", m, old_nents, orig_nents, imsg->data_tbl.nents, m->hdr.type,
	//	front_len, middle_len, data_len);

	// ceph_rdma_dump_xmsg(msg);
	return ret ? 1 : 0;
}

static void ceph_rdma_trigger_send_on_conn(void *data)
{
	struct libceph_rdma_connection *conn = (struct libceph_rdma_connection *)data;
	ceph_rdma_try_sending(conn, 0);
}

/* sender's send completion notification - one way message only */
static int ceph_rdma_on_ow_msg_send_complete(struct xio_session *session,
		struct xio_msg *msg, void *conn_user_context)
{
	struct libceph_rdma_connection *conn;

	RDMA_STATS_INC(ow_send_comp);
	dout("%s: ow msg %p send comp. sess=%p, uc=%p\n",
			__func__, msg, session, conn_user_context);
	conn = (struct libceph_rdma_connection *)conn_user_context;
	conn->last_sent_sn = msg->sn;
	conn->queued_cnt--;
	ceph_xio_cleanup_msg(conn, msg, 1);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* callbacks                                                                 */
/*---------------------------------------------------------------------------*/
struct xio_session_ops ceph_rdma_ses_ops = {
	.on_session_event               =  ceph_rdma_on_session_event,
	.on_new_session                 =  ceph_rdma_new_session,
	.on_session_established         =  ceph_rdma_on_session_established,
	.on_msg_send_complete		=  ceph_rdma_on_msg_send_complete,
	.on_msg                         =  ceph_rdma_on_new_msg,
	.on_msg_delivered		=  ceph_rdma_on_msg_delivered,
	.on_msg_error                   =  ceph_rdma_on_msg_error,
	.on_cancel			=  ceph_rdma_on_cancel,
	.on_cancel_request		=  ceph_rdma_on_cancel_request,
	.assign_data_in_buf		=  ceph_rdma_assign_data_in_buf,
	.on_ow_msg_send_complete	=  ceph_rdma_on_ow_msg_send_complete,
};

static void ceph_rdma_session_free(struct libceph_rdma_session *sess)
{
	if (sess->ctx) {
		xio_context_destroy(sess->ctx);
	}
	mutex_lock(&g_libceph_lock);
	list_del(&sess->list);
	mutex_unlock(&g_libceph_lock);
	RDMA_STATS_INC(sess_free);
	kfree(sess);

	return;
}

static int ceph_rdma_session_work(void *data)
{
	struct libceph_rdma_session *sess = data;
	struct xio_connection_params cparams;
	struct libceph_rdma_connection *conn;

	conn = sess->rdma_conn;
	sess->ctx = xio_context_create(XIO_LOOP_GIVEN_THREAD, NULL, current, 0, /* cpu id  - get_cpu() */ -1);
	if (sess->ctx == NULL) {
		printk("context open failed\n");
		goto cleanup;
	}
        memset(&cparams, 0, sizeof(cparams));
        cparams.session                 = sess->session;
        cparams.ctx                     = sess->ctx;
        cparams.conn_user_context       = conn;
	conn->xio_conn = xio_connect(&cparams);
	if (conn->xio_conn == NULL) {
		printk("connection open failed\n");
		goto cleanup;
	}
	dout("%s: alloc usr con %p for xio con %p, ctx %p(sess %p)\n", __func__,
		conn, conn->xio_conn, sess->ctx, sess);
	xio_context_run_loop(sess->ctx);

cleanup:
	/* Destroy the session when the loop stops */
	ceph_rdma_session_free(sess);
	do_exit(0);
}

static struct libceph_rdma_session *ceph_rdma_session_create(const char *portal)
{
	struct libceph_rdma_session *sess;
	struct xio_session_params params;

	sess = kzalloc(sizeof(struct libceph_rdma_session), GFP_KERNEL);
	if (!sess) {
		printk("failed to allocate libceph rdma session\n");
		return NULL;
	}
	strcpy(sess->portal, portal);
	kref_init(&sess->kref);

	memset(&params, 0, sizeof(params));
	params.type             = XIO_SESSION_CLIENT;
	params.ses_ops          = &ceph_rdma_ses_ops;
	params.user_context     = sess;
	params.uri              = sess->portal;
	sess->session = xio_session_create(&params);
	if (sess->session == NULL) {
		printk("session creation failed\n");
		kfree(sess);
		return NULL;
	}
	dout("%s:allocated user sess %p for xio sess %p\n", __func__, sess,
			sess->session);
	INIT_LIST_HEAD(&sess->conn_list);
	mutex_init(&sess->conn_list_lock);
	mutex_lock(&g_libceph_lock);
	list_add(&sess->list, &g_libceph_sessions);
	mutex_unlock(&g_libceph_lock);
	RDMA_STATS_INC(sess_create);

	return sess;
}

#if 0
static struct libceph_rdma_session *ceph_rdma_session_find_by_portal(struct list_head *s_data_list, const char *portal)
{
	struct libceph_rdma_session *pos;
	struct libceph_rdma_session *ret = NULL;

	mutex_lock(&g_libceph_lock);
	list_for_each_entry(pos, &g_libceph_sessions, list) {
		if (!strcmp(pos->portal, portal)) {
			ret = pos;
			break;
		}
	}
	mutex_unlock(&g_libceph_lock);

	return ret;
}
#endif

void libceph_rdma_session_last_put(struct kref *kref)
{
	struct libceph_rdma_session *sess = container_of(kref, struct libceph_rdma_session, kref);

	RDMA_STATS_INC(sess_destroy);
	BUG_ON(list_empty(&sess->conn_list) == 0);

	/* Remove from global session list */
	mutex_lock(&g_libceph_lock);
	list_del(&sess->list);
	mutex_unlock(&g_libceph_lock);
#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("Destroying session %p/%p\n", sess->session, sess);
#endif
	xio_session_destroy(sess->session);
	sess->session = NULL;
}

static int ceph_rdma_connect(struct ceph_connection *con)
{
	struct sockaddr_storage *paddr = &con->peer_addr.in_addr;
	char rdma[MAX_PORTAL_NAME];
	struct libceph_rdma_session *sess = NULL;
	struct libceph_rdma_connection *conn = NULL;
	char name[50];

	RDMA_STATS_INC(open_conn);
	snprintf(rdma, MAX_PORTAL_NAME, "rdma://%s", ceph_pr_addr(paddr));
	if (sess == NULL) {
		sess = ceph_rdma_session_create(rdma);
		if (sess == NULL) {
			printk("Couldn't create new session with %s\n", rdma);
			return -EINVAL;
		}
	}

	/*TODO:  Is there a race condition between session create and connection create? */
	conn = kzalloc(sizeof(struct libceph_rdma_connection), GFP_KERNEL);
	if (conn == NULL) {
		printk("failed to allocate connection");
		return -ENOMEM;
	}
	conn->session = sess;
	mutex_lock(&sess->conn_list_lock);
	list_add_tail(&conn->list, &sess->conn_list);
	atomic_inc(&sess->conn_count);
	mutex_unlock(&sess->conn_list_lock);
	conn->addr = paddr;
	con->msngr_pvt = (void *)conn; //TODO
	conn->ceph_con = con;
	INIT_LIST_HEAD(&conn->pending_msg_list);
	conn->queued_cnt = 0;
	conn->pending_cnt = 0;
	libceph_rdma_session_get(conn->session);
	sess->rdma_conn = conn;
	sprintf(name, "rdma sess thread %p", sess); 
	sess->sess_th = kthread_run(ceph_rdma_session_work, sess, name);
	dout("%s:connecting %s\n", __func__, ceph_pr_addr(paddr));

	return 0;
}

static int ceph_rdma_open(struct ceph_connection *con)
{
	int ret;

	mutex_lock(&con->mutex);
	BUG_ON(con->state != CON_STATE_PREOPEN);
	dout("%s: con %p PREOPEN\n", __func__, con);
	BUG_ON(con->msngr_pvt);
	con->state = CON_STATE_CONNECTING;

	dout("initiating connect on %p new state %lu\n",
			con, con->state);
#if DEBUG_LVL == DEBUG_SLOW_PATH
	printk("%s: con %p PREOPEN\n", __func__, con);
	printk("initiating connect on %p new state %lu\n",
			con, con->state);
#endif
	ret = ceph_rdma_connect(con);
	if (ret < 0) {
		dout("rdma connection failed with ret %d\n", ret);
		con->error_msg = "connect error";
	}
	mutex_unlock(&con->mutex);

	return 0;
}

static int ceph_rdma_out_msg_cancel(struct ceph_msg *m)
{
#if 0
	struct xio_msg *msg;
	struct ceph_connection *ceph_con;
	struct libceph_rdma_connection *conn;

	trace_printk("msg = %p, ctx = %p \n", m, m->msgr_ctx);
	ceph_con = m->con;
	conn = (struct libceph_rdma_connection *)ceph_con->msngr_pvt;
	if (m->msgr_ctx) {
		for (msg = (struct xio_msg *)m->msgr_ctx; msg; msg = msg->next) {
			trace_printk("Issuing cancel for xio msg %p\n", msg);
			xio_cancel_request(conn->xio_conn, msg);
		}
	}
#endif
	// TODO
	dout("%s:%d msg = %p returning for RDMA\n", __func__, __LINE__, m);
	return 0;
}

static int ceph_rdma_in_msg_cancel(struct ceph_msg *m)
{
	// TODO
	dout("%s:%d msg = %p returning for RDMA\n", __func__, __LINE__, m);
	return 0;
}

static int ceph_rdma_send_keepalive(struct ceph_connection *con)
{
	// TODO
	dout("%s:%d: Ignoring keepalive request!!\n", __func__, __LINE__);
	return 0;
}

static struct ceph_messenger_template ceph_rdma_xio_messenger = {
	.name = "xio",
	.open_connection = ceph_rdma_open,
	.close_connection = ceph_xio_close_conn,
	.queue_msg = ceph_rdma_send,
	.cancel_out_msg = ceph_rdma_out_msg_cancel,
	.cancel_in_msg = ceph_rdma_in_msg_cancel,
	.send_keepalive = ceph_rdma_send_keepalive,
};

static int __init ceph_xio_msgr_init(void)
{
	int xopt;

	dout("INIT: ceph hdr sz = %zu, ceph ftr sz=%zu\n",
			sizeof(struct ceph_msg_header), sizeof(struct ceph_msg_footer));
	mutex_init(&g_libceph_lock);
	INIT_LIST_HEAD(&g_libceph_sessions);

	/* enable flow-control */
	xopt = 1;
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_ENABLE_FLOW_CONTROL,
			&xopt, sizeof(xopt));

	xopt = XIO_MAX_SEND_MSGS;
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_SND_QUEUE_DEPTH_MSGS,
			&xopt, sizeof(xopt));
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_RCV_QUEUE_DEPTH_MSGS,
			&xopt, sizeof(xopt));

	xopt = XIO_MAX_SEND_BYTES;
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_SND_QUEUE_DEPTH_BYTES,
			&xopt, sizeof(xopt));
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_RCV_QUEUE_DEPTH_BYTES,
			&xopt, sizeof(xopt));

	xopt = XIO_MSGR_IOVLEN;
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_OUT_IOVLEN,
			&xopt, sizeof(xopt));
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_IN_IOVLEN,
			&xopt, sizeof(xopt));

	xopt = XIO_HDR_LEN;
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_INLINE_HEADER,
			&xopt, sizeof(xopt));
	xopt = 16384;
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_INLINE_DATA,
			&xopt, sizeof(xopt));

	proc_create("xio_stats", 0, NULL, &xio_stats_proc_fops);

	register_reboot_notifier(&ceph_rdma_xio_reboot_notifier);

	ceph_rdma_header_buf_cache = kmem_cache_create("ceph_rdma_hdr_buf", XIO_HDR_LEN,
		__alignof__(XIO_HDR_LEN), 0, NULL);
	if (ceph_rdma_header_buf_cache == NULL) {
		printk("%s:%d: Header buffer cache alloc failed\n", __func__, __LINE__);
		return -ENOMEM;
	}
	BUG_ON(ceph_rdma_msg_cache != NULL);
	ceph_rdma_msg_cache = kmem_cache_create("ceph_rdma_msg_pool", sizeof(struct xio_msg),
		__alignof__(sizeof(struct xio_msg)), 0, NULL);
	if (ceph_rdma_msg_cache == NULL) {
		printk("%s:%d: Msg buffer cache alloc failed\n", __func__, __LINE__);
		return -ENOMEM;
	}
	ceph_register_new_messenger(&ceph_rdma_xio_messenger);

	pr_info("loaded xio messenger\n");

	return 0;
}

static void __exit ceph_xio_msgr_exit(void)
{
	printk("%s:%d: Unloading modules\n", __func__, __LINE__);
	ceph_unregister_messenger(&ceph_rdma_xio_messenger);
	if (ceph_rdma_header_buf_cache) {
		kmem_cache_destroy(ceph_rdma_header_buf_cache);
	}
	if (ceph_rdma_msg_cache) {
		kmem_cache_destroy(ceph_rdma_msg_cache);
	}
	remove_proc_entry("xio_stats", NULL);

	return;
}

module_init(ceph_xio_msgr_init);
module_exit(ceph_xio_msgr_exit);

MODULE_AUTHOR("Raju Kurunkad <raju.kurunkad@sandisk.com>");
MODULE_DESCRIPTION("XIO messenger for CEPH");
MODULE_LICENSE("GPL");
