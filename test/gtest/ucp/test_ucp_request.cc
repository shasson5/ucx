/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2021. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include <string.h>
#include "ucp_test.h"
#include <common/mem_buffer.h>
extern "C" {
#include <ucp/core/ucp_request.inl>
#include <ucp/core/ucp_worker.h>
#include <ucp/proto/proto_common.h>
}


class test_ucp_request : public ucp_test {
public:
    virtual void init()
    {
        ucp_test::init();
        sender().connect(&receiver(), get_ep_params());
        int mem_type_pair_index = get_variant_value() %
                                  mem_buffer::supported_mem_types().size();
        m_mem_type              =
                mem_buffer::supported_mem_types()[mem_type_pair_index];
    }

    static void get_test_variants(std::vector<ucp_test_variant> &variants)
    {
        int count = 0;
        add_variant(variants, UCP_FEATURE_TAG);

        for (auto mem_type : mem_buffer::supported_mem_types()) {
            std::string name = ucs_memory_type_names[mem_type];
            add_variant_with_value(variants, UCP_FEATURE_TAG, count, name);
            ++count;
        }
    }

    static const size_t msg_size = 4;

protected:
    ucs_memory_type_t m_mem_type;
};


UCS_TEST_P(test_ucp_request, test_request_query)
{
    ucp_request_param_t param;
    ucp_request_attr_t attr;
    ucp_worker_attr_t worker_attr;
    void *reqs[2];

    mem_buffer m_recv_mem_buf(msg_size, m_mem_type);
    mem_buffer m_send_mem_buf(msg_size, m_mem_type);

    param.op_attr_mask = UCP_OP_ATTR_FLAG_NO_IMM_CMPL;

    void *sreq = ucp_tag_send_nbx(sender().ep(), m_send_mem_buf.ptr(), msg_size,
                                  0, &param);
    ASSERT_TRUE(UCS_PTR_IS_PTR(sreq));
    reqs[0] = sreq;

    void *rreq = ucp_tag_recv_nbx(receiver().worker(), m_recv_mem_buf.ptr(),
                                  msg_size, 0, 0, &param);
    ASSERT_TRUE(UCS_PTR_IS_PTR(rreq));
    reqs[1] = rreq;

    while ((ucp_request_check_status(sreq) == UCS_INPROGRESS) ||
           (ucp_request_check_status(rreq) == UCS_INPROGRESS)) {
        progress();
    }

    worker_attr.field_mask = UCP_WORKER_ATTR_FIELD_MAX_INFO_STRING;
    ucp_worker_query(receiver().worker(), &worker_attr);

    char debug_string[worker_attr.max_debug_string];
    memset(&debug_string, 0, worker_attr.max_debug_string);

    attr.field_mask        = UCP_REQUEST_ATTR_FIELD_INFO_STRING      |
                             UCP_REQUEST_ATTR_FIELD_INFO_STRING_SIZE |
                             UCP_REQUEST_ATTR_FIELD_MEM_TYPE         |
                             UCP_REQUEST_ATTR_FIELD_STATUS;
    attr.debug_string      = debug_string;
    attr.debug_string_size = worker_attr.max_debug_string;

    for (int i = 0; i < 2; i++) {
        const char *req_type = (i == 0) ? "send" : "recv";
        ucp_request_query(UCS_STATUS_PTR(reqs[i]), &attr);
        UCS_TEST_MESSAGE << req_type << " req: " << attr.debug_string;
        std::string str(attr.debug_string);
        EXPECT_GT(str.size(), 0);
        EXPECT_NE(str.find(req_type), std::string::npos);
        EXPECT_NE(str.find(ucs_memory_type_names[m_mem_type]),
                  std::string::npos);
        ASSERT_EQ(attr.status, UCS_OK);
        ASSERT_EQ(attr.mem_type, m_mem_type);

        ucp_request_free(reqs[i]);
    }
}

UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_request, all, "all")

class test_proto_reset : public ucp_test {
public:
    typedef enum {
        TAG,
        RMA_GET,
        RMA_PUT,
        STREAM,
        AM
    } operation_t;

    class request_pair_t {
    public:
        request_pair_t(void *user_sreq, void *user_rreq)
        {
            sreq = (ucp_request_t*)user_sreq - 1;
            rreq = (ucp_request_t*)user_rreq - 1;
        }

        static void *to_user(ucp_request_t *req)
        {
            return req + 1;
        }

        bool operator==(const request_pair_t &other) const
        {
            return (sreq == other.sreq) && (rreq == other.rreq);
        }

        ucp_request_t *sreq;
        ucp_request_t *rreq;
    };

    const request_pair_t invalid_pair = {(void*)0xdeadbeef, (void*)0xdeadbeef};

    test_proto_reset() : m_completed(false), m_am_cb_cnt(0)
    {
    }

    void init() override
    {
        if (!m_ucp_config->ctx.proto_enable) {
            UCS_TEST_SKIP_R("reset is not supported for proto v1");
        }

        if (is_self()) {
            UCS_TEST_SKIP_R("self transport has no pending queue");
        }

        ucp_test::init();
        modify_config("TCP_SNDBUF", "8K", IGNORE_IF_NOT_EXIST);
        modify_config("IB_TX_QUEUE_LEN", "65", IGNORE_IF_NOT_EXIST);
        modify_config("MM_FIFO_SIZE", "64", IGNORE_IF_NOT_EXIST);
        create_entity(true);
        create_entity();

        sender().connect(&receiver(), get_ep_params());
        receiver().connect(&sender(), get_ep_params());
    }

    void cleanup() override
    {
        m_rkeys.clear();
        m_rbufs.clear();
        ucp_test::cleanup();
    }

    void get_stream_data(mapped_buffer &rbuf)
    {
        size_t roffset            = 0;
        ucp_request_param_t param = {0};
        constexpr double timeout  = 10;
        const ucs_time_t deadline = ucs::get_deadline(timeout);
        size_t length;
        ucs_status_ptr_t request;
        ucs_status_t status;

        param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
        param.datatype     = ucp_dt_make_contig(1);

        do {
            progress();
            request = ucp_stream_recv_nbx(receiver().ep(),
                                          (uint8_t*)rbuf.ptr() + roffset,
                                          rbuf.size() - roffset, &length,
                                          &param);
            ASSERT_FALSE(UCS_PTR_IS_ERR(request));

            if (UCS_PTR_IS_PTR(request)) {
                do {
                    progress();
                    status = ucp_stream_recv_request_test(request, &length);

                } while ((status == UCS_INPROGRESS) &&
                         (ucs_get_time() < deadline));
                ASSERT_UCS_OK(status);
                ucp_request_free(request);
            }

            roffset += length;
        } while (roffset < rbuf.size());
    }

    static ucs_status_t
    am_data_cb(void *arg, const void *header, size_t header_length, void *data,
               size_t length, const ucp_am_recv_param_t *param)
    {
        test_proto_reset *self = (test_proto_reset*)arg;

        ucs_assert(length == self->m_rbufs[self->m_am_cb_cnt]->size());
        memcpy(self->m_rbufs[self->m_am_cb_cnt]->ptr(), data, length);
        self->m_am_cb_cnt++;
        return UCS_OK;
    }

    void *send_am(std::vector<uint8_t> &sbuf)
    {
        ucp_request_param_t req_param = {0};
        static unsigned am_id         = 1;
        ucp_am_handler_param_t param;
        void *sreq;

        param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                           UCP_AM_HANDLER_PARAM_FIELD_CB |
                           UCP_AM_HANDLER_PARAM_FIELD_ARG;
        param.id         = am_id;
        param.cb         = am_data_cb;
        param.arg        = this;

        ucs_status_t status;
        status = ucp_worker_set_am_recv_handler(receiver().worker(), &param);
        ASSERT_UCS_OK(status);

        sreq = ucp_am_send_nbx(sender().ep(), am_id, NULL, 0, sbuf.data(),
                               sbuf.size(), &req_param);
        am_id++;
        return sreq;
    }

    static void flushed_cb(ucp_request_t *request)
    {
        test_proto_reset *self = static_cast<test_proto_reset*>(
                request->user_data);

        self->m_completed = true;
        ucp_request_complete_send(request, request->status);
    }

    static void purge_enqueue_cb(uct_pending_req_t *uct_req, void *arg)
    {
        ucp_request_t *req = ucs_container_of(uct_req, ucp_request_t, send.uct);
        test_proto_reset *self;

        self = static_cast<test_proto_reset*>(arg);
        self->m_pending.push_back(req);
    }

    void restart(ucp_ep_h ep)
    {
        ucp_request_param_t param;
        param.op_attr_mask = UCP_OP_ATTR_FIELD_USER_DATA |
                             UCP_OP_ATTR_FIELD_CALLBACK,
        param.user_data    = this;
        param.cb.send      = (ucp_send_nbx_callback_t)ucs_empty_function;

        ucp_ep_purge_lanes(ep, purge_enqueue_cb, this);
        void *request = ucp_ep_flush_internal(ep, 0, &param, NULL, flushed_cb,
                                              "ep_restart");

        ASSERT_FALSE(UCS_PTR_IS_ERR(request));
        if (request != NULL) {
            wait_for_value(&m_completed, true);
            ASSERT_EQ(m_completed, true);
            ucp_request_release(request);
        }

        unsigned restart_count = 0;

        UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(ep->worker);
        for (auto &req : m_pending) {
            ucp_proto_request_restart(req);
            restart_count++;
        }
        UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(ep->worker);

        EXPECT_GT(restart_count, 0);
    }

    void send_nb(std::vector<uint8_t> &sbuf, mapped_buffer *rbuf,
                 operation_t op, bool sync, std::vector<request_pair_t> &pairs)
    {
        ucp_request_param_t param = {0};
        void *rreq                = NULL;
        void *sreq                = NULL;
        ucs::fill_random(sbuf);

        switch (op) {
        case TAG:
            param.op_attr_mask = UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
            sreq               = (sync ? ucp_tag_send_sync_nbx :
                                                       ucp_tag_send_nbx)(sender().ep(), sbuf.data(),
                                                           sbuf.size(), 0, &param);
            rreq = ucp_tag_recv_nbx(receiver().worker(), rbuf->ptr(),
                                    rbuf->size(), 0, 0, &param);
            ASSERT_FALSE(UCS_PTR_IS_ERR(rreq));
            break;
        case RMA_GET:
            param.op_attr_mask = UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
            m_rkeys.push_back(rbuf->rkey(sender()));
            sreq = ucp_get_nbx(sender().ep(), sbuf.data(), sbuf.size(),
                               (uint64_t)rbuf->ptr(), m_rkeys.back(), &param);
            break;
        case RMA_PUT:
            m_rkeys.push_back(rbuf->rkey(sender()));
            sreq = ucp_put_nbx(sender().ep(), sbuf.data(), sbuf.size(),
                               (uint64_t)rbuf->ptr(), m_rkeys.back(), &param);
            break;
        case STREAM:
            param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
            param.datatype     = ucp_dt_make_contig(1);
            sreq = ucp_stream_send_nbx(sender().ep(), sbuf.data(), sbuf.size(),
                                       &param);
            break;
        case AM:
            sreq = send_am(sbuf);
            break;
        }

        ASSERT_FALSE(UCS_PTR_IS_ERR(sreq));
        pairs.push_back({sreq, rreq});
    }

    void wait_recv(operation_t op, std::vector<request_pair_t> &pairs)
    {
        if (op == STREAM) {
            for (unsigned i = 0; i < pairs.size(); ++i) {
                get_stream_data(*m_rbufs[i].get());
            }
        } else if (op == AM) {
            wait_for_value(&m_am_cb_cnt, pairs.size());
            ASSERT_EQ(m_am_cb_cnt, pairs.size());
        }

        std::vector<void*> sreqs, rreqs;
        for (auto &pair : pairs) {
            sreqs.push_back(request_pair_t::to_user(pair.sreq));
            rreqs.push_back(request_pair_t::to_user(pair.rreq));
        }

        requests_wait(sreqs);
        requests_wait(rreqs);
    }

    void send_requests(unsigned reqs_count, std::vector<request_pair_t> &pairs,
                       operation_t op, bool sync)
    {
        pairs.clear();
        m_am_cb_cnt = 0;

        for (int i = 0; i < reqs_count; ++i) {
            send_nb(m_sbufs[i], m_rbufs[i].get(), op, sync, pairs);
        }
    }

    virtual void wait_and_restart(const std::vector<request_pair_t> &pairs)
    {
        auto &pair = wait_for_condition(pairs, [](const request_pair_t &pair) {
            if (request_pair_t::to_user(pair.sreq) == NULL) {
                return false;
            }

            ucp_datatype_iter_t *dt_iter = &pair.sreq->send.state.dt_iter;
            return (dt_iter->offset > 0) && (dt_iter->offset < dt_iter->length);
        });

        ASSERT_FALSE(invalid_pair == pair);
        restart(sender().ep());
    }

    typedef std::function<bool(const request_pair_t &pair)> predicate_t;

    const request_pair_t &
    wait_for_condition(const std::vector<request_pair_t> &pairs,
                       const predicate_t &predicate)
    {
        const double timeout      = 10;
        const ucs_time_t deadline = ucs::get_deadline(timeout);

        while (ucs_get_time() < deadline) {
            for (auto &pair : pairs) {
                if (predicate(pair)) {
                    return pair;
                }
            }

            progress();
        }

        return invalid_pair;
    }

    void reset_protocol(operation_t op, bool sync = false,
                        unsigned reqs_count = 1000)
    {
        static const size_t msg_size = UCS_KBYTE * 70;

        for (int i = 0; i < reqs_count; ++i) {
            mapped_buffer *rbuf = new mapped_buffer(msg_size, receiver());
            rbuf->memset(0);
            m_rbufs.push_back(std::unique_ptr<mapped_buffer>(rbuf));
            m_sbufs.push_back(std::vector<uint8_t>(msg_size));
        }

        /* Send a single message to complete wireup before sending actual
           data */
        std::vector<request_pair_t> pairs;
        send_requests(1, pairs, op, sync);
        wait_recv(op, pairs);

        /* Send all messages */
        send_requests(reqs_count, pairs, op, sync);
        wait_and_restart(pairs);
        wait_recv(op, pairs);
        flush_ep(sender());

        for (int i = 0; i < reqs_count; ++i) {
            auto rbuf = (uint8_t*)m_rbufs[i]->ptr();
            std::vector<uint8_t> rvec(rbuf, rbuf + m_rbufs[i]->size());
            EXPECT_EQ(m_sbufs[i], rvec);
        }
    }

    void skip_no_pending_rma()
    {
        const auto config = ucp_ep_config(sender().ep());
        static const std::vector<std::string> np_tls = {"cma", "knem", "xpmem",
                                                        "sysv", "posix"};

        for (ucp_lane_index_t i = 0; i < config->key.num_lanes; ++i) {
            const auto lane = config->key.rma_bw_lanes[i];
            if (lane == UCP_NULL_LANE) {
                break;
            }

            auto tl_name = ucp_ep_get_tl_rsc(sender().ep(), lane)->tl_name;
            if (std::find(np_tls.begin(), np_tls.end(), tl_name) !=
                np_tls.end()) {
                UCS_TEST_SKIP_R("RMA transport does not support pending queue");
            }
        }
    }

    bool no_rma_transport()
    {
        return has_transport("ud") || has_transport("udx") ||
               has_transport("tcp");
    }

    static void get_test_variants(std::vector<ucp_test_variant> &variants)
    {
        add_variant_with_value(variants,
                               UCP_FEATURE_TAG | UCP_FEATURE_RMA |
                               UCP_FEATURE_STREAM | UCP_FEATURE_AM,
                               0, "");
    }

protected:
    std::vector<std::vector<uint8_t>>           m_sbufs;
    std::vector<std::unique_ptr<mapped_buffer>> m_rbufs;
    std::vector<ucs::handle<ucp_rkey_h>>        m_rkeys;
    bool                                        m_completed;
    size_t                                      m_am_cb_cnt;
    std::vector<ucp_request_t *>                m_pending;
};

UCS_TEST_P(test_proto_reset, tag_eager_multi_bcopy, "ZCOPY_THRESH=inf",
           "RNDV_THRESH=inf")
{
    reset_protocol(TAG);
}

UCS_TEST_SKIP_COND_P(test_proto_reset, get_offload_bcopy, no_rma_transport(),
                     "ZCOPY_THRESH=inf", "RNDV_THRESH=inf")
{
    skip_no_pending_rma();
    reset_protocol(RMA_GET);
}

UCS_TEST_P(test_proto_reset, put_offload_bcopy, "ZCOPY_THRESH=inf",
           "RNDV_THRESH=inf")
{
    skip_no_pending_rma();
    reset_protocol(RMA_PUT);
}

UCS_TEST_P(test_proto_reset, stream_multi_bcopy, "ZCOPY_THRESH=inf",
           "RNDV_THRESH=inf")
{
    reset_protocol(STREAM);
}

UCS_TEST_P(test_proto_reset, rndv_am_bcopy, "ZCOPY_THRESH=inf", "RNDV_THRESH=0",
           "RNDV_SCHEME=am")
{
    reset_protocol(TAG);
}

UCS_TEST_SKIP_COND_P(test_proto_reset, eager_sync_multi_bcopy,
                     has_transport("shm"), "ZCOPY_THRESH=inf",
                     "RNDV_THRESH=inf")
{
    reset_protocol(TAG, true);
}

UCS_TEST_P(test_proto_reset, am_eager_multi_bcopy, "ZCOPY_THRESH=inf",
           "RNDV_THRESH=inf")
{
    reset_protocol(AM);
}

UCS_TEST_P(test_proto_reset, tag_eager_multi_zcopy, "ZCOPY_THRESH=0",
           "RNDV_THRESH=inf")
{
    reset_protocol(TAG);
}

UCS_TEST_SKIP_COND_P(test_proto_reset, get_offload_zcopy, no_rma_transport(),
                     "ZCOPY_THRESH=0", "RNDV_THRESH=inf",
                     "RMA_ZCOPY_MAX_SEG_SIZE=1024")
{
    skip_no_pending_rma();
    reset_protocol(RMA_GET);
}

UCS_TEST_P(test_proto_reset, put_offload_zcopy, "ZCOPY_THRESH=0",
           "RNDV_THRESH=inf", "RMA_ZCOPY_MAX_SEG_SIZE=1024")
{
    skip_no_pending_rma();
    reset_protocol(RMA_PUT);
}

UCS_TEST_P(test_proto_reset, stream_multi_zcopy, "ZCOPY_THRESH=0",
           "RNDV_THRESH=inf")
{
    reset_protocol(STREAM);
}

UCS_TEST_P(test_proto_reset, rndv_am_zcopy, "ZCOPY_THRESH=0", "RNDV_THRESH=0",
           "RNDV_SCHEME=am")
{
    reset_protocol(TAG);
}

UCS_TEST_P(test_proto_reset, am_eager_multi_zcopy, "ZCOPY_THRESH=0",
           "RNDV_THRESH=inf")
{
    reset_protocol(AM);
}

UCS_TEST_P(test_proto_reset, rndv_put, "RNDV_THRESH=0", "RNDV_SCHEME=put_zcopy",
           "RMA_ZCOPY_MAX_SEG_SIZE=1024")
{
    reset_protocol(TAG);
}

UCP_INSTANTIATE_TEST_CASE(test_proto_reset)

/* The following tests require ENABLE_DEBUG_DATA flag in order to access
 * req->recv.proto_rndv_request, which is only present with this flag. */
#if ENABLE_DEBUG_DATA
class test_proto_reset_rndv_get : public test_proto_reset {
protected:
    void wait_and_restart(const std::vector<request_pair_t> &pairs) override
    {
        wait_for_condition(pairs, [](const request_pair_t &pair) {
            if ((request_pair_t::to_user(pair.rreq) == NULL) ||
                (pair.rreq->recv.proto_rndv_request == NULL)) {
                return false;
            }

            const ucp_request_t *rndv_req = pair.rreq->recv.proto_rndv_request;

            return (rndv_req->send.state.dt_iter.offset > 0) &&
                   (rndv_req->send.state.dt_iter.offset <
                    rndv_req->send.state.dt_iter.length);
        });

        restart(receiver().ep());
    }
};

UCS_TEST_SKIP_COND_P(test_proto_reset_rndv_get, rndv_get, no_rma_transport(),
                     "RNDV_THRESH=0", "RNDV_SCHEME=get_zcopy",
                     "RMA_ZCOPY_MAX_SEG_SIZE=1024")
{
    reset_protocol(TAG);
}

UCP_INSTANTIATE_TEST_CASE(test_proto_reset_rndv_get)

class test_proto_reset_atp : public test_proto_reset {
public:
    void init() override
    {
        test_proto_reset::init();
    }

    void cleanup() override
    {
        test_proto_reset::cleanup();
    }

private:
    void hook_uct_cbs()
    {
        ucp_ep_h ep                = sender().ep();
        ucp_lane_index_t num_lanes = ucp_ep_config(ep)->key.num_lanes;
        uct_ep_h uct_ep;
        uct_iface_ops_t *ops;

        for (ucp_lane_index_t lane = 0; lane < num_lanes; ++lane) {
            uct_ep                = ucp_ep_get_lane(ep, lane);
            ops                   = &uct_ep->iface->ops;
            ops->ep_put_zcopy     = (uct_ep_put_zcopy_func_t)
                                    ucs_empty_function_return_no_resource;
            ops->ep_pending_add   = add_pending;
            ops->ep_pending_purge = purge_pending;
        }
    }

    void restore_uct_cbs()
    {
        ucp_ep_h ep                = sender().ep();
        ucp_lane_index_t num_lanes = ucp_ep_config(ep)->key.num_lanes;

        for (ucp_lane_index_t lane = 0; lane < num_lanes; ++lane) {
            ucp_ep_get_lane(ep, lane)->iface->ops = m_ops[lane];
        }
    }

    static ucs_status_t
    add_pending(uct_ep_h tl_ep, uct_pending_req_t *n, unsigned flag)
    {
        return UCS_OK;
    }

    static void
    purge_pending(uct_ep_h ep, uct_pending_purge_callback_t cb, void *arg)
    {
        static int once = 0;
        if (!once) {
            cb(&m_req->send.uct, arg);
            once = 1;
        }
    }

protected:
    void wait_and_restart(const std::vector<request_pair_t> &pairs) override
    {
        ucp_ep_h ep                = sender().ep();
        ucp_lane_index_t num_lanes = ucp_ep_config(ep)->key.num_lanes;

        /* Backup uct ops for all lanes */
        for (ucp_lane_index_t lane = 0; lane < num_lanes; ++lane) {
            m_ops.push_back(ucp_ep_get_lane(ep, lane)->iface->ops);
        }

        hook_uct_cbs();

        /* Wait for rndv_put initialization */
        auto &pair = wait_for_condition(pairs, [](const request_pair_t &pair) {
            std::string rndv_put_zcopy_name("rndv/put/zcopy");
            return rndv_put_zcopy_name ==
                   pair.sreq->send.proto_config->proto->name;
        });

        restore_uct_cbs();

        /* Wait until ATP stage starts */
        static const unsigned send_stage = 0;
        const double timeout             = 10;
        ucs_time_t deadline              = ucs::get_deadline(timeout);

        while ((pair.sreq->send.proto_stage == send_stage) &&
               (ucs_get_time() < deadline)) {
            pair.sreq->send.uct.func(&pair.sreq->send.uct);
        }

        /* One more progress to send the first ATP */
        pair.sreq->send.uct.func(&pair.sreq->send.uct);

        const ucp_request_t *rndv_req = pair.rreq->recv.proto_rndv_request;
        deadline                      = ucs::get_deadline(timeout);

        /* Wait until receiver gets the ATP message */
        while (rndv_req->send.state.completed_size == 0 &&
               (ucs_get_time() < deadline)) {
            receiver().progress();
        }

        m_req = pair.sreq;

        hook_uct_cbs();
        restart(ep);
        restore_uct_cbs();
    }

    static ucp_request_t *m_req;
    std::vector<uct_iface_ops> m_ops;
};

ucp_request_t *test_proto_reset_atp::m_req;

UCS_TEST_P(test_proto_reset_atp, rndv_put, "RNDV_THRESH=0",
           "RNDV_SCHEME=put_zcopy", "RMA_ZCOPY_MAX_SEG_SIZE=1024")
{
    if (count_resources(sender(), "rc_mlx5") <= 1) {
        UCS_TEST_SKIP_R("Less than 2 RC resources are found");
    }

    reset_protocol(TAG, false, 1);
}

UCP_INSTANTIATE_TEST_CASE_TLS(test_proto_reset_atp, ib, "ib")

#endif
