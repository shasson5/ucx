/**
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2015. ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#include <common/test.h>

#include "ucp_test.h"
#include "common/test.h"
#include "common/test_helpers.h"
#include "ucp/ucp_test.h"

#include <algorithm>
#include <set>

extern "C" {
#include <ucp/wireup/address.h>
#include <ucp/wireup/wireup_ep.h>
#include <ucp/core/ucp_ep.inl>
#include <ucs/sys/math.h>
#include <uct/base/uct_iface.h>
}

class test_ucp_reconfigure : public ucp_test {
protected:
    class reconf_ep_t {
    public:
        reconf_ep_t(const entity &e) :
            m_ep(e.ep()), m_cfg_index(e.ep()->cfg_index)
        {
            for (ucp_lane_index_t lane = 0; lane < num_lanes(); ++lane) {
                uct_ep_h uct_ep = ucp_ep_get_lane(e.ep(), lane);

                if (ucp_wireup_ep_test(uct_ep)) {
                    m_uct_eps.push_back(ucp_wireup_ep(uct_ep)->super.uct_ep);
                } else {
                    m_uct_eps.push_back(uct_ep);
                }
            }

            m_transport = ucp_ep_get_tl_rsc(m_ep, m_ep->am_lane)->tl_name;
        }

        ucp_lane_index_t num_lanes() const
        {
            return ucp_ep_config(m_ep)->key.num_lanes;
        }

        void verify(unsigned expected_reused, bool reconfigured = true)
        {
            bool is_reconfigured = (m_ep->cfg_index != m_cfg_index);
            EXPECT_EQ(is_reconfigured, reconfigured);
            EXPECT_LE(reused_count(), expected_reused);

            auto config = ucp_ep_config(m_ep);

            for (int i = 0; i < num_lanes(); ++i) {
                const auto lane = config->key.rma_bw_lanes[i];
                if (lane == UCP_NULL_LANE) {
                    break;
                }

                auto transport = ucp_ep_get_tl_rsc(m_ep, lane)->tl_name;
                EXPECT_STREQ("rc_mlx5", transport);
            }
        }

    private:
        bool uct_ep_reused(uct_ep_h uct_ep) const
        {
            for (ucp_lane_index_t lane = 0; lane < num_lanes(); ++lane) {
                if (ucp_ep_get_lane(m_ep, lane) == uct_ep) {
                    return true;
                }
            }

            return false;
        }

        unsigned reused_count() const
        {
            return std::count_if(m_uct_eps.begin(), m_uct_eps.end(),
                                 [this](const uct_ep_h uct_ep) {
                                     return uct_ep_reused(uct_ep);
                                 });
        }

        ucp_ep_h m_ep;
        ucp_worker_cfg_index_t m_cfg_index;
        std::vector<uct_ep_h> m_uct_eps;
        std::string m_transport;
    };

    enum {
        MSG_SIZE_SMALL  = 64,
        MSG_SIZE_MEDIUM = 4096,
        MSG_SIZE_LARGE  = 262144
    };

    void init()
    {
        ucp_test::init();

        if (!has_resource(sender(), "rc_mlx5") &&
            !has_resource(sender(), "dc_mlx5")) {
            UCS_TEST_SKIP_R("IB transport is not present");
        }

        m_reconfigure = false;
    }

    virtual unsigned msg_size()
    {
        return get_variant_value(2);
    }

    void *send_nb(std::string &buffer, uint8_t data)
    {
        ucp_request_param_t param = {
            .op_attr_mask = UCP_OP_ATTR_FLAG_NO_IMM_CMPL
        };

        buffer.resize(msg_size());
        std::fill(buffer.begin(), buffer.end(), data);

        return ucp_tag_send_nbx(sender().ep(), buffer.c_str(), msg_size(), 0,
                                &param);
    }

    void send_recv(unsigned count)
    {
        std::vector<void*> sreqs;

        for (int i = 0; i < count; ++i) {
            sreqs.push_back(send_nb(m_sbuf, i + 1));
            request_wait(recv_nb());
        }

        requests_wait(sreqs);
    }

    void *recv_nb()
    {
        ucp_request_param_t param = {0};
        param.op_attr_mask        = UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
        m_rbuf.resize(msg_size());

        return ucp_tag_recv_nbx(receiver().worker(), (void*)m_rbuf.c_str(),
                                msg_size(), 0, 0, &param);
    }

    std::string m_sbuf;
    std::string m_rbuf;
    bool m_reconfigure;

public:
    static void get_test_variants(std::vector<ucp_test_variant> &variants)
    {
        add_variant_values(variants, get_test_variants_feature, MSG_SIZE_SMALL,
                           "small");
        add_variant_values(variants, get_test_variants_feature, MSG_SIZE_MEDIUM,
                           "medium");
        add_variant_values(variants, get_test_variants_feature, MSG_SIZE_LARGE,
                           "large");
    }

    static void
    get_test_variants_feature(std::vector<ucp_test_variant> &variants)
    {
        add_variant_with_value(variants, UCP_FEATURE_TAG, 0, "");
    }

    bool is_reconf_ep(const entity *e)
    {
        const entity *other;
        other = (e == &sender()) ? &receiver() : &sender();

        return !(e->ep()->flags & UCP_EP_FLAG_CONNECT_REQ_QUEUED) &&
               (other->ep()->flags & UCP_EP_FLAG_CONNECT_REQ_QUEUED);
    }

    void connect_ep(entity &e1, entity &e2, const ucp_tl_bitmap_t &tl_bitmap)
    {
        //todo: check value of get_ep_params()
        unsigned flags = ucp_worker_default_address_pack_flags(e2.worker());
        ucp_address_t *address;
        ucs_status_t status;
        size_t addr_len;

        status = ucp_address_pack(e2.worker(), e2.ep(), &tl_bitmap, flags,
                                  e2.ucph()->config.ext.worker_addr_version,
                                  NULL, UINT_MAX, &addr_len, (void**)&address);
        ASSERT_UCS_OK(status);

        ucp_ep_params_t ep_params = {
                .field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
                .address = address
        };

        ucp_ep_h ep;
        ASSERT_UCS_OK(ucp_ep_create(e1.worker(), &ep_params, &ep));
        e1.set_ep(ep, 0, 0);

        ucp_worker_release_address(e2.worker(), address);
    }

    reconf_ep_t race_connect(const ucp_tl_bitmap_t &tl_bitmap1, const ucp_tl_bitmap_t &tl_bitmap2)
    {
        connect_ep(sender(), receiver(), tl_bitmap1);
        connect_ep(receiver(), sender(), tl_bitmap2);

        reconf_ep_t reconf_sender(sender()), reconf_receiver(receiver());
        send_recv(100);

        return (is_reconf_ep(&sender()) ||
                (!is_reconf_ep(&receiver()) &&
                 sender().worker()->uuid > receiver().worker()->uuid)) ?
                       reconf_sender :
                       reconf_receiver;
    }

    void enable_dev(unsigned dev_index, ucp_tl_bitmap_t &tl_bitmap)
    {
        int rc_count = 0;
        ucp_rsc_index_t tl_id;

        UCS_STATIC_BITMAP_RESET_ALL(&tl_bitmap);

        UCS_STATIC_BITMAP_FOR_EACH_BIT(tl_id, &sender().ucph()->tl_bitmap) {
            auto resource = &sender().ucph()->tl_rscs[tl_id];

            if (std::string(resource->tl_rsc.tl_name) == "rc_mlx5") {
                rc_count++;

                if ((rc_count - 1) == dev_index) {
                    UCS_STATIC_BITMAP_SET(&tl_bitmap, tl_id);
                }
            }

            if (std::string(resource->tl_rsc.tl_name) == "ud_mlx5") {
                UCS_STATIC_BITMAP_SET(&tl_bitmap, tl_id);
            }
        }
    }
};

UCS_TEST_SKIP_COND_P(test_ucp_reconfigure, race_all_reuse, is_self())
{
    ucp_tl_bitmap_t tl_bitmap;

    UCS_STATIC_BITMAP_SET_ALL(&tl_bitmap);
    auto reconf_ep = race_connect(tl_bitmap, tl_bitmap);
    reconf_ep.verify(reconf_ep.num_lanes(), false);
}

//UCS_TEST_SKIP_COND_P(test_ucp_reconfigure, race_all_reuse_part_scale, is_self())
//{
////    disable_dc_dev(0);
////    set_scale(start_scaled());
//    auto reconf_ep = race_connect();
//    reconf_ep.verify(m_disabled_devs, false);
//}

UCS_TEST_SKIP_COND_P(test_ucp_reconfigure, race_no_reuse, is_self())
{
    ucp_tl_bitmap_t tl_bitmap1, tl_bitmap2;
    enable_dev(0, tl_bitmap1);
    enable_dev(1, tl_bitmap2);

    auto reconf_ep = race_connect(tl_bitmap1, tl_bitmap2);
    reconf_ep.verify(0);
}

//UCS_TEST_SKIP_COND_P(test_ucp_reconfigure, race_no_reuse_switch_wireup_lane,
//                     is_self(),
//                     "RESOLVE_REMOTE_EP_ID=y")
//{
////    set_scale(start_scaled(), true);
//    auto reconf_ep = race_connect();
//    reconf_ep.verify(m_disabled_devs);
//}

UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_reconfigure, ib, "ib")
