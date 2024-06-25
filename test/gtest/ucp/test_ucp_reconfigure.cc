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
    class reconfigured_entity : public entity {
    public:
        reconfigured_entity(const ucp_test_param &test_params,
                            ucp_config_t* ucp_config,
                            const ucp_worker_params_t& worker_params,
                            const ucp_test *test_owner,
                            const ucp_tl_bitmap_t &tl_bitmap) :
            entity(test_params, ucp_config, worker_params, test_owner),
            m_tl_bitmap(tl_bitmap) {
        }

        void connect(const entity* other, const ucp_ep_params_t& ep_params,
                     int ep_idx = 0, int do_set_ep = 1) override
        {
            unsigned flags = ucp_worker_default_address_pack_flags(other->worker());
            ucp_address_t *address;
            ucs_status_t status;
            size_t addr_len;
            const reconfigured_entity *local_other = static_cast<const reconfigured_entity*>(other);

            status = ucp_address_pack(other->worker(), other->ep(), &local_other->tl_bitmap(), flags,
                                      other->ucph()->config.ext.worker_addr_version,
                                      NULL, UINT_MAX, &addr_len, (void**)&address);
            ASSERT_UCS_OK(status);

            ucp_ep_params_t local_ep_params = {
                    .field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
                    .address = address
            };

            ucp_ep_h ep;
            ASSERT_UCS_OK(ucp_ep_create(worker(), &local_ep_params, &ep));
            m_workers[0].second.push_back(
                            ucs::handle<ucp_ep_h, entity *>(ep, ucp_ep_destroy));

            ucp_worker_release_address(other->worker(), address);
            store_config();
        }

        void verify() const
        {
            auto reused_count = std::count_if(m_uct_eps.begin(), m_uct_eps.end(),
                                             [this](uct_ep_h uct_ep) {
                                                 return is_lane_reused(uct_ep);
                                              });

            EXPECT_EQ(reused_count, is_reconfigured() ? 0 : ucp_ep_num_lanes(ep()));
            if (!is_reconfigured()) {
                return;
            }

            for (ucp_lane_index_t lane = 0; lane < ucp_ep_num_lanes(ep()); ++lane) {
                auto rsc_index = ucp_ep_get_rsc_index(ep(), lane);
                EXPECT_TRUE(UCS_STATIC_BITMAP_GET(m_tl_bitmap, rsc_index));
            }
        }

        bool is_reconfigured() const
        {
            return m_cfg_index != ep()->cfg_index;
        }

        const ucp_tl_bitmap_t &tl_bitmap() const
        {
            return m_tl_bitmap;
        }

    private:
        void store_config()
        {
            for (ucp_lane_index_t lane = 0; lane < ucp_ep_num_lanes(ep()); ++lane) {
                uct_ep_h uct_ep = ucp_ep_get_lane(ep(), lane);

                if (ucp_wireup_ep_test(uct_ep)) {
                    m_uct_eps.push_back(ucp_wireup_ep(uct_ep)->super.uct_ep);
                } else {
                    m_uct_eps.push_back(uct_ep);
                }
            }

            m_cfg_index = ep()->cfg_index;
        }

        bool is_lane_reused(uct_ep_h uct_ep) const
        {
            for (ucp_lane_index_t lane = 0; lane < ucp_ep_num_lanes(ep()); ++lane) {
                if (ucp_ep_get_lane(ep(), lane) == uct_ep) {
                    return true;
                }
            }

            return false;
        }

        ucp_worker_cfg_index_t m_cfg_index;
        std::vector<uct_ep_h>  m_uct_eps;
        ucp_tl_bitmap_t        m_tl_bitmap;
    };

    void init()
    {
        ucp_test::init();

        /* Check presence of IB devices using rc_verbs, as all devices must
         * support it. */
        if (!has_resource(sender(), "rc_verbs")) {
            UCS_TEST_SKIP_R("IB transport is not present");
        }
    }

public:
    static void
    get_test_variants(std::vector<ucp_test_variant> &variants)
    {
        add_variant_with_value(variants, UCP_FEATURE_TAG, 0, "");
    }

    void send_recv(const ucp_tl_bitmap_t &tl_bitmap1, const ucp_tl_bitmap_t &tl_bitmap2)
    {
        /* Init sender/receiver */
        m_entities.push_front(new reconfigured_entity(GetParam(), m_ucp_config,
                                                      get_worker_params(),
                                                      this, tl_bitmap1));
        m_entities.push_back(new reconfigured_entity(GetParam(), m_ucp_config,
                                                      get_worker_params(),
                                                      this, tl_bitmap2));

        sender().connect(&receiver(), get_ep_params());
        receiver().connect(&sender(), get_ep_params());

        ucp_request_param_t param = {
            .op_attr_mask = UCP_OP_ATTR_FLAG_NO_IMM_CMPL
        };
        std::string m_sbuf, m_rbuf;

        for (int i = 0; i < 100; ++ i) {
            m_sbuf.resize(msg_size);
            m_rbuf.resize(msg_size);
            std::fill(m_sbuf.begin(), m_sbuf.end(), 'a');

            void *sreq = ucp_tag_send_nbx(sender().ep(), m_sbuf.c_str(),
                                          msg_size, 0, &param);
            void *rreq = ucp_tag_recv_nbx(receiver().worker(), (void*)m_rbuf.c_str(),
                                          msg_size, 0, 0, &param);
            request_wait(rreq);
            request_wait(sreq);
        }
        //todo: verify buff content
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

    static const reconfigured_entity& to_reconfigured(const entity &e)
    {
        return *static_cast<const reconfigured_entity*>(&e);
    }

    void verify()
    {
        to_reconfigured(sender()).verify();
        to_reconfigured(receiver()).verify();
    }

    static constexpr size_t msg_size = 16 * UCS_KBYTE;
};

UCS_TEST_P(test_ucp_reconfigure, all_lanes_reused)
{
    ucp_tl_bitmap_t tl_bitmap;
    UCS_STATIC_BITMAP_SET_ALL(&tl_bitmap);
    send_recv(tl_bitmap, tl_bitmap);

    EXPECT_FALSE(to_reconfigured(sender()).is_reconfigured());
    EXPECT_FALSE(to_reconfigured(receiver()).is_reconfigured());
    verify();
}

UCS_TEST_SKIP_COND_P(test_ucp_reconfigure, no_lanes_reused, has_transport("dc"))
{
    /* Split resources between entities, so that intersection is empty */
    ucp_tl_bitmap_t tl_bitmap1, tl_bitmap2;
    enable_dev(0, tl_bitmap1);
    enable_dev(1, tl_bitmap2);
    send_recv(tl_bitmap1, tl_bitmap2);

    EXPECT_NE(to_reconfigured(sender()).is_reconfigured(),
              to_reconfigured(receiver()).is_reconfigured());
    verify();
}

UCP_INSTANTIATE_TEST_CASE(test_ucp_reconfigure);
