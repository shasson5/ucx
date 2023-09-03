/**
 * Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2023. ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include <common/test.h>

extern "C" {
#include "ucs/datastruct/usage_tracker.h"
}

class test_usage_tracker : public ucs::test {
protected:
    virtual void init()
    {
        ucs::test::init();
        ASSERT_UCS_OK(ucs_usage_tracker_create(&m_params, &m_usage_tracker));
    }

    virtual void cleanup()
    {
        ucs_usage_tracker_destroy(m_usage_tracker);
        ucs::test::cleanup();
    }

    void add(const std::vector<uint64_t> &input)
    {
        for (auto &entry : input) {
            ucs_usage_tracker_touch_key(m_usage_tracker, (void*)entry);
        }
    }

    static void rank_cb(void *entry, void *arg)
    {
        std::vector<uint64_t> *results = (std::vector<uint64_t>*)arg;
        results->push_back((uint64_t)entry);
    }

    void verify_rank(const std::vector<uint64_t> &expected,
                     const std::vector<uint64_t> &actual,
                     const std::string &operation)
    {
        ASSERT_EQ(actual.size(), expected.size()) << operation;

        for (int i = 0; i < expected.size(); ++i) {
            ASSERT_TRUE(std::find(expected.begin(), expected.end(),
                                  actual[i]) != expected.end())
                    << "index " << i << ", elem: " << actual[i] << operation;
        }
    }

    void verify(const std::vector<uint64_t> &exp_promoted,
                const std::vector<uint64_t> &exp_demoted)
    {
        verify_rank(exp_promoted, m_promoted, "promotion");
        verify_rank(exp_demoted, m_demoted, "demotion");
    }

    ucs_usage_tracker_params_t m_params = {30, 10, 0.2, rank_cb,
                                           &m_promoted, rank_cb,
                                           &m_demoted, {0.8, 0.2}};
    std::vector<uint64_t> m_promoted;
    std::vector<uint64_t> m_demoted;
    ucs_usage_tracker_h m_usage_tracker;
};

UCS_TEST_F(test_usage_tracker, promote) {
    std::vector<uint64_t> elements, promoted, demoted;

    for (int i = 0; i < m_params.promote_capacity; ++i) {
        elements.push_back(i);
    }

    for (int i = 0; i < 10; ++i) {
        ucs_usage_tracker_progress(m_usage_tracker);
        add(elements);
    }

    promoted = {elements.begin(), elements.begin() + m_params.promote_thresh};
    verify(promoted, demoted);
}

UCS_TEST_F(test_usage_tracker, stability) {
    ucs_usage_tracker_destroy(m_usage_tracker);
    m_params.promote_capacity = 10;
    ASSERT_UCS_OK(ucs_usage_tracker_create(&m_params, &m_usage_tracker));

    std::vector<uint64_t> elements1, elements2, promoted, demoted;
    for (int i = 0; i < m_params.promote_capacity; ++i) {
        elements1.push_back(i);
        elements2.push_back(i + m_params.promote_capacity);
    }

    add(elements1);
    ucs_usage_tracker_progress(m_usage_tracker);

    add(elements2);
    ucs_usage_tracker_progress(m_usage_tracker);

    promoted = {elements1.begin(), elements1.begin() + m_params.promote_thresh};
    verify(promoted, demoted);
}

UCS_TEST_F(test_usage_tracker, demote) {
    ucs_usage_tracker_destroy(m_usage_tracker);
    m_params.promote_capacity = 10;
    ASSERT_UCS_OK(ucs_usage_tracker_create(&m_params, &m_usage_tracker));

    std::vector<uint64_t> elements1, elements2, demoted, promoted;
    for (int i = 0; i < m_params.promote_capacity; ++i) {
        elements1.push_back(i);
        elements2.push_back(i + m_params.promote_capacity);
    }

    for (int i = 0; i < 5; ++i) {
        ucs_usage_tracker_progress(m_usage_tracker);
        add(elements1);
    }

    for (int i = 0; i < 10; ++i) {
        ucs_usage_tracker_progress(m_usage_tracker);
        add(elements2);
    }

    demoted = {elements1.begin(), elements1.begin() + m_params.promote_thresh};
    promoted = {demoted.begin(), demoted.end()};
    promoted.insert(promoted.end(), elements2.begin(), elements2.end());
    verify(promoted, demoted);
}
