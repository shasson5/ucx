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
        ASSERT_UCS_OK(ucs_usage_tracker_create(m_ticks_per_flush, m_max_lru,
                                               m_active_thresh,
                                               m_stability_diff, flush_cb,
                                               &m_results, &m_usage_tracker));
    }

    virtual void cleanup()
    {
        ucs_usage_tracker_destroy(m_usage_tracker);
        ucs::test::cleanup();
    }

    void tick(unsigned count)
    {
        for (size_t i = 0; i < count; ++i) {
            ucs_usage_tracker_tick(m_usage_tracker);
        }
    }

    void add(const std::vector<uint64_t> &input)
    {
        for (auto &entry : input) {
            ucs_usage_tracker_add(m_usage_tracker, (void*)entry);
        }
    }

    static void flush_cb(void *entry, void *opaque)
    {
        std::vector<uint64_t> *results = (std::vector<uint64_t>*)opaque;
        results->push_back((uint64_t)entry);
    }

    void verify(const std::vector<uint64_t> &expected)
    {
        ASSERT_EQ(m_results.size(), expected.size());

        for (int i = 0; i < expected.size(); ++i) {
            ASSERT_TRUE(std::find(expected.begin(), expected.end(),
                                  m_results[i]) != expected.end())
                    << "index " << i << ", elem: " << m_results[i];
        }

        m_results.clear();
    }

    const double m_active_thresh      = 0.2;
    const size_t m_ticks_per_flush    = 30;
    const unsigned m_stability_diff   = 4;
    static constexpr size_t m_max_lru = 10;
    std::vector<uint64_t> m_results;
    ucs_usage_tracker_h m_usage_tracker;
};

constexpr size_t test_usage_tracker::m_max_lru;

UCS_TEST_F(test_usage_tracker, basic) {
    std::vector<uint64_t> elements1;
    for (int i = 0; i < m_max_lru; ++i) {
        elements1.push_back(i);
    }

    const unsigned hits1 = 10;

    tick(m_ticks_per_flush - hits1);
    add(elements1);
    tick(hits1);
    verify(elements1);
}

UCS_TEST_F(test_usage_tracker, stability_no_change) {
    std::vector<uint64_t> elements1;
    for (int i = 0; i < m_max_lru; ++i) {
        elements1.push_back(i);
    }

    const unsigned hits1 = 10;

    tick(m_ticks_per_flush - hits1);
    add(elements1);
    tick(hits1);
    verify(elements1);

    std::vector<uint64_t> elements2;
    for (int i = 0; i < m_max_lru; ++i) {
        elements2.push_back(i + m_max_lru);
    }

    const unsigned hits2 = hits1 + m_stability_diff;

    tick(m_ticks_per_flush - hits1 - hits2);
    add(elements1);
    tick(hits1);

    add(elements2);
    tick(hits2);
    verify(elements1);
}

UCS_TEST_F(test_usage_tracker, stability_change) {
    std::vector<uint64_t> elements1;
    for (int i = 0; i < m_max_lru; ++i) {
        elements1.push_back(i);
    }

    const unsigned hits1 = 10;

    tick(m_ticks_per_flush - hits1);
    add(elements1);
    tick(hits1);
    verify(elements1);

    std::vector<uint64_t> elements2;
    for (int i = 0; i < m_max_lru; ++i) {
        elements2.push_back(i + m_max_lru);
    }

    const unsigned hits2 = hits1 + m_stability_diff + 1;

    tick(m_ticks_per_flush - hits1 - hits2);
    add(elements1);
    tick(hits1);

    add(elements2);
    tick(hits2);
    verify(elements2);
}

UCS_TEST_F(test_usage_tracker, below_active_thresh) {
    std::vector<uint64_t> elements1;
    for (int i = 0; i < m_max_lru; ++i) {
        elements1.push_back(i);
    }

    unsigned hits = m_active_thresh * m_ticks_per_flush;

    tick(m_ticks_per_flush - hits);
    add(elements1);
    tick(hits);

    std::vector<uint64_t> empty_vec;
    verify(empty_vec);
}
