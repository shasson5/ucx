
/**
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2020. ALL RIGHTS RESERVED.
* Copyright (C) Los Alamos National Security, LLC. 2019 ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "khash.h"
#include "lru.h"
#include "rcdc_balancer.h"
#include "ucp/core/ucp_ep.h"
#include "ucp/core/ucp_types.h"
#include "ucp/wireup/address.h"
#include "ucp/wireup/wireup.h"
#include "ucs/datastruct/list.h"
#include "ucs/sys/math.h"

#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>


typedef struct {
    void  *key;
    size_t hit_count;
    int    marked;
    size_t tx;
    size_t rx;
    ucs_list_link_t list;
} ucs_balancer_element_t;

__KHASH_TYPE(aggregator_hash, uint64_t, ucs_balancer_element_t);

__KHASH_IMPL(aggregator_hash, kh_inline, uint64_t, ucs_balancer_element_t, 1,
             kh_int64_hash_func, kh_int64_hash_equal);


typedef khash_t(aggregator_hash) ucs_aggregator_hash_t;

typedef struct {
    ucs_aggregator_hash_t hash;
    ucs_lru_h             lru;
    uint32_t              interval_us;
    unsigned              ticks_per_flush;
    unsigned              rc_size;
    uint64_t              last_aggregated;
    uint64_t              ticks;
    ucs_list_link_t       active_list;
    int                   flushed;
} ucs_balancer_t;

//todo: handle removing endpoint by the user.

static ucs_balancer_t ucs_balancer;

#define SEC_TO_US 1e6

//todo: khash resizing cause issues, find out why it's triggered and avoid it.

static uint64_t getMicrosecondTimeStamp()
{
    struct timeval tv;
    if (gettimeofday(&tv, NULL)) {
        abort();
    }

    return tv.tv_sec * 1000000 + tv.tv_usec;
}

ucs_status_t ucs_balancer_init(uint32_t interval_sec, unsigned ticks_per_flush, unsigned rc_size)
{
    ucs_balancer.lru = ucs_lru_init(UCS_BALANCER_MAX_LRU_SIZE);
    if (ucs_balancer.lru == NULL) {
        return UCS_ERR_NO_RESOURCE;
    }

    kh_init_inplace(aggregator_hash, &ucs_balancer.hash);

    //todo: handle resizing on aggregate.

    if (kh_resize(aggregator_hash, &ucs_balancer.hash, UCS_BALANCER_MAX_LRU_SIZE * 2) < 0) {
        ucs_lru_destroy(ucs_balancer.lru);
        //todo: change error code.
        return UCS_ERR_NO_RESOURCE;
    }

    ucs_balancer.last_aggregated = getMicrosecondTimeStamp();
    ucs_balancer.interval_us     = interval_sec * SEC_TO_US;
    ucs_balancer.ticks_per_flush = ticks_per_flush;
    ucs_balancer.ticks           = 0;
    ucs_balancer.rc_size         = rc_size;
    ucs_balancer.flushed         = 0;
    ucs_list_head_init(&ucs_balancer.active_list);
    return UCS_OK;
}

void ucs_balancer_destroy()
{
    ucs_lru_destroy(ucs_balancer.lru);
    kh_destroy_inplace(aggregator_hash, &ucs_balancer.hash);
}

static ucs_balancer_element_t *ucs_balancer_put_element(void *key)
{
    int ret;
    khint_t iter;
    ucs_balancer_element_t *elem;

    iter      = kh_put(aggregator_hash, &ucs_balancer.hash, (uint64_t)key, &ret);
    elem      = &kh_val(&ucs_balancer.hash, iter);
    elem->key = key;

    if ((ret == UCS_KH_PUT_BUCKET_EMPTY) || (ret == UCS_KH_PUT_BUCKET_CLEAR)) {
        elem->hit_count = 0;
        elem->marked    = 0;
        elem->tx        = 0;
        elem->rx        = 0;
    }

    elem->hit_count ++;
    return elem;
}

void ucs_balancer_aggregate()
{
    static void *results[UCS_BALANCER_MAX_LRU_SIZE];
    size_t size, i;

    ucs_lru_get(ucs_balancer.lru, results, &size);

    for (i = 0; i < size; ++ i) {
        ucs_balancer_put_element(results[i]);
    }

    ucs_lru_reset(ucs_balancer.lru);
}

int ucs_balancer_add(void *element)
{
    uint64_t now;

    ucs_lru_touch(ucs_balancer.lru, element);

    //todo: use HW clock register
    now = getMicrosecondTimeStamp();
    if (now >= ucs_balancer.last_aggregated + ucs_balancer.interval_us) {
        ucs_balancer_aggregate();
        ucs_balancer.last_aggregated = now;
        ucs_balancer.ticks ++;

        if ((ucs_balancer.ticks % ucs_balancer.ticks_per_flush) == 0) {
            ucs_balancer_flush();
        }
    }

    return 0;
}

//todo: change to heap sort. (and then get min by popping instead of searching).

static size_t ucs_balancer_score(ucs_balancer_element_t *item)
{
    return ucs_max(item->tx, item->rx);
}

size_t ucs_balancer_get_score(void *key)
{
    ucs_balancer_element_t *item;
    khint_t iter;

    iter = kh_get(aggregator_hash, &ucs_balancer.hash, (uint64_t)key);
    if (iter == kh_end(&ucs_balancer.hash)) {
        return 0;
//        ucs_assertv(0, "ucs_balancer_get_score failed: %p", key);
    }

    item = &kh_value(&ucs_balancer.hash, iter);
    return ucs_balancer_score(item);
}

static ucs_balancer_element_t *ucs_balancer_get_min_active()
{
    ucs_balancer_element_t *min_item, *item;

    min_item = ucs_list_head(&ucs_balancer.active_list, ucs_balancer_element_t, list);
    ucs_list_for_each(item, &ucs_balancer.active_list, list) {
        if (ucs_balancer_score(item) < ucs_balancer_score(min_item)) {
            min_item = item;
        }
    }

    return min_item;
}

//todo: remove from hash table when ejecting

static ucs_balancer_element_t *ucs_balancer_pushpop_active(ucs_balancer_element_t *elem)
{
    ucs_balancer_element_t *min_item;

    ucs_list_add_tail(&ucs_balancer.active_list, &elem->list);
    if (ucs_list_length(&ucs_balancer.active_list) <= ucs_balancer.rc_size) {
        return NULL;
    }

    min_item = ucs_balancer_get_min_active();
    ucs_list_del(&min_item->list);
    return min_item;
}

static int ucs_balancer_is_active(ucs_balancer_element_t *elem)
{
    ucs_balancer_element_t *item;

    ucs_list_for_each(item, &ucs_balancer.active_list, list) {
        if (elem == item) {
            return 1;
        }
    }

    return 0;
}

void ucs_balancer_remove(void *key)
{
    ucs_balancer_element_t *item;
    khint_t iter;

    iter = kh_get(aggregator_hash, &ucs_balancer.hash, (uint64_t)key);
    if (iter == kh_end(&ucs_balancer.hash)) {
        //todo: error
    }

    item = &kh_value(&ucs_balancer.hash, iter);
    if (!ucs_balancer_is_active(item)) {
        //todo: error
        return;
    }

    //todo: remove from hash
    ucs_list_del(&item->list);
}

void *ucs_balancer_push_rx(void *key, size_t score)
{
    ucs_balancer_element_t *element, *ejected;

    if (!ucs_balancer_is_important(score)) {
        return NULL;
    }

    element     = ucs_balancer_put_element(key);
    element->rx = score;

    ejected = NULL;
    if (!ucs_balancer_is_active(element)) {
        ejected = ucs_balancer_pushpop_active(element);
    }

    return ejected;
}

int ucs_balancer_is_important(size_t score)
{
    static const double rc_thresh = 0.6;
    int epsilon                   = 1;
    ucs_balancer_element_t *min_item;

//    printf("score: %lu %.1f %u\n", score, rc_thresh, ucs_balancer.ticks_per_flush);

    if (score < rc_thresh * ucs_balancer.ticks_per_flush) {
        return 0;
    }

//    printf("passed\n");

    if (ucs_list_length(&ucs_balancer.active_list) < ucs_balancer.rc_size) {
        return 1;
    }

    min_item = ucs_balancer_get_min_active();
    return (score - ucs_balancer_score(min_item)) > epsilon;
}

static void ucs_balancer_flush_tx()
{
    khint_t k;
    ucs_balancer_element_t *elem;

    //todo: remove hit_count and use tx instead.
    for (k = kh_begin(&ucs_balancer.hash); k != kh_end(&ucs_balancer.hash); ++k) {
        if (!kh_exist(&ucs_balancer.hash, k)) {
            continue;
        }

        elem = &kh_val(&ucs_balancer.hash, k);
        elem->tx = elem->hit_count;
        elem->hit_count = 0;
        elem->marked    = 0;
    }
}

void ucs_balancer_get(ucs_balancer_state_t *state)
{
    ucs_balancer_element_t *item;
    void **elem;

    state->flushed = ucs_balancer.flushed;

    if (!ucs_balancer.flushed) {
        return;
    }

    ucs_array_set_length(&state->array, 0);
    printf("RC: ");
    ucs_list_for_each(item, &ucs_balancer.active_list, list) {
        elem = ucs_array_append_fixed(rc_ptr, &state->array);
        *elem = item->key;

        printf("(%p, %lu), ", item->key, item->tx);
    }
    printf("\n");

    ucs_balancer.flushed = 0;
}

void ucs_balancer_flush()
{
    int count = 0;
    khint_t k;
    ucs_balancer_element_t *elem, *max_elem;

    ucs_balancer_flush_tx();

    while (count < ucs_balancer.rc_size) {
        max_elem = NULL;

        //todo: do not sort
        for (k = kh_begin(&ucs_balancer.hash); k != kh_end(&ucs_balancer.hash); ++k) {
            if (!kh_exist(&ucs_balancer.hash, k)) {
                continue;
            }

            elem = &kh_val(&ucs_balancer.hash, k);
            if (ucs_balancer_is_active(elem)) {
                continue;
            }

            if (((max_elem == NULL) ||
                (ucs_balancer_score(elem) > ucs_balancer_score(max_elem))) && !elem->marked) {
                max_elem = elem;
            }
        }

        if (max_elem == NULL) {
            break;
        }

        if (!ucs_balancer_is_important(ucs_balancer_score(max_elem))) {
            break;
        }

        ucs_balancer_pushpop_active(max_elem);
        max_elem->marked = 1;
        count ++;
    }

    ucs_balancer.flushed = 1;
}

