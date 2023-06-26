
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
#include "ucs/sys/math.h"
#include "ucs/datastruct/list.h"

#include "ucp/core/ucp_ep.h"
#include "ucp/core/ucp_types.h"
#include "ucp/wireup/wireup.h"
#include "ucp/wireup/address.h"

#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>


typedef struct {
    void  *key;
    size_t hit_count;
    int    active;
    int    marked;
    int    active_marked;
    ucs_list_link_t active_list;
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
    int                   flush;
} ucs_balancer_t;

//todo: handle removing endpoint by the user.

static ucs_balancer_t ucs_balancer;

//todo: remove and use tick_per_flush.
#define UCS_BALANCER_MAX_SAMPLES 100
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
    ucs_balancer.flush           = 0;
    ucs_balancer.rc_size         = rc_size;
    ucs_list_head_init(&ucs_balancer.active_list);
    return UCS_OK;
}

void ucs_balancer_destroy()
{
    ucs_lru_destroy(ucs_balancer.lru);
    kh_destroy_inplace(aggregator_hash, &ucs_balancer.hash);
}

void ucs_balancer_aggregate()
{
    static void *results[UCS_BALANCER_MAX_LRU_SIZE];
    ucs_balancer_element_t *elem;
    khint_t iter;
    int ret;
    size_t size, i;

    ucs_lru_get(ucs_balancer.lru, results, &size);

    for (i = 0; i < size; ++ i) {
        iter      = kh_put(aggregator_hash, &ucs_balancer.hash, (uint64_t)results[i], &ret);
        elem      = &kh_val(&ucs_balancer.hash, iter);
        elem->key = results[i];

        if ((ret == UCS_KH_PUT_BUCKET_EMPTY) || (ret == UCS_KH_PUT_BUCKET_CLEAR)) {
            elem->hit_count = 0;
            elem->active    = 0;
            elem->marked    = 0;
            elem->active_marked = 0;
        }

        elem->hit_count ++;
    }
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
            ucs_balancer.flush = 1;
        }
    }

    return 0;
}

//todo: change to heap sort. (and then get min by popping instead of searching).

static void ucs_balancer_reset(ucs_balancer_element_t *elem_arr)
{
    int i;
    khint_t iter;
    int ret;
    ucs_balancer_element_t *elem;

    kh_clear(aggregator_hash, &ucs_balancer.hash);

    for (i = 0; i < ucs_balancer.rc_size; ++ i) {
        iter            = kh_put(aggregator_hash, &ucs_balancer.hash, (uint64_t)elem_arr[i].key, &ret);
        elem            = &kh_val(&ucs_balancer.hash, iter);
        elem->key       = elem_arr[i].key;
        elem->hit_count = 0;
        elem->active    = 1;
        elem->marked    = 0;
        elem->active_marked = 0;
    }

    ucs_list_head_init(&ucs_balancer.active_list);
}

static void get_list()
{
    khint_t k;
    ucs_balancer_element_t *elem, *max_elem;

    while(1) {
        max_elem = NULL;
        for (k = kh_begin(&ucs_balancer.hash); k != kh_end(&ucs_balancer.hash); ++k) {
            if (kh_exist(&ucs_balancer.hash, k)) {
                elem = &kh_val(&ucs_balancer.hash, k);
                if (elem->active && !elem->active_marked && ((max_elem == NULL)  || (elem->hit_count > max_elem->hit_count))) {
                    max_elem = elem;
                }
            }
        }

        if (max_elem == NULL) {
            break;
        }

        max_elem->active_marked = 1;
        ucs_list_add_tail(&ucs_balancer.active_list, &max_elem->active_list);
    }

    ucs_balancer.flush = 0;
}

void ucs_balancer_flush(void **arr_p, size_t *size_p)
{
    static const double rc_thresh = 0.00002;
    int count = 0, epsilon = 1;
    int i;
    khint_t k;
    static ucs_balancer_element_t elem_arr[UCS_BALANCER_MAX_LRU_SIZE * UCS_BALANCER_MAX_SAMPLES];
    ucs_balancer_element_t *elem, *tail, *max_elem;

    if (!ucs_balancer.flush) {
        *size_p = 0;
        return;
    }

    get_list();

    while (count < ucs_balancer.rc_size) {
        max_elem = NULL;

        for (k = kh_begin(&ucs_balancer.hash); k != kh_end(&ucs_balancer.hash); ++k) {
            if (kh_exist(&ucs_balancer.hash, k)) {
                elem = &kh_val(&ucs_balancer.hash, k);
                if ((elem->hit_count > rc_thresh * UCS_BALANCER_MAX_SAMPLES) && ((max_elem == NULL) ||
                    (elem->hit_count > max_elem->hit_count)) && !elem->marked) {
                    max_elem = elem;
                }
            }
        }

        if (max_elem == NULL) {
            break;
        }

        if (ucs_list_is_empty(&ucs_balancer.active_list)) {
            elem_arr[count] = *max_elem;
            count ++;
        }

        else {
            tail = ucs_container_of(ucs_balancer.active_list.prev, ucs_balancer_element_t, active_list);
            if (((max_elem->hit_count - tail->hit_count) > epsilon) || max_elem->active) {
                elem_arr[count] = *max_elem;
                count ++;

                if (!max_elem->active) {
                    ucs_list_del(ucs_balancer.active_list.prev);
                }
            }
        }

        max_elem->marked = 1;
    }

    for (i = 0; i < ucs_balancer.rc_size; ++ i) {
        arr_p[i] = elem_arr[i].key;
    }

    *size_p = ucs_balancer.rc_size;
    ucs_balancer_reset(elem_arr);

//////////////////////////////////////////
    printf("RC:\n");

    for (i = 0; i < ucs_balancer.rc_size; ++ i) {
        printf("(%p,%lu), ", elem_arr[i].key, elem_arr[i].hit_count);
    }

    printf("\n");
}

