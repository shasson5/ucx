
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

#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>


typedef struct {
    void  *key;
    size_t hit_count;
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
    uint64_t              last_aggregated;
    uint64_t              ticks;
} ucs_balancer_t;

static ucs_balancer_t ucs_balancer;

#define UCS_BALANCER_MAX_LRU_SIZE 20
#define UCS_BALANCER_MAX_SAMPLES 100
#define SEC_TO_US 1e6

static uint64_t getMicrosecondTimeStamp()
{
    struct timeval tv;
    if (gettimeofday(&tv, NULL)) {
        abort();
    }

    return tv.tv_sec * 1000000 + tv.tv_usec;
}

ucs_status_t ucs_balancer_init(uint32_t interval_sec, unsigned ticks_per_flush)
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
        }

        elem->hit_count ++;
    }
}

unsigned ucs_balancer_add(void *element, unsigned id)
{
    uint64_t now;
    size_t size;
    static void *results[30];

    id = ucs_lru_touch(ucs_balancer.lru, id, element);

    //todo: use HW clock register
    now = getMicrosecondTimeStamp();
    if (now >= ucs_balancer.last_aggregated + ucs_balancer.interval_us) {
        ucs_balancer_aggregate();
        ucs_balancer.last_aggregated = now;
        ucs_balancer.ticks ++;

        if ((ucs_balancer.ticks % ucs_balancer.ticks_per_flush) == 0) {
            ucs_balancer_flush(results, &size);
        }
    }

    return id;
}

//todo: change to heap sort.
//todo: add important filtering.
//todo: add over switch protection.

static int compare_hit_count(const void *elem1, const void *elem2) {

    return ((ucs_balancer_element_t *)elem2)->hit_count - ((ucs_balancer_element_t *)elem1)->hit_count;
}

void ucs_balancer_flush(void **arr_p, size_t *size_p)
{
    int count = 0;
    int i;
    khint_t k;
    static ucs_balancer_element_t elem_arr[UCS_BALANCER_MAX_LRU_SIZE * UCS_BALANCER_MAX_SAMPLES];

    for (k = kh_begin(&ucs_balancer.hash); k != kh_end(&ucs_balancer.hash); ++k) {
        if (kh_exist(&ucs_balancer.hash, k)) {
            elem_arr[count] = kh_val(&ucs_balancer.hash, k);
            count ++;
        }
    }

    qsort(elem_arr, count, sizeof(ucs_balancer_element_t), compare_hit_count);
    for (i = 0; i < count; ++ i) {
        arr_p[i] = elem_arr[i].key;
    }

    *size_p = count;
    kh_clear(aggregator_hash, &ucs_balancer.hash);

//////////////////////////////////////////
//    printf("RC: \n");
//    for (i = 0; i < count; ++ i) {
//        printf("(%p,%lu), ", elem_arr[i].key, elem_arr[i].hit_count);
//    }
//
//    printf("\n");
}



