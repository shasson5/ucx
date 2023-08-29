/**
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2023. ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "usage_tracker.h"

#include <ucs/debug/log.h>
#include <ucs/debug/memtrack_int.h>


ucs_status_t
ucs_usage_tracker_create(unsigned ticks_per_flush, unsigned active_capacity,
                         double active_thresh, unsigned eject_thresh,
                         ucs_usage_tracker_flush_cb_t cb, void *opaque,
                         ucs_usage_tracker_h *usage_tracker_p)
{
    ucs_status_t status;
    ucs_usage_tracker_h usage_tracker;

    usage_tracker = ucs_calloc(1, sizeof(*usage_tracker), "ucs_usage_tracker");
    if (usage_tracker == NULL) {
        ucs_error("failed to allocate Usage Tracker (ticks_per_flush %u, "
                  "capacity: %u, "
                  "active_thresh: %.2f, eject_thresh %u)",
                  ticks_per_flush, active_capacity, active_thresh,
                  eject_thresh);
        status = UCS_ERR_NO_MEMORY;
        goto err;
    }

    status = ucs_lru_create(active_capacity, &usage_tracker->lru);
    if (status != UCS_OK) {
        goto err_free_tracker;
    }

    kh_init_inplace(usage_tracker_hash, &usage_tracker->hash);

    if (kh_resize(usage_tracker_hash, &usage_tracker->hash,
                  active_capacity * 2) < 0) {
        status = UCS_ERR_NO_MEMORY;
        goto err_free_lru;
    }

    usage_tracker->ticks_per_flush = ticks_per_flush;
    usage_tracker->active_capacity = active_capacity;
    usage_tracker->active_thresh   = active_thresh;
    usage_tracker->eject_thresh    = eject_thresh;
    usage_tracker->flush_cb        = cb;
    usage_tracker->opaque          = opaque;
    usage_tracker->ticks           = 0;
    *usage_tracker_p               = usage_tracker;
    return UCS_OK;

err_free_lru:
    ucs_lru_destroy(usage_tracker->lru);
err_free_tracker:
    ucs_free(usage_tracker);
err:
    return status;
}

void ucs_usage_tracker_destroy(ucs_usage_tracker_h usage_tracker)
{
    ucs_lru_destroy(usage_tracker->lru);
    kh_destroy_inplace(usage_tracker_hash, &usage_tracker->hash);
    ucs_free(usage_tracker);
}

static ucs_usage_tracker_element_t *
ucs_usage_tracker_put(ucs_usage_tracker_h usage_tracker, void *key)
{
    int ret;
    khint_t iter;
    ucs_usage_tracker_element_t *elem;

    iter = kh_put(usage_tracker_hash, &usage_tracker->hash, (uint64_t)key,
                  &ret);
    ucs_assert(ret != UCS_KH_PUT_FAILED);

    elem      = &kh_val(&usage_tracker->hash, iter);
    elem->key = key;

    if ((ret == UCS_KH_PUT_BUCKET_EMPTY) || (ret == UCS_KH_PUT_BUCKET_CLEAR)) {
        elem->hit_count = 0;
        elem->tx        = 0;
        elem->rx        = 0;
        elem->active    = 0;
    }

    elem->hit_count++;
    return elem;
}

ucs_status_t ucs_usage_tracker_get_score(ucs_usage_tracker_h usage_tracker,
                                         void *key, size_t *score_p)
{
    ucs_usage_tracker_element_t *item;
    khint_t iter;

    iter = kh_get(usage_tracker_hash, &usage_tracker->hash, (uint64_t)key);
    if (iter == kh_end(&usage_tracker->hash)) {
        return UCS_ERR_NO_ELEM;
    }

    item     = &kh_value(&usage_tracker->hash, iter);
    *score_p = ucs_usage_tracker_score(item);
    return UCS_OK;
}

static ucs_usage_tracker_element_t *
ucs_usage_tracker_get_min_active(ucs_usage_tracker_h usage_tracker)
{
    ucs_usage_tracker_element_t *min_item = NULL;
    unsigned active_count                 = 0;
    ucs_usage_tracker_element_t *item;
    khint_t k;

    for (k = kh_begin(&usage_tracker->hash); k != kh_end(&usage_tracker->hash);
         ++k) {
        if (!kh_exist(&usage_tracker->hash, k)) {
            continue;
        }

        item = &kh_val(&usage_tracker->hash, k);

        if (!item->active) {
            continue;
        }

        active_count++;

        if ((min_item == NULL) || (ucs_usage_tracker_score(item) <
                                   ucs_usage_tracker_score(min_item))) {
            min_item = item;
        }
    }

    return (active_count == usage_tracker->active_capacity) ? min_item : NULL;
}

static ucs_usage_tracker_element_t *
ucs_usage_tracker_pushpop_active(ucs_usage_tracker_h usage_tracker,
                                 ucs_usage_tracker_element_t *elem)
{
    ucs_usage_tracker_element_t *min_item;

    min_item     = ucs_usage_tracker_get_min_active(usage_tracker);
    elem->active = 1;

    if (min_item != NULL) {
        min_item->active = 0;
    }

    return min_item;
}

ucs_status_t
ucs_usage_tracker_remove(ucs_usage_tracker_h usage_tracker, void *key)
{
    khint_t iter;

    iter = kh_get(usage_tracker_hash, &usage_tracker->hash, (uint64_t)key);
    if (iter == kh_end(&usage_tracker->hash)) {
        return UCS_ERR_NO_ELEM;
    }

    kh_del(usage_tracker_hash, &usage_tracker->hash, iter);
    return UCS_OK;
}

void *ucs_usage_tracker_push_rx(ucs_usage_tracker_h usage_tracker, void *key,
                                size_t score)
{
    ucs_usage_tracker_element_t *element, *ejected;
    element     = ucs_usage_tracker_put(usage_tracker, key);
    element->rx = score;

    ejected = NULL;
    if (!element->active) {
        ejected = ucs_usage_tracker_pushpop_active(usage_tracker, element);
    }

    return ejected;
}

static int
ucs_usage_tracker_is_important(ucs_usage_tracker_h usage_tracker, size_t score)
{
    ucs_usage_tracker_element_t *min_item;

    if (score <=
        usage_tracker->active_thresh * usage_tracker->ticks_per_flush) {
        return 0;
    }

    min_item = ucs_usage_tracker_get_min_active(usage_tracker);
    if (min_item == NULL) {
        return 1;
    }

    return (score - ucs_usage_tracker_score(min_item)) >
           usage_tracker->eject_thresh;
}

static void ucs_usage_tracker_flush_tx(ucs_usage_tracker_h usage_tracker)
{
    khint_t k;
    ucs_usage_tracker_element_t *elem;

    for (k = kh_begin(&usage_tracker->hash); k != kh_end(&usage_tracker->hash);
         ++k) {
        if (!kh_exist(&usage_tracker->hash, k)) {
            continue;
        }

        elem            = &kh_val(&usage_tracker->hash, k);
        elem->tx        = elem->hit_count;
        elem->hit_count = 0;
    }
}

void ucs_usage_tracker_get(ucs_usage_tracker_h usage_tracker)
{
    ucs_usage_tracker_element_t *item;
    khint_t k;

    for (k = kh_begin(&usage_tracker->hash); k != kh_end(&usage_tracker->hash);
         ++k) {
        if (!kh_exist(&usage_tracker->hash, k)) {
            continue;
        }

        item = &kh_val(&usage_tracker->hash, k);

        if (!item->active) {
            continue;
        }

        usage_tracker->flush_cb(item->key, usage_tracker->opaque);
    }
}

static void ucs_usage_tracker_flush(ucs_usage_tracker_h usage_tracker)
{
    khint_t k;
    ucs_usage_tracker_element_t *elem;

    ucs_usage_tracker_flush_tx(usage_tracker);

    for (k = kh_begin(&usage_tracker->hash); k != kh_end(&usage_tracker->hash);
         ++k) {
        if (!kh_exist(&usage_tracker->hash, k)) {
            continue;
        }

        elem = &kh_val(&usage_tracker->hash, k);
        if (elem->active) {
            continue;
        }

        if (!ucs_usage_tracker_is_important(usage_tracker,
                                            ucs_usage_tracker_score(elem))) {
            continue;
        }

        ucs_usage_tracker_pushpop_active(usage_tracker, elem);
    }

    if (usage_tracker->flush_cb != NULL) {
        ucs_usage_tracker_get(usage_tracker);
    }
}

void ucs_usage_tracker_tick(ucs_usage_tracker_h usage_tracker)
{
    void **item;

    ucs_lru_for_each(item, usage_tracker->lru) {
        ucs_usage_tracker_put(usage_tracker, *item);
    }

    usage_tracker->ticks++;

    if ((usage_tracker->ticks % usage_tracker->ticks_per_flush) == 0) {
        ucs_usage_tracker_flush(usage_tracker);
        ucs_lru_reset(usage_tracker->lru);
        usage_tracker->ticks = 0;
    }
}
