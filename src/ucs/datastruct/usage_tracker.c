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


ucs_status_t ucs_usage_tracker_create(const ucs_usage_tracker_params_t *params,
                                      ucs_usage_tracker_h *usage_tracker_p)
{
    ucs_status_t status;
    ucs_usage_tracker_h usage_tracker;

    if (params->flush_cb == NULL) {
        status = UCS_ERR_INVALID_PARAM;
        goto err;
    }

    usage_tracker = ucs_calloc(1, sizeof(*usage_tracker), "ucs_usage_tracker");
    if (usage_tracker == NULL) {
        ucs_error("failed to allocate usage tracker");
        status = UCS_ERR_NO_MEMORY;
        goto err;
    }

    status = ucs_lru_create(params->active_capacity, &usage_tracker->lru);
    if (status != UCS_OK) {
        goto err_free_tracker;
    }

    kh_init_no_shrink(usage_tracker_hash, &usage_tracker->hash);

    if (kh_resize(usage_tracker_hash, &usage_tracker->hash,
                  params->active_capacity * 2) < 0) {
        ucs_error("failed to resize usage tracker hash table: "
                  "active_capacity=%u", params->active_capacity);
        status = UCS_ERR_NO_MEMORY;
        goto err_free_lru;
    }

    usage_tracker->params = *params;
    usage_tracker->ticks  = 0;
    *usage_tracker_p      = usage_tracker;

    return UCS_OK;

err_free_lru:
    ucs_lru_destroy(usage_tracker->lru);
    kh_destroy_inplace(usage_tracker_hash, &usage_tracker->hash);
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

/* Return entries's score. */
static UCS_F_ALWAYS_INLINE size_t
ucs_usage_tracker_score(const ucs_usage_tracker_element_t *item)
{
    return ucs_max(item->score, item->min_score);
}

/* Update an entry in hash table and increment its hit count. */
static ucs_usage_tracker_element_t *
ucs_usage_tracker_put(ucs_usage_tracker_h usage_tracker, void *key)
{
    int khret;
    khiter_t iter;
    ucs_usage_tracker_element_t *elem;

    iter = kh_put(usage_tracker_hash, &usage_tracker->hash, (uint64_t)key,
                  &khret);
    ucs_assert(khret != UCS_KH_PUT_FAILED);

    elem      = &kh_val(&usage_tracker->hash, iter);
    elem->key = key;

    if ((khret == UCS_KH_PUT_BUCKET_EMPTY) ||
        (khret == UCS_KH_PUT_BUCKET_CLEAR)) {
        elem->hit_count = 0;
        elem->score     = 0;
        elem->min_score = 0;
        elem->active    = 0;
    }

    elem->hit_count++;
    return elem;
}

ucs_status_t ucs_usage_tracker_get_score(ucs_usage_tracker_h usage_tracker,
                                         void *key, size_t *score_p)
{
    ucs_usage_tracker_element_t *item;
    khiter_t iter;

    iter = kh_get(usage_tracker_hash, &usage_tracker->hash, (uint64_t)key);
    if (iter == kh_end(&usage_tracker->hash)) {
        return UCS_ERR_NO_ELEM;
    }

    item     = &kh_value(&usage_tracker->hash, iter);
    *score_p = ucs_usage_tracker_score(item);
    return UCS_OK;
}

/* Return number of entries in active list. */
static unsigned
ucs_usage_tracker_get_active_count(ucs_usage_tracker_h usage_tracker)
{
    unsigned active_count = 0;
    ucs_usage_tracker_element_t *item;
    khiter_t k;

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
    }

    return active_count;
}

/* Find and return the lowest score entry in the active list.
 * If active list is not yet full capacity, returns NULL. */
static ucs_usage_tracker_element_t *
ucs_usage_tracker_get_min_active(ucs_usage_tracker_h usage_tracker)
{
    ucs_usage_tracker_element_t *min_item = NULL;
    ucs_usage_tracker_params_t *params    = usage_tracker->params;
    ucs_usage_tracker_element_t *item;
    khiter_t k;
    unsigned active_count;

    for (k = kh_begin(&usage_tracker->hash); k != kh_end(&usage_tracker->hash);
         ++k) {
        if (!kh_exist(&usage_tracker->hash, k)) {
            continue;
        }

        item = &kh_val(&usage_tracker->hash, k);
        if (!item->active) {
            continue;
        }

        if ((min_item == NULL) || (ucs_usage_tracker_score(item) <
                                   ucs_usage_tracker_score(min_item))) {
            min_item = item;
        }
    }

    active_count = ucs_usage_tracker_get_active_count(usage_tracker);
    return (active_count == params->active_capacity) ? min_item : NULL;
}

/* Insert a new entry to the active list and possibly eject the entry with
 * the lowest score. */
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
    khiter_t iter;

    iter = kh_get(usage_tracker_hash, &usage_tracker->hash, (uint64_t)key);
    if (iter == kh_end(&usage_tracker->hash)) {
        return UCS_ERR_NO_ELEM;
    }

    kh_del(usage_tracker_hash, &usage_tracker->hash, iter);
    return UCS_OK;
}

void *ucs_usage_tracker_push_min_score(ucs_usage_tracker_h usage_tracker,
                                       void *key, size_t score)
{
    ucs_usage_tracker_element_t *element, *ejected;

    element            = ucs_usage_tracker_put(usage_tracker, key);
    element->min_score = score;
    ejected            = NULL;

    if (!element->active) {
        ejected = ucs_usage_tracker_pushpop_active(usage_tracker, element);
    }

    return ejected;
}

/* Checks if an entry has high enough score to enter the active list. */
static int
ucs_usage_tracker_is_important(ucs_usage_tracker_h usage_tracker, size_t score)
{
    const ucs_usage_tracker_params_t *params = &usage_tracker->params;
    ucs_usage_tracker_element_t *min_item;

    if (score <= (params->active_thresh * params->ticks_per_flush)) {
        return 0;
    }

    min_item = ucs_usage_tracker_get_min_active(usage_tracker);
    if (min_item == NULL) {
        return 1;
    }

    return (score - ucs_usage_tracker_score(min_item)) > params->eject_thresh;
}

/* Update entry's score from last hit count and reset hit count */
static void ucs_usage_tracker_flush_score(ucs_usage_tracker_h usage_tracker)
{
    khiter_t k;
    ucs_usage_tracker_element_t *elem;

    for (k = kh_begin(&usage_tracker->hash); k != kh_end(&usage_tracker->hash);
         ++k) {
        if (!kh_exist(&usage_tracker->hash, k)) {
            continue;
        }

        elem            = &kh_val(&usage_tracker->hash, k);
        elem->score     = elem->hit_count;
        elem->hit_count = 0;
    }
}

void ucs_usage_tracker_get(ucs_usage_tracker_h usage_tracker)
{
    const ucs_usage_tracker_params_t *params = &usage_tracker->params;
    ucs_usage_tracker_element_t *item;
    khiter_t k;

    for (k = kh_begin(&usage_tracker->hash); k != kh_end(&usage_tracker->hash);
         ++k) {
        if (!kh_exist(&usage_tracker->hash, k)) {
            continue;
        }

        item = &kh_val(&usage_tracker->hash, k);

        if (!item->active) {
            continue;
        }

        params->flush_cb(item->key, params->flush_arg);
    }
}

/* Performs a flush operation.
 * Active list will be updated with the new results. */
static void ucs_usage_tracker_flush(ucs_usage_tracker_h usage_tracker)
{
    khiter_t k;
    ucs_usage_tracker_element_t *elem;

    ucs_usage_tracker_flush_score(usage_tracker);

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

    ucs_usage_tracker_get(usage_tracker);
}

void ucs_usage_tracker_tick(ucs_usage_tracker_h usage_tracker)
{
    void **item;

    ucs_lru_for_each(item, usage_tracker->lru) {
        ucs_usage_tracker_put(usage_tracker, *item);
    }

    usage_tracker->ticks++;

    if ((usage_tracker->ticks % usage_tracker->params.ticks_per_flush) == 0) {
        ucs_usage_tracker_flush(usage_tracker);
        ucs_lru_reset(usage_tracker->lru);
        usage_tracker->ticks = 0;
    }
}
