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
#include <ucs/sys/compiler.h>
#include <ucs/sys/math.h>

#define UCS_USAGE_TRACKER_VERIFY_SCORE_PARAM(_param) \
    if (_param > 1 || _param < 0) { \
        ucs_error("##param must be in the range [0-1] (actual=%.2f)", _param); \
        status = UCS_ERR_INVALID_PARAM; \
        goto err; \
    }

ucs_status_t ucs_usage_tracker_create(const ucs_usage_tracker_params_t *params,
                                      ucs_usage_tracker_h *usage_tracker_p)
{
    ucs_status_t status;
    ucs_usage_tracker_h usage_tracker;

    if ((params->promote_cb == NULL) || (params->demote_cb == NULL)) {
        ucs_error("got NULL pointers in callbacks arguments (promote_cb=%p, "
                  "demote_cb=%p)",
                  params->promote_cb, params->demote_cb);
        status = UCS_ERR_INVALID_PARAM;
        goto err;
    }

    if (params->promote_thresh > params->promote_capacity) {
        ucs_error("promote thresh must be smaller than promote capacity "
                  "(promote_thresh=%u, promote_capacity=%u)",
                  params->promote_thresh, params->promote_capacity);
        status = UCS_ERR_INVALID_PARAM;
        goto err;
    }

    UCS_USAGE_TRACKER_VERIFY_SCORE_PARAM(params->remove_thresh);
    UCS_USAGE_TRACKER_VERIFY_SCORE_PARAM(params->exp_decay.a);
    UCS_USAGE_TRACKER_VERIFY_SCORE_PARAM(params->exp_decay.b);

    usage_tracker = ucs_malloc(sizeof(*usage_tracker), "ucs_usage_tracker");
    if (usage_tracker == NULL) {
        ucs_error("failed to allocate usage tracker");
        status = UCS_ERR_NO_MEMORY;
        goto err;
    }

    status = ucs_lru_create(params->promote_capacity, &usage_tracker->lru);
    if (status != UCS_OK) {
        goto err_free_tracker;
    }

    kh_init_inplace(usage_tracker_hash, &usage_tracker->hash);

    usage_tracker->params = *params;
    *usage_tracker_p      = usage_tracker;

    return UCS_OK;

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

/* Return entry's score. */
static UCS_F_ALWAYS_INLINE double
ucs_usage_tracker_score(const ucs_usage_tracker_element_t *item)
{
    return ucs_max(item->score, item->min_score);
}

/* Insert an entry to the hash table and update its score. */
static ucs_usage_tracker_element_t *
ucs_usage_tracker_put(ucs_usage_tracker_h usage_tracker, void *key)
{
    int khret;
    khiter_t iter;
    ucs_usage_tracker_element_t *elem;

    iter = kh_put(usage_tracker_hash, &usage_tracker->hash, (uint64_t)key,
                  &khret);
    ucs_assert(khret != UCS_KH_PUT_FAILED);

    elem = &kh_val(&usage_tracker->hash, iter);

    if ((khret == UCS_KH_PUT_BUCKET_EMPTY) ||
        (khret == UCS_KH_PUT_BUCKET_CLEAR)) {
        elem->score     = usage_tracker->params.exp_decay.b;
        elem->min_score = 0;
        elem->promoted  = 0;
        elem->key       = key;
    }

    return elem;
}

ucs_status_t ucs_usage_tracker_get_score(ucs_usage_tracker_h usage_tracker,
                                         void *key, double *score_p)
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

void ucs_usage_tracker_push_min_score(ucs_usage_tracker_h usage_tracker,
                                      void *key, double score)
{
    ucs_usage_tracker_element_t *element;

    element            = ucs_usage_tracker_put(usage_tracker, key);
    element->min_score = score;
}

/* Checks if an entry has high enough score to get promoted. */
static int
ucs_usage_tracker_compare(const void *elem1, const void *elem2, void *arg)
{
    const ucs_usage_tracker_params_t *params = arg;
    ucs_usage_tracker_element_t *item1 = *(ucs_usage_tracker_element_t**)elem1;
    ucs_usage_tracker_element_t *item2 = *(ucs_usage_tracker_element_t**)elem2;

    if (fabs(item1->score - item2->score) < params->remove_thresh) {
        return (item1->key > item2->key) ? 1 : -1;
    }

    return (item1->score < item2->score) ? 1 : -1;
}

/* Promote/Demote entires base on the latest score, and triggers user
  * callback accordingly. */
static void ucs_usage_tracker_update_rank(ucs_usage_tracker_h usage_tracker)
{
    const ucs_usage_tracker_params_t *params = &usage_tracker->params;
    size_t elem_index                        = 0;
    ucs_usage_tracker_element_t **elems_array, *item;
    khiter_t iter;
    uint64_t key;

    elems_array = ucs_alloca(sizeof(*elems_array) *
                             (kh_size(&usage_tracker->hash)));

    kh_foreach_key(&usage_tracker->hash, key,
        iter                    = kh_get(usage_tracker_hash,
                                         &usage_tracker->hash, key);
        item                    = &kh_val(&usage_tracker->hash, iter);
        elems_array[elem_index] = item;
        elem_index ++;
    )

        qsort_r(elems_array, kh_size(&usage_tracker->hash),
                sizeof(*elems_array), ucs_usage_tracker_compare,
                (ucs_usage_tracker_params_t*)params);

    for (elem_index = 0; elem_index < kh_size(&usage_tracker->hash);
         ++elem_index) {
        item = elems_array[elem_index];

        if (elem_index < params->promote_thresh) {
            if (item->promoted) {
                continue;
            }

            params->promote_cb(item->key, params->promote_arg);
            item->promoted = 1;
        }

        else if (elem_index >= params->promote_capacity) {
            if (item->promoted) {
                params->demote_cb(item->key, params->demote_arg);
                item->promoted = 0;
            }

            ucs_usage_tracker_remove(usage_tracker, item->key);
        }
    }
}

void ucs_usage_tracker_progress(ucs_usage_tracker_h usage_tracker)
{
    void **item;
    khiter_t iter;
    ucs_usage_tracker_element_t *elem;
    uint64_t key;

    kh_foreach_key(&usage_tracker->hash, key,
        iter = kh_get(usage_tracker_hash, &usage_tracker->hash, key);
        elem = &kh_val(&usage_tracker->hash, iter);

        elem->score *= usage_tracker->params.exp_decay.a;
        if (ucs_lru_has_entry(usage_tracker->lru, (void *)key)) {
            elem->score += usage_tracker->params.exp_decay.b;
        }
    )

    ucs_lru_for_each(item, usage_tracker->lru) {
        ucs_usage_tracker_put(usage_tracker, *item);
    }

    if (kh_size(&usage_tracker->hash) > 0) {
        ucs_usage_tracker_update_rank(usage_tracker);
    }

    ucs_lru_reset(usage_tracker->lru);
}
