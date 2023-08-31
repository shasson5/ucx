/**
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2023. ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifndef UCS_USAGE_TRACKER_H_
#define UCS_USAGE_TRACKER_H_


#include <stdint.h>
#include <stddef.h>


#include <ucs/datastruct/khash.h>
#include <ucs/datastruct/lru.h>
#include <ucs/sys/math.h>


/* Usage Tracker element data structure */
typedef struct {
    /* Key to use as hash table input */
    void  *key;
    /* Hit count since the last flush */
    size_t hit_count;
    /* Hit count which was counted between the 2 last flush operations */
    size_t score;
    /* Min hit count based on the remote side */
    size_t min_score;
    /* Is this entry considered 'active' in terms of traffic */
    int    active;
} ucs_usage_tracker_element_t;


/* Callback type for flush notification */
typedef void (*ucs_usage_tracker_flush_cb_t)(void *entry, void *opaque);


typedef struct {
    /* How many ticks will pass between 2 consecutive flush operations */
    unsigned                     ticks_per_flush;
    /* Max number of active entries */
    unsigned                     active_capacity;
    /* Min percent of max hit count (ticks_per_flush) in order to consider an
     * entry 'active' */
    double                       active_thresh;
    /* Min hit count difference in order to remove an entry from active list */
    unsigned                     remove_thresh;
    /* User callback which will be called when new data is flushed. */
    ucs_usage_tracker_flush_cb_t flush_cb;
    /* User object which will be passed to flush callback. */
    void                        *flush_arg;
} ucs_usage_tracker_params_t;


/* Hash table type for Usage Tracker class */
KHASH_INIT(usage_tracker_hash, uint64_t, ucs_usage_tracker_element_t, 1,
           kh_int64_hash_func, kh_int64_hash_equal);


typedef khash_t(usage_tracker_hash) ucs_usage_tracker_hash_t;


/* Usage Tracker main data structure */
typedef struct ucs_usage_tracker {
    /* Hash table of addresses as keys */
    ucs_usage_tracker_hash_t   hash;
    /* LRU cache to track most active entries */
    ucs_lru_h                  lru;
    /* Internal tick counter */
    uint64_t                   ticks;
    /* Usage Tracker params */
    ucs_usage_tracker_params_t params;
} ucs_usage_tracker_t;


typedef struct ucs_usage_tracker *ucs_usage_tracker_h;


/**
 * @brief Create a new Usage Tracker object.
 *
 * Usage tracking is done by sampling an LRU cache of the most active entries.
 * We sample the LRU several times and sum up the hit count of each entry.
 * After a predefined amount of samples, we flush the results to the 'active
 * list', which contains the most active entries over time.
 * If no room is left on the active list, the least active entries will be
 * removed.
 *
 * @param [in]  params           usage tracker params.
 * @param [out] usage_tracker_p  Pointer to the Usage Tracker struct. Filled
 *                               with a Usage Tracker handle.
 *
 * @return UCS_OK if successful, or an error code as defined by
 * @ref ucs_status_t otherwise.
 */
ucs_status_t ucs_usage_tracker_create(const ucs_usage_tracker_params_t *params,
                                      ucs_usage_tracker_h *usage_tracker_p);


/**
 * @brief Destroys a Usage Tracker object.
 *
 * @param [in] usage_tracker  Handle to the Usage Tracker object.
 */
void ucs_usage_tracker_destroy(ucs_usage_tracker_h usage_tracker);


/**
 * @brief Ticks the usage tracker.
 *
 * Triggers increment of hit counters for all LRU entries.
 * Each @ref ucs_usage_tracker_t::ticks_per_flush ticks, a flush operation is
 * performed.
 *
 * @param [in] usage_tracker  Handle to the Usage Tracker object.
 */
void ucs_usage_tracker_tick(ucs_usage_tracker_h usage_tracker);


/**
 * @brief Update an entry with min score.
 *
 * @param [in]  usage_tracker  Handle to the Usage Tracker object.
 * @param [in]  key            Key to insert.
 * @param [in]  score          Min score of the entry.
 *
 * @return removed entry in case there is no room in the active list, or NULL
 * otherwise.
 */
void *ucs_usage_tracker_push_min_score(ucs_usage_tracker_h usage_tracker,
                                       void *key, size_t score);


/**
 * @brief Get score of a specific entry.
 *
 * @param [in]  usage_tracker  Handle to the Usage Tracker object.
 * @param [in]  key            Key of the entry.
 * @param [out] score_p        Filled with the requested entry's score.
 *
 * @return UCS_OK if successful, or an error code as defined by
 * @ref ucs_status_t otherwise.
 */
ucs_status_t ucs_usage_tracker_get_score(ucs_usage_tracker_h usage_tracker,
                                         void *key, size_t *score_p);


/**
 * @brief Remove an entry from Usage Tracker.
 *
 * @param [in]  usage_tracker  Handle to the Usage Tracker object.
 * @param [in]  key            Key of the entry to be removed.
 *
 * @return UCS_OK if successful, or an error code as defined by
 * @ref ucs_status_t otherwise.
 */
ucs_status_t
ucs_usage_tracker_remove(ucs_usage_tracker_h usage_tracker, void *key);


/**
 * @brief Add an entry to the Usage Tracker.
 *
 * @param [in]  usage_tracker  Handle to the Usage Tracker object.
 * @param [in]  key            Key of the entry to be added.
 */
static UCS_F_ALWAYS_INLINE void
ucs_usage_tracker_add(ucs_usage_tracker_h usage_tracker, void *key)
{
    ucs_lru_push(usage_tracker->lru, key);
}


#endif
