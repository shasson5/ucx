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
    /* TX hit count since the last flush */
    size_t hit_count;
    /* TX hit count which was counted between the 2 last flush operations */
    size_t tx;
    /* RX hit count based on the remote side TX count */
    size_t rx;
    /* Is this entry considered 'active' in terms of traffic */
    int    active;
} ucs_usage_tracker_element_t;


/* Hash table type for Usage Tracker class */
KHASH_INIT(usage_tracker_hash, uint64_t, ucs_usage_tracker_element_t, 1,
           kh_int64_hash_func, kh_int64_hash_equal);


typedef khash_t(usage_tracker_hash) ucs_usage_tracker_hash_t;


/* Callback type for flush notification */
typedef void (*ucs_usage_tracker_flush_cb_t)(void *entry, void *opaque);


/* Usage Tracker main data structure */
typedef struct ucs_usage_tracker {
    /* Hash table of addresses as keys */
    ucs_usage_tracker_hash_t     hash;
    /* LRU cache to track most active entries */
    ucs_lru_h                    lru;
    /* How many ticks will pass between 2 consecutive flush operations */
    unsigned                     ticks_per_flush;
    /* Max number of active entries */
    unsigned                     active_capacity;
    /* Internal tick counter */
    uint64_t                     ticks;
    /* Min percent of max hit count (ticks_per_flush) in order to consider an
     * entry 'active' */
    double                       active_thresh;
    /* Min hit count difference in order to eject an entry from active list */
    unsigned                     eject_thresh;
    /* User callback which will be called when new data is flushed. */
    ucs_usage_tracker_flush_cb_t flush_cb;
    /* User object which will be passed to flush callback. */
    void                        *opaque;
} ucs_usage_tracker_t;


typedef struct ucs_usage_tracker *ucs_usage_tracker_h;


/**
 * @brief Create a new Usage Tracker object.
 *
 * @param [in]    ticks_per_flush  How many ticks will pass between 2
                                   consecutive flush operations
 * @param [in]    active_capacity  Active list capacity
 * @param [in]    active_thresh    Min percent of max hit count in order to
 *                                 consider an entry 'active'
 * @param [in]    eject_thresh     Min hit count difference in order to eject
 *                                 an entry from active list
 * @param [in]    cb               User callback which will be called when new
 *                                 data is flushed.
 * @param [in]    opaque           User object which will be passed to flush
 *                                 callback.
 * @param [out] usage_tracker_p    Pointer to the Usage Tracker struct. Filled
 *                                 with a Usage Tracker handle.
 *
 * @return UCS_OK if successful, or an error code as defined by
 * @ref ucs_status_t otherwise.
 */
ucs_status_t
ucs_usage_tracker_create(unsigned ticks_per_flush, unsigned active_capacity,
                         double active_thresh, unsigned eject_thresh,
                         ucs_usage_tracker_flush_cb_t cb, void *opaque,
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
 * @brief Get most active entries.
 *
 * @param [in]  usage_tracker  Handle to the Usage Tracker object.
 * @param [out] state          Filled with a list of most active entries.
 */
//flush_cb


/**
 * @brief Update an entry with RX usage data.
 *
 * @param [in]  usage_tracker  Handle to the Usage Tracker object.
 * @param [in]  key            Key to insert.
 * @param [in]  score          RX score of the entry.
 *
 * @return Ejected entry in case there is no room in the active list, or NULL
 * otherwise.
 */
void *ucs_usage_tracker_push_rx(ucs_usage_tracker_h usage_tracker, void *key,
                                size_t score);


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
    ucs_lru_put(usage_tracker->lru, key);
}


static UCS_F_ALWAYS_INLINE size_t
ucs_usage_tracker_score(const ucs_usage_tracker_element_t *item)
{
    return ucs_max(item->tx, item->rx);
}


#endif
