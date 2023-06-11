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
#include "list.h"
#include "lru.h"
#include "ucs/debug/memtrack_int.h"

#include <stdint.h>


typedef struct {
    void           *key;
    ucs_list_link_t list;
} ucs_lru_element_t;


__KHASH_TYPE(lru_hash, uint64_t, ucs_lru_element_t);


__KHASH_IMPL(lru_hash, kh_inline, uint64_t, ucs_lru_element_t, 1,
             kh_int64_hash_func, kh_int64_hash_equal);


typedef khash_t(lru_hash) ucs_lru_hash_t;


typedef struct ucs_lru {
    ucs_lru_hash_t  hash;
    ucs_list_link_t list;
    size_t          size;
    size_t          capacity;
} ucs_lru_t;


ucs_lru_h ucs_lru_init(size_t capacity)
{
    ucs_lru_h lru;

    if (capacity == 0) {
        return NULL;
    }

    lru = ucs_calloc(1, sizeof(ucs_lru_t), "ucs_lru");
    if (lru == NULL) {
        return NULL;
    }

    /* Init LRU hash table */
    kh_init_inplace(lru_hash, &lru->hash);

    /* Resize to get the required size. Need to allocate extra space for khash
     * to be happy. */
    if (kh_resize(lru_hash, &lru->hash, capacity * 2) < 0) {
        ucs_free(lru);
        return NULL;
    }

    /* Init LRU linked list */
    ucs_list_head_init(&lru->list);

    /* Init other fields */
    lru->capacity = capacity;
    lru->size     = 0;
    return lru;
}

void ucs_lru_destroy(ucs_lru_h lru)
{
    kh_destroy_inplace(lru_hash, &lru->hash);
    ucs_free(lru);
}

static void ucs_lru_pop(ucs_lru_h lru)
{
    ucs_list_del(lru->list.prev);
}

static void ucs_lru_push(ucs_lru_h lru, ucs_lru_element_t *elem)
{
    ucs_list_add_head(&lru->list, &elem->list);
}

//todo: optimize fast path with ucs_likely.

ucs_status_t ucs_lru_touch(ucs_lru_h lru, void *key)
{
    khint_t iter;
    int ret;
    ucs_lru_element_t *elem;

    iter = kh_put(lru_hash, &lru->hash, (uint64_t)key, &ret);
    if (ret == UCS_KH_PUT_FAILED) {
        //todo: replace error code.
        //todo: maybe should ignore this error because it can never happen.
        return UCS_ERR_NO_RESOURCE;
    }

    elem      = &kh_val(&lru->hash, iter);
    elem->key = key;

    if (ret == UCS_KH_PUT_KEY_PRESENT) {
        ucs_list_del(&elem->list);
    } else if (lru->size == lru->capacity) {
        ucs_lru_pop(lru);
        kh_del(lru_hash, &lru->hash, iter);
    } else {
        lru->size++;
    }

    ucs_lru_push(lru, elem);
    return UCS_OK;
}

void ucs_lru_get(const ucs_lru_h lru, void **elements, size_t *size_p)
{
    ucs_list_link_t *link;

    for (link = lru->list.next; link != &lru->list; link = link->next) {
        *elements = ucs_container_of(link, ucs_lru_element_t, list)->key;
        elements ++;
    }

    *size_p = lru->size;
}
