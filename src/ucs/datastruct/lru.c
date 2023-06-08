/**
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2020. ALL RIGHTS RESERVED.
* Copyright (C) Los Alamos National Security, LLC. 2019 ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "list.h"
#include "lru.h"
#include "ucs/debug/memtrack_int.h"

#include <stdint.h>


typedef struct {
    void           *key;
    unsigned        id;
    ucs_list_link_t list;
} ucs_lru_element_t;


typedef struct ucs_lru {
    ucs_list_link_t   list;
    size_t            size;
    size_t            capacity;
    ucs_lru_element_t array[0];
} ucs_lru_t;


ucs_lru_h ucs_lru_init(size_t capacity)
{
    ucs_lru_h lru;

    if (capacity == 0) {
        return NULL;
    }

    lru = ucs_calloc(1, sizeof(ucs_lru_t) + sizeof(ucs_lru_element_t) * capacity, "ucs_lru");
    if (lru == NULL) {
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

unsigned ucs_lru_touch(ucs_lru_h lru, unsigned id, void *opaque)
{
    ucs_lru_element_t *elem = &lru->array[id];

    if (elem->key == opaque) {
        ucs_list_del(&elem->list);
    } else if (lru->size == lru->capacity) {
        id = ucs_container_of(lru->list.prev, ucs_lru_element_t, list)->id;
        ucs_lru_pop(lru);
    } else {
        id = lru->size;
        lru->size++;
    }

    elem = &lru->array[id];
    elem->key = opaque;
    ucs_lru_push(lru, elem);
    return id;
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
