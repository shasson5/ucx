
#ifndef __LRU_H__
#define __LRU_H__

#include "khash.h"
#include "list.h"
#include "ucs/type/status.h"
#include <stdint.h>


typedef struct ucs_lru *ucs_lru_h;

ucs_lru_h ucs_lru_init(size_t capacity);
ucs_status_t ucs_lru_touch(ucs_lru_h lru, ucs_list_link_t *elem);

#endif
