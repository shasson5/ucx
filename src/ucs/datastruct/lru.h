
#ifndef __LRU_H__
#define __LRU_H__

#include "khash.h"
#include "list.h"
#include "ucs/type/status.h"
#include <stdint.h>


typedef struct ucs_lru *ucs_lru_h;

ucs_lru_h ucs_lru_init(size_t capacity);
void ucs_lru_destroy(ucs_lru_h lru);
ucs_status_t ucs_lru_touch(ucs_lru_h lru, void *key);
void ucs_lru_get(const ucs_lru_h lru, void **elements, size_t *size);

#endif
