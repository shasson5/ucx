/**
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2020. ALL RIGHTS RESERVED.
* Copyright (C) Los Alamos National Security, LLC. 2019 ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifndef __LRU_H__
#define __LRU_H__

#include "ucs/type/status.h"
#include <stddef.h>

typedef struct ucs_lru *ucs_lru_h;

ucs_lru_h ucs_lru_init(size_t capacity);
void ucs_lru_destroy(ucs_lru_h lru);
ucs_status_t ucs_lru_touch(ucs_lru_h lru, void *key);
void ucs_lru_get(const ucs_lru_h lru, void **elements, size_t *size);

#endif
