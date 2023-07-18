/**
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2020. ALL RIGHTS RESERVED.
* Copyright (C) Los Alamos National Security, LLC. 2019 ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifndef __RCDC_BALANCER_H__
#define __RCDC_BALANCER_H__

#include <stdint.h>

#define UCS_BALANCER_MAX_LRU_SIZE 5

ucs_status_t ucs_balancer_init(uint32_t interval, unsigned ticks_per_flush, unsigned rc_size);
void ucs_balancer_aggregate();
void ucs_balancer_destroy();
int  ucs_balancer_add(void *element);
void ucs_balancer_flush();
void ucs_balancer_get(void **arr_p, size_t *size_p);
int ucs_balancer_is_important(size_t score);
void *ucs_balancer_push_rx(void *key, size_t score);
size_t ucs_balancer_get_score(void *key);

#endif
