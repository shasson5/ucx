/**
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2020. ALL RIGHTS RESERVED.
* Copyright (C) Los Alamos National Security, LLC. 2019 ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifndef __RCDC_BALANCER_H__
#define __RCDC_BALANCER_H__

#include "ucs/type/status.h"
#include <stdint.h>

ucs_status_t ucs_balancer_init(uint32_t interval, unsigned ticks_per_flush);
void ucs_balancer_aggregate();
void ucs_balancer_destroy();
unsigned ucs_balancer_add(void *element, unsigned id);
void ucs_balancer_flush(void **arr_p, size_t *size_p);


#endif
