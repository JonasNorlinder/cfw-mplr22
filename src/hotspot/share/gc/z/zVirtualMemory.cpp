/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

#include "precompiled.hpp"
#include "gc/shared/gcLogPrecious.hpp"
#include "gc/z/zAddress.inline.hpp"
#include "gc/z/zAddressSpaceLimit.hpp"
#include "gc/z/zGlobals.hpp"
#include "gc/z/zVirtualMemory.inline.hpp"
#include "services/memTracker.hpp"
#include "utilities/align.hpp"
#include "utilities/debug.hpp"

ZVirtualMemoryManager::ZVirtualMemoryManager(size_t max_capacity) :
    _manager(),
    _reserved(0),
    _initialized(false) {

  // Check max supported heap size
  if (max_capacity > ZAddressOffsetMax) {
    log_error_p(gc)("Java heap too large (max supported heap size is " SIZE_FORMAT "G)",
                    ZAddressOffsetMax / G);
    return;
  }

  // Initialize platform specific parts before reserving address space
  pd_initialize_before_reserve();

  // Reserve address space
  if (!reserve(max_capacity)) {
    log_error_pd(gc)("Failed to reserve enough address space for Java heap");
    return;
  }

  // Initialize platform specific parts after reserving address space
  pd_initialize_after_reserve();

  // Successfully initialized
  _initialized = true;
}

size_t ZVirtualMemoryManager::reserve_discontiguous(zoffset start, size_t size, size_t min_range) {
  if (size < min_range) {
    // Too small
    return 0;
  }

  assert(is_aligned(size, ZGranuleSize), "Misaligned");

  if (reserve_contiguous(start, size)) {
    return size;
  }

  const size_t half = size / 2;
  if (half < min_range) {
    // Too small
    return 0;
  }

  // Divide and conquer
  const size_t first_part = align_down(half, ZGranuleSize);
  const size_t second_part = size - first_part;
  return reserve_discontiguous(start, first_part, min_range) +
         reserve_discontiguous(start + first_part, second_part, min_range);
}

size_t ZVirtualMemoryManager::reserve_discontiguous(size_t size) {
  // Don't try to reserve address ranges smaller than 1% of the requested size.
  // This avoids an explosion of reservation attempts in case large parts of the
  // address space is already occupied.
  const size_t min_range = align_up(size / 100, ZGranuleSize);
  uintptr_t start = 0;
  size_t reserved = 0;

  // Reserve size somewhere between [0, ZAddressOffsetMax)
  while (reserved < size && start < ZAddressOffsetMax) {
    const size_t remaining = MIN2(size - reserved, ZAddressOffsetMax - start);
    reserved += reserve_discontiguous(to_zoffset(start), remaining, min_range);
    start += remaining;
  }

  return reserved;
}

bool ZVirtualMemoryManager::reserve_contiguous(zoffset start, size_t size) {
  assert(is_aligned(size, ZGranuleSize), "Must be granule aligned");

  // Reserve address views
  const zaddress_unsafe addr = ZOffset::address_unsafe(start);

  // Reserve address space
  if (!pd_reserve(addr, size)) {
    return false;
  }

  // Register address views with native memory tracker
  nmt_reserve(addr, size);

  // Make the address range free
  _manager.free(start, size);

  return true;
}

bool ZVirtualMemoryManager::reserve_contiguous(size_t size) {
  // Allow at most 8192 attempts spread evenly across [0, ZAddressOffsetMax)
  const size_t unused = ZAddressOffsetMax - size;
  const size_t increment = MAX2(align_up(unused / 8192, ZGranuleSize), ZGranuleSize);

  for (zoffset start = zoffset(0); start + size <= zoffset(ZAddressOffsetMax); start += increment) {
    if (reserve_contiguous(start, size)) {
      // Success
      return true;
    }
  }

  // Failed
  return false;
}

bool ZVirtualMemoryManager::reserve(size_t max_capacity) {
  const size_t limit = MIN2(ZAddressOffsetMax, ZAddressSpaceLimit::heap());
  const size_t size = MIN2(max_capacity * ZVirtualToPhysicalRatio, limit);

  size_t reserved = size;
  bool contiguous = true;

  // Prefer a contiguous address space
  if (!reserve_contiguous(size)) {
    // Fall back to a discontiguous address space
    reserved = reserve_discontiguous(size);
    contiguous = false;
  }

  log_info_p(gc, init)("Address Space Type: %s/%s/%s",
                       (contiguous ? "Contiguous" : "Discontiguous"),
                       (limit == ZAddressOffsetMax ? "Unrestricted" : "Restricted"),
                       (reserved == size ? "Complete" : "Degraded"));
  log_info_p(gc, init)("Address Space Size: " SIZE_FORMAT "M", reserved / M);

  // Record reserved
  _reserved = reserved;

  return reserved >= max_capacity;
}

void ZVirtualMemoryManager::nmt_reserve(zaddress_unsafe start, size_t size) {
  MemTracker::record_virtual_memory_reserve((void*)untype(start), size, CALLER_PC);
  MemTracker::record_virtual_memory_type((void*)untype(start), mtJavaHeap);
}

bool ZVirtualMemoryManager::is_initialized() const {
  return _initialized;
}

ZVirtualMemory ZVirtualMemoryManager::alloc(size_t size, bool force_low_address) {
  zoffset start;

  // Small pages are allocated at low addresses, while medium/large pages
  // are allocated at high addresses (unless forced to be at a low address).
  if (force_low_address || size <= ZPageSizeSmall) {
    start = _manager.alloc_low_address(size);
  } else {
    start = _manager.alloc_high_address(size);
  }

  return ZVirtualMemory(start, size);
}

void ZVirtualMemoryManager::free(const ZVirtualMemory& vmem) {
  _manager.free(vmem.start(), vmem.size());
}
