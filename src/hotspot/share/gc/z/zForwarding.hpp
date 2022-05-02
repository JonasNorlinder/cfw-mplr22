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

#ifndef SHARE_GC_Z_ZFORWARDING_HPP
#define SHARE_GC_Z_ZFORWARDING_HPP

#include "gc/z/zAttachedArray.hpp"
#include "gc/z/zForwardingEntry.hpp"
#include "gc/z/zGenerationId.hpp"
#include "gc/z/zPageAge.hpp"
#include "gc/z/zPageType.hpp"
#include "gc/z/zVirtualMemory.hpp"

class ObjectClosure;
class ZForwardingAllocator;
class ZPage;

typedef size_t ZForwardingCursor;

class ZCompactForwarding {
  friend class VMStructs;
  friend class ZForwardingTest;
  friend class ZCompactForwardingEntry;
  friend class ZCompactRelocationSetInstallTask;

public:
  zaddress               _first_on_snd_page = zaddress::null;
  ZCompactForwardingEntry* _first_on_snd_page_entry = NULL;
  bool                   _need_snd_page = false;
  const ZVirtualMemory   _virtual;
  const size_t           _object_alignment_shift;
  ZCompactForwardingEntry* _compact_entries;
  ZPartialEntry* _partial_entries;
  ZPage* const           _page;
  ZCompactForwarding*           _prev;
  ZCompactForwarding*           _next = NULL;
  const uint32_t         _live_objects;
  ZGenerationId          _generation_id;
  ZPageAge               _age_from;
  ZPageAge               _age_to;
  volatile bool          _claimed;
  mutable ZConditionLock _ref_lock;
  volatile int32_t       _ref_count;
  bool                   _ref_abort;
  bool                   _remset_scanned;
  bool                   _in_place = false;

  const uintptr_t to_page_start(const zaddress addr) const;
  const uintptr_t to_page_start(const ZCompactForwardingEntry* entry) const;
  void install_to_page(ZPage* to_page);

  // In-place relocation support
  uintptr_t              _in_place_clear_remset_watermark;
  zoffset                _in_place_top_at_start;

  // Debugging
  volatile Thread*       _in_place_thread;

  template <typename Function>
  void fragment_iterate(Function function);
  template <typename Function>
  void object_iterate_forwarded_via_livemap(Function function);
  template <typename Function>
  void object_iterate_forwarded_via_table(Function function);

  // Compact forwarding
  size_t _top = 0; // In object sizes. For lazy allocation
  ZPage* _to_fst = NULL;
  ZPage* _to_snd = NULL;
  ZPage* _to_in_place = NULL;

  const size_t addr_to_index(zaddress addr) const;
  size_t addr_to_internal_index(zaddress addr) const;

  inline void set_partial(zaddress addr) {
    const size_t external_index = ((ZAddress::offset(addr) - _virtual.start()) >> 8);
    const size_t internal_index = addr_to_internal_index(addr);
    _partial_entries[external_index].set_partial(internal_index);
  };

  inline const bool get_partial(zaddress addr) {
    const size_t external_index = ((ZAddress::offset(addr) - _virtual.start()) >> 8);
    const size_t internal_index = addr_to_internal_index(addr);
    return _partial_entries[external_index].get_partial(internal_index);
  };

  size_t offset_to_index_raw(zaddress addr) const;

  bool has_snd_page() const;
  ZPage* get_first(bool gc_thread);
  ZPage* get_first_inner(bool gc_thread);
  ZPage* get_second(bool gc_thread);
  ZPage* get_last(bool gc_thread);
  ZPage* install_page(ZPage** p, bool gc_thread);
  void   populate_compact_entries();
  void   install_to_page();

  bool           _is_last       = false;
  volatile bool  _taken         = false;

public:
  const size_t top() const;
  const bool is_last() const;
  const size_t last_to_page_top() const;
  void mark_as_last();
  const inline bool is_first();

  const zaddress to_addr(const zaddress from_addr);

  // CFW v2
  const bool has_to_page(const zaddress addr) const;
  const bool has_to_page(const ZCompactForwardingEntry* entry) const;
  ZCompactForwardingEntry* const find_raw(zaddress from_addr) const;
  ZCompactForwardingEntry* const find(zaddress from_addr);
  zaddress from(const size_t entry_index, const size_t internal_index) const;
  const zaddress to(const zaddress from_addr);
  const zaddress to(const zaddress from_addr, const ZCompactForwardingEntry* entry);
  const zaddress to_offset(const zaddress from_addr, const ZCompactForwardingEntry* entry) const;
  bool is_on_snd_page(const zaddress from_offset) const;
  bool is_on_page_break(const ZCompactForwardingEntry *entry);

  ZCompactForwarding(ZPage* page, bool to_old, ZCompactForwarding* prev);
  ~ZCompactForwarding() {
    free(_compact_entries);
    free(_partial_entries);
  }

  static uint32_t nentries(const ZPage* page);
  static ZPageAge compute_age_to(ZPageAge age_from, bool promote_all);

  ZPageType type() const;
  ZPageAge from_age() const;
  ZPageAge to_age() const;
  zoffset start() const;
  size_t size() const;
  size_t object_alignment_shift() const;

  bool is_promotion() const;

  // Visit from-objects
  template <typename Function>
  void object_iterate(Function function);

  // Visit to-objects
  template <typename Function>
  void object_iterate_forwarded(Function function);

  template <typename Function>
  void oops_do_in_forwarded(Function function);

  template <typename Function>
  void oops_do_in_forwarded_via_table(Function function);

  bool claim();

  // In-place relocation support
  bool in_place_relocation() const;
  void in_place_relocation_claim_page();
  void in_place_relocation_start();
  void in_place_relocation_finish();
  bool in_place_relocation_is_below_top_at_start(zoffset addr) const;
  void in_place_relocation_clear_remset_up_to(uintptr_t local_offset) const;
  void in_place_relocation_set_clear_remset_watermark(uintptr_t local_offset);

  bool retain_page();
  void release_page();
  bool wait_page_released() const;

  ZPage* detach_page();
  ZPage* page();
  void abort_page();

  void verify() const;

  bool get_and_set_remset_scanned();
};

class ZForwarding {
  friend class VMStructs;
  friend class ZForwardingTest;

private:
  typedef ZAttachedArray<ZForwarding, ZForwardingEntry> AttachedArray;

  const ZVirtualMemory   _virtual;
  const size_t           _object_alignment_shift;
  const AttachedArray    _entries;
  ZPage* const           _page;
  ZPageAge               _from_age;
  ZPageAge               _to_age;
  volatile bool          _claimed;
  mutable ZConditionLock _ref_lock;
  volatile int32_t       _ref_count;
  bool                   _ref_abort;
  bool                   _remset_scanned;

  // In-place relocation support
  bool                   _in_place;
  uintptr_t              _in_place_clear_remset_watermark;
  zoffset                _in_place_top_at_start;

  // Debugging
  volatile Thread*       _in_place_thread;

  ZForwardingEntry* entries() const;
  ZForwardingEntry at(ZForwardingCursor* cursor) const;
  ZForwardingEntry first(uintptr_t from_index, ZForwardingCursor* cursor) const;
  ZForwardingEntry next(ZForwardingCursor* cursor) const;

  template <typename Function>
  void object_iterate_forwarded_via_livemap(Function function);
  template <typename Function>
  void object_iterate_forwarded_via_table(Function function);

  ZForwarding(ZPage* page, ZPageAge to_age, size_t nentries);

public:
  static uint32_t nentries(const ZPage* page);
  static ZForwarding* alloc(ZForwardingAllocator* allocator, ZPage* page, ZPageAge to_age);

  ZPageType type() const;
  ZPageAge from_age() const;
  ZPageAge to_age() const;
  zoffset start() const;
  size_t size() const;
  size_t object_alignment_shift() const;

  bool is_promotion() const;

  // Visit from-objects
  template <typename Function>
  void object_iterate(Function function);

  // Visit to-objects
  template <typename Function>
  void object_iterate_forwarded(Function function);

  template <typename Function>
  void oops_do_in_forwarded(Function function);

  template <typename Function>
  void oops_do_in_forwarded_via_table(Function function);

  bool claim();

  // In-place relocation support
  bool in_place_relocation() const;
  void in_place_relocation_claim_page();
  void in_place_relocation_start();
  void in_place_relocation_finish();
  bool in_place_relocation_is_below_top_at_start(zoffset addr) const;
  void in_place_relocation_clear_remset_up_to(uintptr_t local_offset) const;
  void in_place_relocation_set_clear_remset_watermark(uintptr_t local_offset);

  bool retain_page();
  void release_page();
  bool wait_page_released() const;

  ZPage* detach_page();
  ZPage* page();
  void abort_page();

  zaddress find(zaddress_unsafe addr);

  ZForwardingEntry find(uintptr_t from_index, ZForwardingCursor* cursor) const;
  zoffset insert(uintptr_t from_index, zoffset to_offset, ZForwardingCursor* cursor);

  void verify() const;

  bool get_and_set_remset_scanned();
};

#endif // SHARE_GC_Z_ZFORWARDING_HPP
