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

#ifndef SHARE_GC_Z_ZFORWARDING_INLINE_HPP
#define SHARE_GC_Z_ZFORWARDING_INLINE_HPP

#include "utilities/align.hpp"
#include "gc/z/zAttachedArray.inline.hpp"
#include "gc/z/zForwarding.hpp"
#include "gc/z/zForwardingEntry.inline.hpp"
#include "gc/z/zUtils.hpp"
#include "gc/z/zForwardingAllocator.inline.hpp"
#include "gc/z/zHash.inline.hpp"
#include "gc/z/zHeap.hpp"
#include "gc/z/zIterator.inline.hpp"
#include "gc/z/zLock.inline.hpp"
#include "gc/z/zPage.inline.hpp"
#include "gc/z/zUtils.inline.hpp"
#include "gc/z/zVirtualMemory.inline.hpp"
#include "gc/shared/gc_globals.hpp"
#include "runtime/atomic.hpp"
#include "utilities/debug.hpp"
#include "utilities/powerOfTwo.hpp"

inline uint32_t ZCompactForwarding::nentries(const ZPage* page) {
  return page->live_objects();
}

inline ZPageAge ZCompactForwarding::compute_age_to(ZPageAge age_from, bool promote_all) {
  // don't promote for now TODO restore
  if (promote_all) {
    return ZPageAge::old;
  } else if (age_from == ZPageAge::eden) {
    return ZPageAge::survivor;
  } else {
    return ZPageAge::old;
  }
}

inline ZCompactForwarding::ZCompactForwarding(ZPage* page, bool to_old, ZCompactForwarding* prev) :
    _virtual(page->virtual_memory()),
    _object_alignment_shift(page->object_alignment_shift()),
    _page(page),
    _prev(prev),
    _live_objects(page->live_objects()),
    _generation_id(page->generation_id()),
    _age_from(page->age()),
    _age_to(to_old ? ZPageAge::old : ZPageAge::survivor),
    _claimed(false),
    _ref_lock(),
    _ref_count(1),
    _ref_abort(false),
    _remset_scanned(false),
    _in_place_clear_remset_watermark(0),
    _in_place_top_at_start(),
    _in_place_thread(NULL) {
  if (prev) {
    prev->_next = this;
    assert(prev->_age_to == _age_to, "inconsistent to age");
  }

  // v2
  // Use each bit in a 32 bit int to store liveness information,
  // where each bit spans one word in the page. One word = 8 bytes.
  //
  // Example: Page size is small: 2 MB,
  // then requiered number of entries to describe the liveness
  // is 2 MB / (32 words * 8 bytes) = 8 192
  const size_t nentries = page->size() / 256 + 1; // One extra entry for page break
  _compact_entries = static_cast<ZCompactForwardingEntry*>(calloc(nentries, sizeof(ZCompactForwardingEntry)));
  this->populate_compact_entries();
}

inline ZPageType ZCompactForwarding::type() const {
  return _page->type();
}


inline ZPageAge ZCompactForwarding::from_age() const {
  return _age_from;
}

inline ZPageAge ZCompactForwarding::to_age() const {
  return _age_to;
}

inline zoffset ZCompactForwarding::start() const {
  return _virtual.start();
}

inline size_t ZCompactForwarding::size() const {
  return _virtual.size();
}

inline size_t ZCompactForwarding::object_alignment_shift() const {
  return _object_alignment_shift;
}

inline bool ZCompactForwarding::is_promotion() const {
  return _age_from != ZPageAge::old &&
         _age_to == ZPageAge::old;
}

template <typename Function>
inline void ZCompactForwarding::object_iterate(Function function) {
  _page->object_iterate(function);
}

inline const bool ZCompactForwarding::has_to_page(const ZCompactForwardingEntry* entry) const {
    return (has_snd_page() && entry >= _first_on_snd_page_entry)
      ? _to_snd
      : _to_fst;
}

inline const bool ZCompactForwarding::has_to_page(const zaddress addr) const {
    return (has_snd_page() && addr >= _first_on_snd_page)
      ? _to_snd
      : _to_fst;
}

inline const uintptr_t ZCompactForwarding::to_page_start(const zaddress addr) const {
    if (!has_to_page(addr)) {
      ShouldNotReachHere();
    }
    zoffset heap_offset = (has_snd_page() && addr >= _first_on_snd_page)
      ? _to_snd->start()
      : _to_fst->start();
    return (uintptr_t) heap_offset | ZAddressHeapBase;
}

inline const uintptr_t ZCompactForwarding::to_page_start(const ZCompactForwardingEntry* entry) const {
    if (!has_to_page(entry)) {
      ShouldNotReachHere();
    }
    zoffset heap_offset = (has_snd_page() && entry >= _first_on_snd_page_entry)
      ? _to_snd->start()
      : _to_fst->start();
    return (uintptr_t) heap_offset | ZAddressHeapBase;
}

inline void ZCompactForwarding::install_to_page(ZPage* to_page) {
  if (_prev) {
    Atomic::store(&_to_snd, to_page);
  } else {
    Atomic::store(&_to_fst, to_page);
  }
  Atomic::store(&_in_place, false)  ; // TODO move this
}

inline const size_t ZCompactForwarding::top() const {
  return _top;
}

inline const bool ZCompactForwarding::is_last() const {
  return _is_last;
}

inline const size_t ZCompactForwarding::last_to_page_top() const {
  assert(_is_last, "Should only be called on last entries in a chain");
  return _top;
}

inline void ZCompactForwarding::mark_as_last() {
  _is_last = true;
}

inline const bool ZCompactForwarding::is_first() {
  if (_prev == NULL) {
    return claim();
  } else {
    return false;
  }
}

inline const zaddress ZCompactForwarding::to_addr(const zaddress from_addr) {
  auto e = find_raw(from_addr);
  assert(e->is_relocated(), "Should be relocated by now");
  return to(from_addr, e);
}

inline size_t ZCompactForwarding::offset_to_index_raw(zaddress from_addr) const {
  zoffset from_offset = ZAddress::offset(from_addr);
  auto index = ((size_t)from_offset - (size_t)_virtual.start()) >> object_alignment_shift();
  assert(index < (_virtual.size() >> object_alignment_shift()), "");
  return index;
}

inline ZCompactForwardingEntry* const ZCompactForwarding::find_raw(zaddress from_addr) const {
  return _compact_entries + addr_to_index(from_addr);
}

inline ZCompactForwardingEntry* const ZCompactForwarding::find(zaddress from_addr) {
  ZCompactForwardingEntry* const entry = find_raw(from_addr);

  if (has_to_page(from_addr)) {
    return entry;
  }

  if (has_to_page(from_addr) && !ZStressRelocateInPlace) {
    /// Nothing to do!
  } else {
    ZPage* to_page = (has_snd_page() && zaddress(from_addr) >= _first_on_snd_page)
      ? get_second(false)
      : get_first(false);
    if (to_page == NULL) {
      return NULL;
    }
  }
  return entry;
}

inline const size_t ZCompactForwarding::addr_to_index(zaddress addr) const {
  const size_t page_break_offset = has_snd_page() && addr >= _first_on_snd_page ? 1 : 0;
  return ((ZAddress::offset(addr) - _virtual.start()) >> 8) + page_break_offset;
}

inline size_t ZCompactForwarding::addr_to_internal_index(zaddress addr) const {
  return ((ZAddress::offset(addr) - _virtual.start()) >> 3) % 32;
}

inline void ZCompactForwarding::populate_compact_entries() {
  const size_t page_size = _page->size();
  size_t offset = _prev == NULL ? 0ULL: _prev->top();
  bool first = true;

  ZCompactForwardingEntry* current_entry = _compact_entries;
  bool only_once = true;
  size_t index = 0;
  auto fun = [&](oop obj) {
    const zaddress addr = to_zaddress(obj);
    const size_t object_size = align_up(ZUtils::object_size(addr), _page->object_alignment());
    const size_t internal_index = addr_to_internal_index(addr);
    ZCompactForwardingEntry* entry = const_cast<ZCompactForwardingEntry*>(find_raw(addr));
    // guarantee(entry != NULL, "should exist");
    index++;

    if (offset + object_size > page_size) {
      offset = 0;
      _first_on_snd_page = addr;
      entry = const_cast<ZCompactForwardingEntry*>(find_raw(addr));
      _first_on_snd_page_entry = entry;
      current_entry = entry;
    }

    if (first) {
      first = false;
      _compact_entries->set_live_bytes_before_fragment(offset);
    }

    // Copy liveness information
    entry->set_liveness(internal_index);

    // Store object size
    entry->set_size_bit(internal_index, object_size);

    // Allocate for object
    if (current_entry < entry) {
      current_entry = entry;
      current_entry->set_live_bytes_before_fragment(offset);
    }

    offset += object_size;
  };

  auto do_bit = [&](BitMap::idx_t index) -> bool {
    const oop obj = _page->object_from_bit_index(index);
    // Apply function
    fun(obj);
    return true;
  };

  _page->_livemap.iterate(_generation_id, do_bit);
  _top = offset;
}

template <typename Function>
inline void ZCompactForwarding::fragment_iterate(Function function) {
    for (size_t index = 0; index < (_virtual.size() / 256) + 1; index++) {
      ZCompactForwardingEntry* entry = _compact_entries + index;
      if (((uint32_t)entry->_entry) == 0) continue;
      function(entry);
    }
}

template <typename Function>
inline void ZCompactForwarding::object_iterate_forwarded_via_livemap(Function function) {
  assert(!in_place_relocation(), "Not allowed to use livemap iteration");

  object_iterate([&](oop obj) {
    // Find to-object
    oop to_obj = to_oop(to_addr((zaddress) to_zaddress_unsafe(obj)));

    // Apply function
    function(to_obj);
  });
}

inline zaddress ZCompactForwarding::from(size_t entry_index, size_t internal_index) const {
  return (zaddress)((size_t)_virtual.start() | ZAddressHeapBase) + (entry_index * 256) + (internal_index * 8);
}

inline const zaddress ZCompactForwarding::to(zaddress from_addr) {
  return to(from_addr, find(from_addr));
}

inline const zaddress ZCompactForwarding::to(zaddress from_addr, const ZCompactForwardingEntry* entry) {
  //guarantee(has_to_page(from_addr), "trying to get to addr without installed to page");
  if (!has_to_page(from_addr)) return (zaddress)((size_t)_to_in_place->start() +
    (size_t)to_offset(from_addr, entry) | ZAddressHeapBase);
  return
    (zaddress)(to_page_start(from_addr) +
    (size_t)to_offset(from_addr, entry));
}

inline const zaddress ZCompactForwarding::to_offset(const zaddress from_addr, const ZCompactForwardingEntry* entry) const {
  const size_t live_bytes_before_fragment = entry->live_bytes_before_fragment();

  return
    (zaddress)(
    live_bytes_before_fragment +
    entry->live_bytes_on_fragment((size_t)_virtual.start()|ZAddressHeapBase, (size_t)from_addr, this));
}


inline bool ZCompactForwarding::is_on_snd_page(zaddress from_addr) const {
  return from_addr >= _first_on_snd_page && _to_snd;
}

template <typename Function>
inline void ZCompactForwarding::object_iterate_forwarded_via_table(Function function) {
  for (size_t i = 0; i < (_virtual.size() / 256) + 1; i++) {
    ZCompactForwardingEntry* entry = _compact_entries + i;
    if (entry->_entry == 0) continue;

    int32_t cursor = 0;
    size_t index = 0;
    while (cursor < 32) {
      cursor = index > 0 ?
        entry->get_next_live_object(cursor, true) :
        entry->get_next_live_object(cursor, false);
      if (cursor == -1) {
        break;
      }
      const zaddress from_addr = from(i, cursor);
      const zaddress to_addr = to(from_addr, entry);
      oop to_obj = to_oop(to_addr);

      // Apply function
      function(to_obj);
      index++;
    }
  }
}

template <typename Function>
inline void ZCompactForwarding::object_iterate_forwarded(Function function) {
  if (in_place_relocation()) {
    // The original objects are not available anymore, can't use the livemap
    object_iterate_forwarded_via_table(function);
  } else {
    object_iterate_forwarded_via_livemap(function);
  }
}

template <typename Function>
void ZCompactForwarding::oops_do_in_forwarded(Function function) {
  object_iterate_forwarded([&](oop to_obj) {
    ZIterator::basic_oop_iterate_safe(to_obj, function);
  });
}

template <typename Function>
void ZCompactForwarding::oops_do_in_forwarded_via_table(Function function) {
  object_iterate_forwarded_via_table([&](oop to_obj) {
    ZIterator::basic_oop_iterate_safe(to_obj, function);
  });
}

inline bool ZCompactForwarding::in_place_relocation() const {
  assert(Atomic::load(&_ref_count) != 0, "The page has been released/detached");
  return _in_place;
}

inline bool ZCompactForwarding::get_and_set_remset_scanned() {
  if (_remset_scanned) {
    return true;
  }
  _remset_scanned = true;
  return false;
}

inline bool ZCompactForwarding::has_snd_page() const {
  return _first_on_snd_page != zaddress::null;
}

inline ZPage* ZCompactForwarding::get_first(bool gc_thread) {
  if (auto page = Atomic::load(&_to_fst)) {
    return page;
  }

  auto success = gc_thread ? false : retain_page();
  auto result = get_first_inner(gc_thread);
  if (success) release_page();
  return result;
}

inline ZPage* ZCompactForwarding::get_first_inner(bool gc_thread) {
  if (_prev) {
    // Continue on previous' last page
    ZPage* prev_last_page = _prev->get_last(gc_thread);
    if (prev_last_page) {
      auto old = Atomic::cmpxchg(&_to_fst, (ZPage*) NULL, prev_last_page);
      if (old == NULL || old == prev_last_page) {
        // Success installing page for the first time, or commuting race
        return prev_last_page;
      } else if (old == NULL) {
        // TODO: this probably cannot happen anymore
        // GC thread managed to claim current page for in-place relocation
        return NULL;
      } else {
        // Other thread managed to install another page (should only happen when in-place is done)
        if (old->start() != _page->start()) ShouldNotReachHere();
        return old;
      }
    } else {
      // OOM
      return NULL;
    }
  } else {
    // Take a new page and install it
    return install_page(&_to_fst, gc_thread);
  }
}

inline ZPage* ZCompactForwarding::get_second(bool gc_thread) {
  if (auto page = Atomic::load(&_to_snd)) {
    return page;       // Return the page
  } else {
    auto success = gc_thread ? false : retain_page();
    auto result = install_page(&_to_snd, gc_thread);
    if (success) release_page();
    return result;
  }
}

inline ZPage* ZCompactForwarding::get_last(bool gc_thread) {
  return has_snd_page()
    ? get_second(gc_thread)
    : get_first(gc_thread);
}

inline ZPage* ZCompactForwarding::install_page(ZPage** p, bool gc_thread) {
  if (ZStressRelocateInPlace) return NULL;

  if (auto page = Atomic::load(p)) {
    return page;
  }

  ZAllocationFlags flags;
  /// Allow this to fail even on GC thread because that is handled by in-place relocation
  flags.set_non_blocking();

  ZPage* new_page = ZHeap::heap()->alloc_page(_page->type(),
                                              _page->size(),
                                              flags,
                                              _age_to);
  if (new_page == NULL) {
    return NULL;
  }

  // TODO: this code is untested, it should not max top on the final medium pages in a chain
  //if (_is_last) {
  //  new_page->set_top(_top);
  //} else {
  new_page->move_top_to_max();
  //}

  if (auto old = Atomic::cmpxchg(p, (ZPage*) NULL, new_page)) {
    /* Another allocation was installed first */
    new_page->_top = zoffset(0); // probably not needed
    ZHeap::heap()->undo_alloc_page(new_page);

    new_page = ((uintptr_t) old == 1UL
                ? NULL  // lost race with in-place
                : old); // lost race with non in-place
  }

  return new_page;
}


inline uint32_t ZForwarding::nentries(const ZPage* page) {
  // The number returned by the function is used to size the hash table of
  // forwarding entries for this page. This hash table uses linear probing.
  // The size of the table must be a power of two to allow for quick and
  // inexpensive indexing/masking. The table is also sized to have a load
  // factor of 50%, i.e. sized to have double the number of entries actually
  // inserted, to allow for good lookup/insert performance.
  return round_up_power_of_2(page->live_objects() * 2);
}

inline ZForwarding* ZForwarding::alloc(ZForwardingAllocator* allocator, ZPage* page, ZPageAge to_age) {
  const size_t nentries = ZForwarding::nentries(page);
  void* const addr = AttachedArray::alloc(allocator, nentries);
  return ::new (addr) ZForwarding(page, to_age, nentries);
}

inline ZForwarding::ZForwarding(ZPage* page, ZPageAge to_age, size_t nentries) :
    _virtual(page->virtual_memory()),
    _object_alignment_shift(page->object_alignment_shift()),
    _entries(nentries),
    _page(page),
    _from_age(page->age()),
    _to_age(to_age),
    _claimed(false),
    _ref_lock(),
    _ref_count(1),
    _ref_abort(false),
    _remset_scanned(false),
    _in_place(false),
    _in_place_clear_remset_watermark(0),
    _in_place_top_at_start(),
    _in_place_thread(NULL) {}

inline ZPageType ZForwarding::type() const {
  return _page->type();
}

inline ZPageAge ZForwarding::from_age() const {
  return _from_age;
}

inline ZPageAge ZForwarding::to_age() const {
  return _to_age;
}

inline zoffset ZForwarding::start() const {
  return _virtual.start();
}

inline size_t ZForwarding::size() const {
  return _virtual.size();
}

inline size_t ZForwarding::object_alignment_shift() const {
  return _object_alignment_shift;
}

inline bool ZForwarding::is_promotion() const {
  return _from_age != ZPageAge::old &&
         _to_age == ZPageAge::old;
}

template <typename Function>
inline void ZForwarding::object_iterate(Function function) {
  ZObjectClosure<Function> cl(function);
  _page->object_iterate(function);
}

template <typename Function>
inline void ZForwarding::object_iterate_forwarded_via_livemap(Function function) {
  assert(!in_place_relocation(), "Not allowed to use livemap iteration");

  object_iterate([&](oop obj) {
    // Find to-object
    zaddress_unsafe from_addr = to_zaddress_unsafe(obj);
    zaddress to_addr = this->find(from_addr);
    oop to_obj = to_oop(to_addr);

    // Apply function
    function(to_obj);
  });
}

template <typename Function>
inline void ZForwarding::object_iterate_forwarded_via_table(Function function) {
  for (ZForwardingCursor i = 0; i < _entries.length(); i++) {
    const ZForwardingEntry entry = at(&i);
    if (!entry.populated()) {
      // Skip empty entries
      continue;
    }

    // Find to-object
    zoffset to_offset = to_zoffset(entry.to_offset());
    zaddress to_addr = ZOffset::address(to_offset);
    oop to_obj = to_oop(to_addr);

    // Apply function
    function(to_obj);
  }
}

template <typename Function>
inline void ZForwarding::object_iterate_forwarded(Function function) {
  if (in_place_relocation()) {
    // The original objects are not available anymore, can't use the livemap
    object_iterate_forwarded_via_table(function);
  } else {
    object_iterate_forwarded_via_livemap(function);
  }
}

template <typename Function>
void ZForwarding::oops_do_in_forwarded(Function function) {
  object_iterate_forwarded([&](oop to_obj) {
    ZIterator::basic_oop_iterate_safe(to_obj, function);
  });
}

template <typename Function>
void ZForwarding::oops_do_in_forwarded_via_table(Function function) {
  object_iterate_forwarded_via_table([&](oop to_obj) {
    ZIterator::basic_oop_iterate_safe(to_obj, function);
  });
}

inline bool ZForwarding::in_place_relocation() const {
  assert(Atomic::load(&_ref_count) != 0, "The page has been released/detached");
  return _in_place;
}

inline ZForwardingEntry* ZForwarding::entries() const {
  return _entries(this);
}

inline ZForwardingEntry ZForwarding::at(ZForwardingCursor* cursor) const {
  // Load acquire for correctness with regards to
  // accesses to the contents of the forwarded object.
  return Atomic::load_acquire(entries() + *cursor);
}

inline ZForwardingEntry ZForwarding::first(uintptr_t from_index, ZForwardingCursor* cursor) const {
  const size_t mask = _entries.length() - 1;
  const size_t hash = ZHash::uint32_to_uint32((uint32_t)from_index);
  *cursor = hash & mask;
  return at(cursor);
}

inline ZForwardingEntry ZForwarding::next(ZForwardingCursor* cursor) const {
  const size_t mask = _entries.length() - 1;
  *cursor = (*cursor + 1) & mask;
  return at(cursor);
}

inline zaddress ZForwarding::find(zaddress_unsafe addr) {
  const uintptr_t from_index = (ZAddress::offset(addr) - start()) >> object_alignment_shift();
  ZForwardingCursor cursor;
  const ZForwardingEntry entry = find(from_index, &cursor);
  return entry.populated() ? ZOffset::address(to_zoffset(entry.to_offset())) : zaddress::null;
}

inline ZForwardingEntry ZForwarding::find(uintptr_t from_index, ZForwardingCursor* cursor) const {
  // Reading entries in the table races with the atomic CAS done for
  // insertion into the table. This is safe because each entry is at
  // most updated once (from zero to something else).
  ZForwardingEntry entry = first(from_index, cursor);
  while (entry.populated()) {
    if (entry.from_index() == from_index) {
      // Match found, return matching entry
      return entry;
    }

    entry = next(cursor);
  }

  // Match not found, return empty entry
  return entry;
}

inline zoffset ZForwarding::insert(uintptr_t from_index, zoffset to_offset, ZForwardingCursor* cursor) {
  const ZForwardingEntry new_entry(from_index, untype(to_offset));
  const ZForwardingEntry old_entry; // Empty

  // Make sure that object copy is finished
  // before forwarding table installation
  OrderAccess::release();

  for (;;) {
    const ZForwardingEntry prev_entry = Atomic::cmpxchg(entries() + *cursor, old_entry, new_entry, memory_order_relaxed);
    if (!prev_entry.populated()) {
      // Success
      return to_offset;
    }

    // Find next empty or matching entry
    ZForwardingEntry entry = at(cursor);
    while (entry.populated()) {
      if (entry.from_index() == from_index) {
        // Match found, return already inserted address
        return to_zoffset(entry.to_offset());
      }

      entry = next(cursor);
    }
  }
}

inline bool ZForwarding::get_and_set_remset_scanned() {
  if (_remset_scanned) {
    return true;
  }
  _remset_scanned = true;
  return false;
}

#endif // SHARE_GC_Z_ZFORWARDING_INLINE_HPP
