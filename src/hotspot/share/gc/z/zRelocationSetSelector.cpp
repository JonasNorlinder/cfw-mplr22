/*
 * Copyright (c) 2017, 2021, Oracle and/or its affiliates. All rights reserved.
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
#include "gc/shared/gc_globals.hpp"
#include "gc/z/zArray.inline.hpp"
#include "gc/z/zGeneration.inline.hpp"
#include "gc/z/zForwarding.inline.hpp"
#include "gc/z/zPage.inline.hpp"
#include "gc/z/zRelocationSetSelector.inline.hpp"
#include "jfr/jfrEvents.hpp"
#include "logging/log.hpp"
#include "runtime/globals.hpp"
#include "utilities/debug.hpp"
#include "utilities/powerOfTwo.hpp"

ZRelocationSetSelectorGroupStats::ZRelocationSetSelectorGroupStats() :
    _npages(0),
    _total(0),
    _live(0),
    _empty(0),
    _relocate(0) {}

ZRelocationSetSelectorGroup::ZRelocationSetSelectorGroup(const char* name,
                                                         ZPageType page_type,
                                                         size_t page_size,
                                                         size_t object_size_limit) :
    _name(name),
    _page_type(page_type),
    _page_size(page_size),
    _object_size_limit(object_size_limit),
    _fragmentation_limit(page_size * (ZFragmentationLimit / 100)),
    _live_pages(),
    _not_selected_pages(),
    _forwarding_entries(0),
    _stats(),
    _forwarding_overhead(0),
    _compact_forwarding_overhead(0) {}

bool ZRelocationSetSelectorGroup::is_disabled() {
  // Medium pages are disabled when their page size is zero
  return _page_type == ZPageType::medium && _page_size == 0;
}

bool ZRelocationSetSelectorGroup::is_selectable() {
  // Large pages are not selectable
  return _page_type != ZPageType::large;
}

void ZRelocationSetSelectorGroup::semi_sort() {
  // Semi-sort live pages by number of live bytes in ascending order
  const size_t npartitions_shift = 11;
  const size_t npartitions = (size_t)1 << npartitions_shift;
  const size_t partition_size = _page_size >> npartitions_shift;
  const size_t partition_size_shift = exact_log2(partition_size);

  // Partition slots/fingers
  int partitions[npartitions] = { /* zero initialize */ };

  // Calculate partition slots
  ZArrayIterator<ZPage*> iter1(&_live_pages);
  for (ZPage* page; iter1.next(&page);) {
    const size_t index = page->live_bytes() >> partition_size_shift;
    partitions[index]++;
  }

  // Calculate partition fingers
  int finger = 0;
  for (size_t i = 0; i < npartitions; i++) {
    const int slots = partitions[i];
    partitions[i] = finger;
    finger += slots;
  }

  // Allocate destination array
  const int npages = _live_pages.length();
  ZArray<ZPage*> sorted_live_pages(npages, npages, NULL);

  // Sort pages into partitions
  ZArrayIterator<ZPage*> iter2(&_live_pages);
  for (ZPage* page; iter2.next(&page);) {
    const size_t index = page->live_bytes() >> partition_size_shift;
    const int finger = partitions[index]++;
    assert(sorted_live_pages.at(finger) == NULL, "Invalid finger");
    sorted_live_pages.at_put(finger, page);
  }

  _live_pages.swap(&sorted_live_pages);
}

void ZRelocationSetSelectorGroup::select_inner() {
  // Calculate the number of pages to relocate by successively including pages in
  // a candidate relocation set and calculate the maximum space requirement for
  // their live objects.
  const int npages = _live_pages.length();
  int selected_from = 0;
  int selected_to = 0;
  size_t selected_live_objects = 0;
  size_t selected_live_bytes = 0;
  size_t selected_forwarding_entries = 0;
  size_t selected_compact_forwarding_entries = 0;

  size_t from_live_bytes = 0;
  size_t from_forwarding_entries = 0;
  size_t from_forwarding_entries_compact = 0;

  semi_sort();

  for (int from = 1; from <= npages; from++) {
    // Add page to the candidate relocation set
    ZPage* const page = _live_pages.at(from - 1);
    from_live_bytes += page->live_bytes();
    // CFW:        page->live objects()
    // Hash table: round_up_power_of_2(page->live_objects() * 2)
    from_forwarding_entries += ZForwarding::nentries(page);
    from_forwarding_entries_compact += ZCompactForwarding::nentries(page);

    // Calculate the maximum number of pages needed by the candidate relocation set.
    // By subtracting the object size limit from the pages size we get the maximum
    // number of pages that the relocation set is guaranteed to fit in, regardless
    // of in which order the objects are relocated.
    const int to = ceil((double)(from_live_bytes) / (double)(_page_size - _object_size_limit));

    // Calculate the relative difference in reclaimable space compared to our
    // currently selected final relocation set. If this number is larger than the
    // acceptable fragmentation limit, then the current candidate relocation set
    // becomes our new final relocation set.
    const int diff_from = from - selected_from;
    const int diff_to = to - selected_to;
    const double diff_reclaimable = 100 - percent_of(diff_to, diff_from);
    if (diff_reclaimable > ZFragmentationLimit) {
      selected_from = from;
      selected_to = to;
      selected_live_bytes = from_live_bytes;
      selected_forwarding_entries = from_forwarding_entries;
      selected_compact_forwarding_entries = from_forwarding_entries_compact;
    }

    log_trace(gc, reloc)("Candidate Relocation Set (%s Pages): %d->%d, "
                         "%.1f%% relative defragmentation, " SIZE_FORMAT " forwarding entries, %s",
                         _name, from, to, diff_reclaimable, from_forwarding_entries,
                         (selected_from == from) ? "Selected" : "Rejected");
  }

  // Finalize selection
  for (int i = selected_from; i < _live_pages.length(); i++) {
    ZPage* page = _live_pages.at(i);
    if (page->is_young()) {
      _not_selected_pages.append(page);
    }
  }
  _live_pages.trunc_to(selected_from);
  _forwarding_entries = selected_forwarding_entries; // round_up_power_of_2(page->live_objects() * 2)
  _compact_forwarding_entries = selected_compact_forwarding_entries; // live_objects

  // Update statistics
  _stats._relocate = selected_live_bytes;

  // Update forwarding overhead
  size_t selected_pages = _live_pages.length();
  size_t entries_needed = selected_forwarding_entries;
  size_t nentries = _page_type == ZPageType::small ? (ZPageSizeSmall/256+1) * selected_pages : (ZPageSizeMedium/256+1) * selected_pages;
  size_t size_forwardings = selected_pages * sizeof(ZCompactForwarding*) + selected_pages * sizeof(ZCompactForwarding);
  size_t size_compact_entries = nentries * sizeof(ZCompactForwardingEntry);
  _compact_forwarding_overhead = size_forwardings + size_compact_entries;

  const size_t relocation_set_size = selected_pages * sizeof(ZForwarding*);
  const size_t forwardings_size = selected_pages * sizeof(ZForwarding);
  const size_t forwarding_entries_size = entries_needed * sizeof(ZForwardingEntry);
  _forwarding_overhead = relocation_set_size + forwardings_size + forwarding_entries_size;

  log_trace(gc, reloc)("Relocation Set (%s Pages): %d->%d, %d skipped, " SIZE_FORMAT " forwarding entries",
                       _name, selected_from, selected_to, npages - selected_from, selected_forwarding_entries);
}

const size_t ZRelocationSetSelectorGroup::forwarding_overhead() const {
  return _forwarding_overhead;
}
const size_t ZRelocationSetSelectorGroup::compact_forwarding_overhead() const {
  return _compact_forwarding_overhead;
}

void ZRelocationSetSelectorGroup::select() {
  if (is_disabled()) {
    return;
  }

  EventZRelocationSetGroup event;

  if (is_selectable()) {
    select_inner();
  } else {
    // Mark pages as not selected
    const int npages = _live_pages.length();
    for (int from = 1; from <= npages; from++) {
      ZPage* const page = _live_pages.at(from - 1);
      _not_selected_pages.append(page);
    }
  }

  // Send event
  event.commit((u8)_page_type, _stats.npages(), _stats.total(), _stats.empty(), _stats.relocate());
}

ZRelocationSetSelector::ZRelocationSetSelector(bool promote_all, ZGeneration* generation) :
    _small("Small", ZPageType::small, ZPageSizeSmall, ZObjectSizeLimitSmall),
    _medium("Medium", ZPageType::medium, ZPageSizeMedium, ZObjectSizeLimitMedium),
    _large("Large", ZPageType::large, 0 /* page_size */, 0 /* object_size_limit */),
    _empty_pages(),
    _promote_all(promote_all),
    _generation(generation) {}

void ZRelocationSetSelector::select() {
  // Select pages to relocate. The resulting relocation set will be
  // sorted such that medium pages comes first, followed by small
  // pages. Pages within each page group will be semi-sorted by live
  // bytes in ascending order. Relocating pages in this order allows
  // us to start reclaiming memory more quickly.

  EventZRelocationSet event;

  // Select pages from each group
  _large.select();
  _medium.select();
  _small.select();

  size_t memory_overhead  = _medium.forwarding_overhead() + _small.forwarding_overhead();
  double oh = (double)memory_overhead / MaxHeapSize;
  // ZMaxOffHeap == 0   => All will be compact
  // ZMaxOffHeap == 100 => All will be hash table
  // so ZMaxOffHeap == 3.5 will run compact if overhead is more than 3.5% of Java heap
  if (ZMaxOffHeap == 0 || oh > (ZMaxOffHeap/100) && ZMaxOffHeap != 100) {
   _generation->use_hash_forwarding = false;
  } else {
   _generation->use_hash_forwarding = true;
  }

  // Send event
  event.commit(total(), empty(), relocate());
}

ZRelocationSetSelectorStats ZRelocationSetSelector::stats() const {
  ZRelocationSetSelectorStats stats;
  stats._small = _small.stats();
  stats._medium = _medium.stats();
  stats._large = _large.stats();
  return stats;
}
