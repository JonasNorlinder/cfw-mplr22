/*
 * Copyright (c) 2021, Oracle and/or its affiliates. All rights reserved.
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

#ifndef SHARE_GC_Z_ZCOLLECTOR_HPP
#define SHARE_GC_Z_ZCOLLECTOR_HPP

#include "gc/z/zCollectorId.hpp"
#include "gc/z/zForwardingTable.hpp"
#include "gc/z/zGenerationId.hpp"
#include "gc/z/zMark.hpp"
#include "gc/z/zReferenceProcessor.hpp"
#include "gc/z/zRelocate.hpp"
#include "gc/z/zRelocationSet.hpp"
#include "gc/z/zStat.hpp"
#include "gc/z/zTracer.hpp"
#include "gc/z/zUnload.hpp"
#include "gc/z/zWeakRootsProcessor.hpp"
#include "gc/z/zWorkers.hpp"
#include "memory/allocation.hpp"

class ThreadClosure;
class ZForwardingTable;
class ZPage;
class ZPageAllocator;
class ZPageTable;
class ZRelocationSetSelector;

class ZCollector {
  friend class ZForwardingTest;
  friend class ZLiveMapTest;

protected:
  enum class Phase {
    Mark,
    MarkComplete,
    Relocate
  };

  ZCollectorId      _id;
  ZPageAllocator*   _page_allocator;
  ZPageTable*       _page_table;
  ZForwardingTable  _forwarding_table;
  ZWorkers          _workers;
  ZMark             _mark;
  ZRelocate         _relocate;
  ZRelocationSet    _relocation_set;

  size_t            _used_high;
  size_t            _used_low;
  volatile size_t   _freed;
  volatile size_t   _compacted;
  volatile size_t   _promoted;

  Phase             _phase;
  uint32_t          _seqnum;

  ZStatHeap         _stat_heap;
  ZStatCycle        _stat_cycle;
  ZStatWorkers      _stat_workers;
  ZStatMark         _stat_mark;
  ZStatRelocation   _stat_relocation;

  ConcurrentGCTimer _timer;

  void free_empty_pages(ZRelocationSetSelector* selector, int bulk);
  void flip_age_pages(const ZRelocationSetSelector* selector);
  void flip_age_pages(const ZArray<ZPage*>* pages);

  ZCollector(ZCollectorId id, const char* worker_prefix, ZPageTable* page_table, ZPageAllocator* page_allocator);

  void log_phase_switch(Phase from, Phase to);

public:
  bool is_initialized() const;

  // GC phases
  void set_phase(Phase new_phase);
  bool is_phase_relocate() const;
  bool is_phase_mark() const;
  bool is_phase_mark_complete() const;
  const char* phase_to_string() const;

  uint32_t seqnum() const;

  ZCollectorId id() const;
  bool is_young() const;
  bool is_old() const;

  // Statistics
  void reset_statistics();
  size_t used_high() const;
  size_t used_low() const;
  ssize_t freed() const;
  void increase_freed(size_t size);
  size_t promoted() const;
  void increase_promoted(size_t size);
  size_t compacted() const;
  void increase_compacted(size_t size);
  void update_used(size_t used);

  ConcurrentGCTimer* timer();

  ZStatHeap* stat_heap();
  ZStatCycle* stat_cycle();
  ZStatWorkers* stat_workers();
  ZStatMark* stat_mark();
  ZStatRelocation* stat_relocation();

  void set_at_collection_start();
  void set_at_generation_collection_start();

  // Workers
  ZWorkers* workers();
  uint active_workers() const;
  void set_active_workers(uint nworkers);

  ZPageTable* page_table() const;
  const ZForwardingTable* forwarding_table() const;

  ZForwarding* forwarding(zaddress_unsafe addr) const;

  // Marking
  template <bool resurrect, bool gc_thread, bool follow, bool finalizable, bool publish>
  void mark_object(zaddress addr);
  void mark_flush_and_free(Thread* thread);
  void mark_free();

  // Relocation set
  void select_relocation_set(bool promote_all);
  void reset_relocation_set();

  // Relocation
  void synchronize_relocation();
  void desynchronize_relocation();
  zaddress relocate_or_remap_object(zaddress_unsafe addr);
  zaddress remap_object(zaddress_unsafe addr);

  // Tracing
  virtual GCTracer* tracer() = 0;

  // Threads
  void threads_do(ThreadClosure* tc) const;
};

enum class ZYoungType {
  minor,
  major_preclean,
  major_roots,
  undefined
};

class ZYoungTypeSetter {
public:
  ZYoungTypeSetter(ZYoungType type);
  ~ZYoungTypeSetter();
};

class ZYoungCollector : public ZCollector {
  friend class ZYoungTypeSetter;

private:
  ZYoungType        _type;
  ZYoungTracer      _tracer;
  ConcurrentGCTimer _minor_timer;

public:
  ZYoungCollector(ZPageTable* page_table, ZPageAllocator* page_allocator);

  ZYoungType type() const;

  ConcurrentGCTimer* minor_timer();

  // GC operations
  void mark_start();
  void mark_roots();
  void mark_follow();
  bool mark_end();

  void relocate_start();
  void relocate();

  void flip_promote(ZPage* from_page, ZPage* to_page);
  void in_place_relocate_promote(ZPage* from_page, ZPage* to_page);

  void register_flip_promoted(const ZArray<ZPage*>& pages);
  void register_in_place_relocate_promoted(ZPage* page);

  virtual GCTracer* tracer();
};

class ZOldCollector : public ZCollector {
private:
  ZReferenceProcessor _reference_processor;
  ZWeakRootsProcessor _weak_roots_processor;
  ZUnload             _unload;
  int                 _total_collections_at_end;
  ZOldTracer          _tracer;
  ConcurrentGCTimer   _major_timer;

public:
  ZOldCollector(ZPageTable* page_table, ZPageAllocator* page_allocator);

  ConcurrentGCTimer* major_timer();

  // Reference processing
  ReferenceDiscoverer* reference_discoverer();
  void set_soft_reference_policy(bool clear);

  // Non-strong reference processing
  void process_non_strong_references();

  // GC operations
  void mark_start();
  void mark_roots();
  void mark_follow();
  bool mark_end();
  void relocate_start();
  void relocate();
  void roots_remap();

  int total_collections_at_end() const;

  virtual GCTracer* tracer();
};

#endif // SHARE_GC_Z_ZCOLLECTOR_HPP
