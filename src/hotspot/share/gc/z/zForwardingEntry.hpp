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

#ifndef SHARE_GC_Z_ZFORWARDINGENTRY_HPP
#define SHARE_GC_Z_ZFORWARDINGENTRY_HPP

#include "gc/z/zLock.hpp"
#include "gc/z/zAddress.inline.hpp"
#include "gc/z/zBitField.hpp"
#include "memory/allocation.hpp"
#include "metaprogramming/primitiveConversions.hpp"
#include <type_traits>

class ZCompactForwarding;

enum class ZEntryStatus : uint8_t {
  locked,
  relocated
};

/// FIXME: move to another file
class ZCompactForwardingEntry {
  friend class ZCompactForwarding;
private:
  typedef ZBitField<uint64_t, uint32_t, 0, 32>     field_live_bits;
  typedef ZBitField<uint64_t, uint32_t, 32, 30>    field_live_bytes;
  typedef ZBitField<uint64_t, bool, 62, 1>         field_locked;
  typedef ZBitField<uint64_t, bool, 63, 1>         field_relocated;

  uint64_t _entry;

  void set_relocated() {
    _entry |= 1ULL<< 63;
  }

public:
  ZCompactForwardingEntry() :
    _entry(0) {}

  // Liveness and size bits describes one fragment
  void set_liveness(size_t index);
  void set_size_bit(size_t index, size_t size);

  int32_t move_cursor(int32_t cursor, bool count) const;
  int32_t get_next_live_object(int32_t cursor, bool count) const;

  uint32_t live_bytes_before_fragment() const;
  void set_live_bytes_before_fragment(size_t value);

  const size_t get_size(int32_t cursor) const;
  const int32_t last_live() const;
  const size_t live_bytes_on_fragment(uintptr_t old_page, uintptr_t from_offset, const ZCompactForwarding* fw) const;

  void mark_as_relocated();
  bool is_relocated() const;
  const ZEntryStatus wait_until_locked_or_relocated();
  void lock();
  void unlock();
  void unlock_and_mark_relocated();
};



class ZForwardingEntry {
  friend struct PrimitiveConversions::Translate<ZForwardingEntry>;
  friend class VMStructs;

private:
  typedef ZBitField<uint64_t, bool,   0,   1> field_populated;
  typedef ZBitField<uint64_t, size_t, 1,  45> field_to_offset;
  typedef ZBitField<uint64_t, size_t, 46, 18> field_from_index;

  uint64_t _entry;

public:
  ZForwardingEntry() :
      _entry(0) {}

  ZForwardingEntry(size_t from_index, size_t to_offset) :
      _entry(field_populated::encode(true) |
             field_to_offset::encode(to_offset) |
             field_from_index::encode(from_index)) {}

  bool populated() const {
    return field_populated::decode(_entry);
  }

  size_t to_offset() const {
    return field_to_offset::decode(_entry);
  }

  size_t from_index() const {
    return field_from_index::decode(_entry);
  }
};

// Needed to allow atomic operations on ZForwardingEntry
template <>
struct PrimitiveConversions::Translate<ZForwardingEntry> : public std::true_type {
  typedef ZForwardingEntry Value;
  typedef uint64_t         Decayed;

  static Decayed decay(Value v) {
    return v._entry;
  }

  static Value recover(Decayed d) {
    ZForwardingEntry entry;
    entry._entry = d;
    return entry;
  }
};

#endif // SHARE_GC_Z_ZFORWARDINGENTRY_HPP
