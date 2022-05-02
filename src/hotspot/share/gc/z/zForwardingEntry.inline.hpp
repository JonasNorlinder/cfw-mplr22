#ifndef SHARE_GC_Z_ZCOMPACTFORWARDINGENTRY_INLINE_HPP
#define SHARE_GC_Z_ZCOMPACTFORWARDINGENTRY_INLINE_HPP

#include "gc/z/zForwardingEntry.hpp"
#include "gc/z/zForwarding.inline.hpp"
#include "gc/z/zUtils.hpp"
#include "utilities/count_trailing_zeros.hpp"
#include "utilities/count_leading_zeros.hpp"

inline uint32_t ZCompactForwardingEntry::live_bytes_before_fragment() const {
  return field_live_bytes::decode(_entry);
}

inline void ZCompactForwardingEntry::set_live_bytes_before_fragment(size_t value) {
  assert((value >> 18) == 0, "overflow");
  assert(!is_relocated(), "Updating not allowed");
  // 0xFFFC0000FFFFFFFF =
  // Relocated
  // |Lock
  // || Partial map
  // || |
  // 11 111111 111111 000000000000000000 11111111111111111111111111111111
  _entry = field_live_bytes::encode((uint32_t)value) | (_entry & 0xFFFC0000FFFFFFFF);
}

inline void ZCompactForwardingEntry::set_liveness(size_t index) {
  assert(!is_relocated(), "Updating liveness not allowed");
  assert(index < 32, "Invalid index");
  _entry |= 1ULL << index;
}

inline const bool ZCompactForwardingEntry::get_liveness(size_t index) const {
  return ((1ULL) << index) & _entry;
}

inline void ZCompactForwardingEntry::set_size_bit(size_t index, size_t size) {
  assert(!is_relocated(), "Updating not allowed");
  const size_t size_index = index + size / 8 - 1;

  // If size_index is larger than the current entry, that
  // would imply that this is the last living object on this entry.
  //
  // We need to calculate the live bytes of all living objects *before*
  // the current object we are inspecting, therefore this implies that
  // we don't have to store the size bit of the last object on the entry.
  if (size_index > 31) {
    return;
  }
  set_liveness(size_index);
}

static inline const size_t fragment_internal_index(uintptr_t old_page, uintptr_t from_offset) {
  assert(from_offset >= old_page, "invalid from_offset");
  return ((from_offset - old_page) >> 3) % 32;
}

inline int32_t ZCompactForwardingEntry::move_cursor(int32_t cursor, bool count) const {
  cursor = !count ? cursor : cursor + 1;
  int32_t mask = cursor < 32 ? (~0U) << (cursor) : 0U;
  uint32_t entry = _entry & mask;
  if (entry == 0) return -1;
  return count_trailing_zeros(entry);
}

inline int32_t ZCompactForwardingEntry::get_next_live_object(int32_t cursor, bool count) const {
  cursor = move_cursor(cursor, count);
  if (cursor == -1) return -1; // No more live objects

  if (!count) { // first encounter
    return cursor;
  }

  return move_cursor(cursor, count);
}

inline const size_t ZCompactForwardingEntry::get_size(int32_t cursor) const {
  int32_t mask = cursor < 32 ? (~0U) << (cursor + 1) : 0U;
  uint32_t entry = _entry & mask;
  if (entry == 0) return 0;
  return (count_trailing_zeros(entry) - cursor + 1) << 3;
}

inline const int32_t ZCompactForwardingEntry::last_live() const {
  uint32_t entry = (uint32_t)_entry;
  assert(entry > 0, "count leading zeros behaviour is not defined for zero");
  return 31 - count_leading_zeros(entry);
}

inline const size_t ZCompactForwardingEntry::live_bytes_on_fragment(uintptr_t old_page, uintptr_t from_offset, const ZCompactForwarding* fw) const {
  const size_t index = fragment_internal_index(old_page, from_offset);
  assert(index < 32, "index out of bounds");

  uint32_t cursor = move_cursor(0, false);
  if (index == 0 || cursor == index) return 0;
  size_t live_bytes = get_size(cursor);
  cursor = move_cursor(cursor, true);

  for (cursor = move_cursor(cursor, true);
       cursor < index;
       cursor = move_cursor(cursor, true)) {
    live_bytes += get_size(cursor);
    cursor = move_cursor(cursor, true);
  }
  return live_bytes;
}

inline void ZCompactForwardingEntry::mark_as_relocated() {
  assert(is_relocated() == false, "");
  set_relocated();
}

inline bool ZCompactForwardingEntry::is_relocated() const {
  return _entry & 1ULL<< 63;
}

inline const ZEntryStatus ZCompactForwardingEntry::wait_until_locked_or_relocated() {
  if (is_relocated()) {
    return ZEntryStatus::relocated;
  } else {
    lock();
    if (is_relocated()) {
      unlock();
      return ZEntryStatus::relocated;
    } else {
      return ZEntryStatus::locked;
    }
  }
}

inline void ZCompactForwardingEntry::lock() {
  while (true) {
    auto old_unlocked = Atomic::load(&_entry) & ~(1ULL<< 62);
    auto old_locked   = Atomic::load(&_entry) |  (1ULL<< 62);
    if (Atomic::cmpxchg((long long unsigned int*)&_entry, old_unlocked, old_locked) == old_unlocked) {
      return;
    }
  }
}

inline void ZCompactForwardingEntry::unlock() {
  auto old_locked   = Atomic::load(&_entry);
  assert(old_locked & (1ULL<< 62), "Attempt to unlock without holding the lock");
  Atomic::store((long long unsigned int*)&_entry, old_locked & ~(1ULL<< 62));
}

inline void ZCompactForwardingEntry::unlock_and_mark_relocated() {
  mark_as_relocated();
  unlock();
}

#endif // SHARE_GC_Z_ZCOMPACTFORWARDINGENTRY_INLINE_HPP