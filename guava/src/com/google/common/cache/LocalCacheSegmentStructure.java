/*
 * Copyright (C) 2009 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.cache;

import java.util.concurrent.atomic.AtomicReferenceArray;
import org.jspecify.annotations.Nullable;

final class LocalCacheSegmentStructure<K, V> {
  private LocalCacheSegmentStructure() {}

  static <K, V> ReferenceEntry<K, V> getFirst(LocalCache.Segment<K, V> segment, int hash) {
    AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
    return table.get(hash & (table.length() - 1));
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void expand(LocalCache.Segment<K, V> segment) {
    AtomicReferenceArray<ReferenceEntry<K, V>> oldTable = segment.table;
    int oldCapacity = oldTable.length();
    if (oldCapacity >= LocalCache.MAXIMUM_CAPACITY) {
      return;
    }

    int newCount = segment.count;
    AtomicReferenceArray<ReferenceEntry<K, V>> newTable = segment.newEntryArray(oldCapacity << 1);
    segment.threshold = newTable.length() * 3 / 4;
    int newMask = newTable.length() - 1;
    for (int oldIndex = 0; oldIndex < oldCapacity; ++oldIndex) {
      ReferenceEntry<K, V> head = oldTable.get(oldIndex);

      if (head != null) {
        ReferenceEntry<K, V> next = head.getNext();
        int headIndex = head.getHash() & newMask;

        if (next == null) {
          newTable.set(headIndex, head);
        } else {
          ReferenceEntry<K, V> tail = head;
          int tailIndex = headIndex;
          for (ReferenceEntry<K, V> entry = next; entry != null; entry = entry.getNext()) {
            int newIndex = entry.getHash() & newMask;
            if (newIndex != tailIndex) {
              tailIndex = newIndex;
              tail = entry;
            }
          }
          newTable.set(tailIndex, tail);

          for (ReferenceEntry<K, V> entry = head; entry != tail; entry = entry.getNext()) {
            int newIndex = entry.getHash() & newMask;
            ReferenceEntry<K, V> newNext = newTable.get(newIndex);
            ReferenceEntry<K, V> newFirst = segment.copyEntry(entry, newNext);
            if (newFirst != null) {
              newTable.set(newIndex, newFirst);
            } else {
              removeCollectedEntry(segment, entry);
              newCount--;
            }
          }
        }
      }
    }
    segment.table = newTable;
    segment.count = newCount;
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> @Nullable ReferenceEntry<K, V> removeValueFromChain(
      LocalCache.Segment<K, V> segment,
      ReferenceEntry<K, V> first,
      ReferenceEntry<K, V> entry,
      @Nullable K key,
      int hash,
      V value,
      LocalCache.ValueReference<K, V> valueReference,
      RemovalCause cause) {
    segment.enqueueNotification(key, hash, value, valueReference.getWeight(), cause);
    segment.writeQueue.remove(entry);
    segment.accessQueue.remove(entry);

    if (valueReference.isLoading()) {
      valueReference.notifyNewValue(null);
      return first;
    }
    return removeEntryFromChain(segment, first, entry);
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> @Nullable ReferenceEntry<K, V> removeEntryFromChain(
      LocalCache.Segment<K, V> segment, ReferenceEntry<K, V> first, ReferenceEntry<K, V> entry) {
    int newCount = segment.count;
    ReferenceEntry<K, V> newFirst = entry.getNext();
    for (ReferenceEntry<K, V> chainEntry = first;
        chainEntry != entry;
        chainEntry = chainEntry.getNext()) {
      ReferenceEntry<K, V> next = segment.copyEntry(chainEntry, newFirst);
      if (next != null) {
        newFirst = next;
      } else {
        removeCollectedEntry(segment, chainEntry);
        newCount--;
      }
    }
    segment.count = newCount;
    return newFirst;
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void removeCollectedEntry(
      LocalCache.Segment<K, V> segment, ReferenceEntry<K, V> entry) {
    segment.enqueueNotification(
        entry.getKey(),
        entry.getHash(),
        entry.getValueReference().get(),
        entry.getValueReference().getWeight(),
        RemovalCause.COLLECTED);
    segment.writeQueue.remove(entry);
    segment.accessQueue.remove(entry);
  }
}
