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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.lang.ref.Reference;
import java.util.concurrent.atomic.AtomicReferenceArray;

final class LocalCacheMaintenance<K, V> {
  private LocalCacheMaintenance() {}

  static <K, V> void tryDrainReferenceQueues(LocalCache.Segment<K, V> segment) {
    if (segment.tryLock()) {
      try {
        drainReferenceQueues(segment);
      } finally {
        segment.unlock();
      }
    }
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void drainReferenceQueues(LocalCache.Segment<K, V> segment) {
    if (segment.map.usesKeyReferences()) {
      drainKeyReferenceQueue(segment);
    }
    if (segment.map.usesValueReferences()) {
      drainValueReferenceQueue(segment);
    }
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void drainKeyReferenceQueue(LocalCache.Segment<K, V> segment) {
    Reference<? extends K> ref;
    int i = 0;
    while ((ref = segment.keyReferenceQueue.poll()) != null) {
      @SuppressWarnings("unchecked")
      ReferenceEntry<K, V> entry = (ReferenceEntry<K, V>) ref;
      segment.map.reclaimKey(entry);
      if (++i == LocalCache.DRAIN_MAX) {
        break;
      }
    }
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void drainValueReferenceQueue(LocalCache.Segment<K, V> segment) {
    Reference<? extends V> ref;
    int i = 0;
    while ((ref = segment.valueReferenceQueue.poll()) != null) {
      @SuppressWarnings("unchecked")
      LocalCache.ValueReference<K, V> valueReference = (LocalCache.ValueReference<K, V>) ref;
      segment.map.reclaimValue(valueReference);
      if (++i == LocalCache.DRAIN_MAX) {
        break;
      }
    }
  }

  static <K, V> void clearReferenceQueues(LocalCache.Segment<K, V> segment) {
    if (segment.map.usesKeyReferences()) {
      clearKeyReferenceQueue(segment);
    }
    if (segment.map.usesValueReferences()) {
      clearValueReferenceQueue(segment);
    }
  }

  static <K, V> void clearKeyReferenceQueue(LocalCache.Segment<K, V> segment) {
    while (segment.keyReferenceQueue.poll() != null) {}
  }

  static <K, V> void clearValueReferenceQueue(LocalCache.Segment<K, V> segment) {
    while (segment.valueReferenceQueue.poll() != null) {}
  }

  static <K, V> void tryExpireEntries(LocalCache.Segment<K, V> segment, long now) {
    if (segment.tryLock()) {
      try {
        segment.expireEntries(now);
      } finally {
        segment.unlock();
        // don't call postWriteCleanup as we're in a read
      }
    }
  }

  static <K, V> void clear(LocalCache.Segment<K, V> segment) {
    if (segment.count != 0) {
      segment.lock();
      try {
        long now = segment.map.ticker.read();
        preWriteCleanup(segment, now);

        AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
        for (int i = 0; i < table.length(); ++i) {
          for (ReferenceEntry<K, V> e = table.get(i); e != null; e = e.getNext()) {
            if (e.getValueReference().isActive()) {
              K key = e.getKey();
              V value = e.getValueReference().get();
              RemovalCause cause =
                  (key == null || value == null) ? RemovalCause.COLLECTED : RemovalCause.EXPLICIT;
              segment.enqueueNotification(
                  key, e.getHash(), value, e.getValueReference().getWeight(), cause);
            }
          }
        }
        for (int i = 0; i < table.length(); ++i) {
          table.set(i, null);
        }
        clearReferenceQueues(segment);
        segment.writeQueue.clear();
        segment.accessQueue.clear();
        segment.readCount.set(0);

        ++segment.modCount;
        segment.count = 0;
      } finally {
        segment.unlock();
        postWriteCleanup(segment);
      }
    }
  }

  @CanIgnoreReturnValue
  static <K, V> boolean reclaimKey(
      LocalCache.Segment<K, V> segment, ReferenceEntry<K, V> entry, int hash) {
    segment.lock();
    try {
      int newCount = segment.count - 1;
      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (ReferenceEntry<K, V> e = first; e != null; e = e.getNext()) {
        if (e == entry) {
          ++segment.modCount;
          ReferenceEntry<K, V> newFirst =
              segment.removeValueFromChain(
                  first,
                  e,
                  e.getKey(),
                  hash,
                  e.getValueReference().get(),
                  e.getValueReference(),
                  RemovalCause.COLLECTED);
          newCount = segment.count - 1;
          table.set(index, newFirst);
          segment.count = newCount;
          return true;
        }
      }

      return false;
    } finally {
      segment.unlock();
      postWriteCleanup(segment);
    }
  }

  @CanIgnoreReturnValue
  static <K, V> boolean reclaimValue(
      LocalCache.Segment<K, V> segment,
      K key,
      int hash,
      LocalCache.ValueReference<K, V> valueReference) {
    segment.lock();
    try {
      int newCount = segment.count - 1;
      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (ReferenceEntry<K, V> e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          LocalCache.ValueReference<K, V> v = e.getValueReference();
          if (v == valueReference) {
            ++segment.modCount;
            ReferenceEntry<K, V> newFirst =
                segment.removeValueFromChain(
                    first,
                    e,
                    entryKey,
                    hash,
                    valueReference.get(),
                    valueReference,
                    RemovalCause.COLLECTED);
            newCount = segment.count - 1;
            table.set(index, newFirst);
            segment.count = newCount;
            return true;
          }
          return false;
        }
      }

      return false;
    } finally {
      segment.unlock();
      if (!segment.isHeldByCurrentThread()) {
        postWriteCleanup(segment);
      }
    }
  }

  @CanIgnoreReturnValue
  static <K, V> boolean removeLoadingValue(
      LocalCache.Segment<K, V> segment,
      K key,
      int hash,
      LocalCache.LoadingValueReference<K, V> valueReference) {
    segment.lock();
    try {
      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (ReferenceEntry<K, V> e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          LocalCache.ValueReference<K, V> v = e.getValueReference();
          if (v == valueReference) {
            if (valueReference.isActive()) {
              e.setValueReference(valueReference.getOldValue());
            } else {
              ReferenceEntry<K, V> newFirst = segment.removeEntryFromChain(first, e);
              table.set(index, newFirst);
            }
            return true;
          }
          return false;
        }
      }

      return false;
    } finally {
      segment.unlock();
      postWriteCleanup(segment);
    }
  }

  static <K, V> void postReadCleanup(LocalCache.Segment<K, V> segment) {
    if ((segment.readCount.incrementAndGet() & LocalCache.DRAIN_THRESHOLD) == 0) {
      cleanUp(segment);
    }
  }

  static <K, V> void preWriteCleanup(LocalCache.Segment<K, V> segment, long now) {
    runLockedCleanup(segment, now);
  }

  static <K, V> void postWriteCleanup(LocalCache.Segment<K, V> segment) {
    runUnlockedCleanup(segment);
  }

  static <K, V> void cleanUp(LocalCache.Segment<K, V> segment) {
    long now = segment.map.ticker.read();
    runLockedCleanup(segment, now);
    runUnlockedCleanup(segment);
  }

  static <K, V> void runLockedCleanup(LocalCache.Segment<K, V> segment, long now) {
    if (segment.tryLock()) {
      try {
        drainReferenceQueues(segment);
        segment.expireEntries(now);
        segment.readCount.set(0);
      } finally {
        segment.unlock();
      }
    }
  }

  static <K, V> void runUnlockedCleanup(LocalCache.Segment<K, V> segment) {
    if (!segment.isHeldByCurrentThread()) {
      segment.map.processPendingNotifications();
    }
  }
}
