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
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.jspecify.annotations.Nullable;

final class LocalCacheWriteOperations<K, V> {
  private LocalCacheWriteOperations() {}

  @CanIgnoreReturnValue
  static <K, V> @Nullable V put(
      LocalCache.Segment<K, V> segment, K key, int hash, V value, boolean onlyIfAbsent) {
    segment.lock();
    try {
      long now = segment.map.ticker.read();
      segment.preWriteCleanup(now);

      int newCount = segment.count + 1;
      if (newCount > segment.threshold) {
        segment.expand();
        newCount = segment.count + 1;
      }

      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (ReferenceEntry<K, V> e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          LocalCache.ValueReference<K, V> valueReference = e.getValueReference();
          V entryValue = valueReference.get();

          if (entryValue == null) {
            ++segment.modCount;
            if (valueReference.isActive()) {
              segment.enqueueNotification(
                  key, hash, entryValue, valueReference.getWeight(), RemovalCause.COLLECTED);
              segment.setValue(e, key, value, now);
              newCount = segment.count;
            } else {
              segment.setValue(e, key, value, now);
              newCount = segment.count + 1;
            }
            segment.count = newCount;
            segment.evictEntries(e);
            return null;
          } else if (onlyIfAbsent) {
            segment.recordLockedRead(e, now);
            return entryValue;
          } else {
            ++segment.modCount;
            segment.enqueueNotification(
                key, hash, entryValue, valueReference.getWeight(), RemovalCause.REPLACED);
            segment.setValue(e, key, value, now);
            segment.evictEntries(e);
            return entryValue;
          }
        }
      }

      ++segment.modCount;
      ReferenceEntry<K, V> newEntry = segment.newEntry(key, hash, first);
      segment.setValue(newEntry, key, value, now);
      table.set(index, newEntry);
      newCount = segment.count + 1;
      segment.count = newCount;
      segment.evictEntries(newEntry);
      return null;
    } finally {
      segment.unlock();
      segment.postWriteCleanup();
    }
  }

  static <K, V> boolean replace(
      LocalCache.Segment<K, V> segment, K key, int hash, V oldValue, V newValue) {
    segment.lock();
    try {
      long now = segment.map.ticker.read();
      segment.preWriteCleanup(now);

      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (ReferenceEntry<K, V> e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          LocalCache.ValueReference<K, V> valueReference = e.getValueReference();
          V entryValue = valueReference.get();
          if (entryValue == null) {
            if (valueReference.isActive()) {
              int newCount = segment.count - 1;
              ++segment.modCount;
              ReferenceEntry<K, V> newFirst =
                  segment.removeValueFromChain(
                      first,
                      e,
                      entryKey,
                      hash,
                      entryValue,
                      valueReference,
                      RemovalCause.COLLECTED);
              newCount = segment.count - 1;
              table.set(index, newFirst);
              segment.count = newCount;
            }
            return false;
          }

          if (segment.map.valueEquivalence.equivalent(oldValue, entryValue)) {
            ++segment.modCount;
            segment.enqueueNotification(
                key, hash, entryValue, valueReference.getWeight(), RemovalCause.REPLACED);
            segment.setValue(e, key, newValue, now);
            segment.evictEntries(e);
            return true;
          } else {
            segment.recordLockedRead(e, now);
            return false;
          }
        }
      }

      return false;
    } finally {
      segment.unlock();
      segment.postWriteCleanup();
    }
  }

  static <K, V> @Nullable V replace(
      LocalCache.Segment<K, V> segment, K key, int hash, V newValue) {
    segment.lock();
    try {
      long now = segment.map.ticker.read();
      segment.preWriteCleanup(now);

      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (ReferenceEntry<K, V> e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          LocalCache.ValueReference<K, V> valueReference = e.getValueReference();
          V entryValue = valueReference.get();
          if (entryValue == null) {
            if (valueReference.isActive()) {
              int newCount = segment.count - 1;
              ++segment.modCount;
              ReferenceEntry<K, V> newFirst =
                  segment.removeValueFromChain(
                      first,
                      e,
                      entryKey,
                      hash,
                      entryValue,
                      valueReference,
                      RemovalCause.COLLECTED);
              newCount = segment.count - 1;
              table.set(index, newFirst);
              segment.count = newCount;
            }
            return null;
          }

          ++segment.modCount;
          segment.enqueueNotification(
              key, hash, entryValue, valueReference.getWeight(), RemovalCause.REPLACED);
          segment.setValue(e, key, newValue, now);
          segment.evictEntries(e);
          return entryValue;
        }
      }

      return null;
    } finally {
      segment.unlock();
      segment.postWriteCleanup();
    }
  }

  static <K, V> @Nullable V remove(LocalCache.Segment<K, V> segment, Object key, int hash) {
    segment.lock();
    try {
      long now = segment.map.ticker.read();
      segment.preWriteCleanup(now);

      int newCount = segment.count - 1;
      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (ReferenceEntry<K, V> e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          LocalCache.ValueReference<K, V> valueReference = e.getValueReference();
          V entryValue = valueReference.get();

          RemovalCause cause;
          if (entryValue != null) {
            cause = RemovalCause.EXPLICIT;
          } else if (valueReference.isActive()) {
            cause = RemovalCause.COLLECTED;
          } else {
            return null;
          }

          ++segment.modCount;
          ReferenceEntry<K, V> newFirst =
              segment.removeValueFromChain(first, e, entryKey, hash, entryValue, valueReference, cause);
          newCount = segment.count - 1;
          table.set(index, newFirst);
          segment.count = newCount;
          return entryValue;
        }
      }

      return null;
    } finally {
      segment.unlock();
      segment.postWriteCleanup();
    }
  }

  static <K, V> boolean remove(
      LocalCache.Segment<K, V> segment, Object key, int hash, Object value) {
    segment.lock();
    try {
      long now = segment.map.ticker.read();
      segment.preWriteCleanup(now);

      int newCount = segment.count - 1;
      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (ReferenceEntry<K, V> e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          LocalCache.ValueReference<K, V> valueReference = e.getValueReference();
          V entryValue = valueReference.get();

          RemovalCause cause;
          if (segment.map.valueEquivalence.equivalent(value, entryValue)) {
            cause = RemovalCause.EXPLICIT;
          } else if (entryValue == null && valueReference.isActive()) {
            cause = RemovalCause.COLLECTED;
          } else {
            return false;
          }

          ++segment.modCount;
          ReferenceEntry<K, V> newFirst =
              segment.removeValueFromChain(first, e, entryKey, hash, entryValue, valueReference, cause);
          newCount = segment.count - 1;
          table.set(index, newFirst);
          segment.count = newCount;
          return (cause == RemovalCause.EXPLICIT);
        }
      }

      return false;
    } finally {
      segment.unlock();
      segment.postWriteCleanup();
    }
  }

  @CanIgnoreReturnValue
  static <K, V> boolean storeLoadedValue(
      LocalCache.Segment<K, V> segment,
      K key,
      int hash,
      LocalCache.LoadingValueReference<K, V> oldValueReference,
      V newValue) {
    segment.lock();
    try {
      long now = segment.map.ticker.read();
      segment.preWriteCleanup(now);

      int newCount = segment.count + 1;
      if (newCount > segment.threshold) {
        segment.expand();
        newCount = segment.count + 1;
      }

      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (ReferenceEntry<K, V> e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          LocalCache.ValueReference<K, V> valueReference = e.getValueReference();
          V entryValue = valueReference.get();
          if (oldValueReference == valueReference
              || (entryValue == null && valueReference != LocalCache.UNSET)) {
            ++segment.modCount;
            if (oldValueReference.isActive()) {
              RemovalCause cause =
                  (entryValue == null) ? RemovalCause.COLLECTED : RemovalCause.REPLACED;
              segment.enqueueNotification(
                  key, hash, entryValue, oldValueReference.getWeight(), cause);
              newCount--;
            }
            segment.setValue(e, key, newValue, now);
            segment.count = newCount;
            segment.evictEntries(e);
            return true;
          }

          segment.enqueueNotification(key, hash, newValue, 0, RemovalCause.REPLACED);
          return false;
        }
      }

      ++segment.modCount;
      ReferenceEntry<K, V> newEntry = segment.newEntry(key, hash, first);
      segment.setValue(newEntry, key, newValue, now);
      table.set(index, newEntry);
      segment.count = newCount;
      segment.evictEntries(newEntry);
      return true;
    } finally {
      segment.unlock();
      segment.postWriteCleanup();
    }
  }
}
