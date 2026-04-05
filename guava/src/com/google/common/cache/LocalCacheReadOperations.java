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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.jspecify.annotations.Nullable;

final class LocalCacheReadOperations<K, V> {
  private LocalCacheReadOperations() {}

  @CanIgnoreReturnValue
  static <K, V> V get(
      LocalCache.Segment<K, V> segment, K key, int hash, CacheLoader<? super K, V> loader)
      throws ExecutionException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(loader);
    try {
      if (segment.count != 0) {
        ReferenceEntry<K, V> e = getEntry(segment, key, hash);
        if (e != null) {
          long now = segment.map.ticker.read();
          V value = getLiveValue(segment, e, now);
          if (value != null) {
            segment.recordRead(e, now);
            segment.statsRecorder.recordHit(1);
            return segment.scheduleRefresh(e, key, hash, value, now, loader);
          }
          LocalCache.ValueReference<K, V> valueReference = e.getValueReference();
          if (valueReference.isLoading()) {
            return waitForLoadingValue(segment, e, key, valueReference);
          }
        }
      }

      return lockedGetOrLoad(segment, key, hash, loader);
    } catch (ExecutionException ee) {
      Throwable cause = ee.getCause();
      if (cause instanceof Error) {
        throw new ExecutionError((Error) cause);
      } else if (cause instanceof RuntimeException) {
        throw new UncheckedExecutionException(cause);
      }
      throw ee;
    } finally {
      segment.postReadCleanup();
    }
  }

  static <K, V> @Nullable V get(LocalCache.Segment<K, V> segment, Object key, int hash) {
    try {
      if (segment.count != 0) {
        long now = segment.map.ticker.read();
        ReferenceEntry<K, V> e = getLiveEntry(segment, key, hash, now);
        if (e == null) {
          return null;
        }

        V value = e.getValueReference().get();
        if (value != null) {
          segment.recordRead(e, now);
          return segment.scheduleRefresh(e, e.getKey(), hash, value, now, segment.map.defaultLoader);
        }
        segment.tryDrainReferenceQueues();
      }
      return null;
    } finally {
      segment.postReadCleanup();
    }
  }

  static <K, V> V lockedGetOrLoad(
      LocalCache.Segment<K, V> segment, K key, int hash, CacheLoader<? super K, V> loader)
      throws ExecutionException {
    ReferenceEntry<K, V> e;
    LocalCache.ValueReference<K, V> valueReference = null;
    LocalCache.LoadingValueReference<K, V> loadingValueReference = null;
    boolean createNewEntry = true;

    segment.lock();
    try {
      long now = segment.map.ticker.read();
      segment.preWriteCleanup(now);

      int newCount = segment.count - 1;
      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          valueReference = e.getValueReference();
          if (valueReference.isLoading()) {
            createNewEntry = false;
          } else {
            V value = valueReference.get();
            if (value == null) {
              segment.enqueueNotification(
                  entryKey, hash, value, valueReference.getWeight(), RemovalCause.COLLECTED);
            } else if (segment.map.isExpired(e, now)) {
              segment.enqueueNotification(
                  entryKey, hash, value, valueReference.getWeight(), RemovalCause.EXPIRED);
            } else {
              segment.recordLockedRead(e, now);
              segment.statsRecorder.recordHit(1);
              return value;
            }

            segment.writeQueue.remove(e);
            segment.accessQueue.remove(e);
            segment.count = newCount;
          }
          break;
        }
      }

      if (createNewEntry) {
        loadingValueReference = new LocalCache.LoadingValueReference<>();

        if (e == null) {
          e = segment.newEntry(key, hash, first);
          e.setValueReference(loadingValueReference);
          table.set(index, e);
        } else {
          e.setValueReference(loadingValueReference);
        }
      }
    } finally {
      segment.unlock();
      segment.postWriteCleanup();
    }

    if (createNewEntry) {
      try {
        synchronized (e) {
          return segment.loadSync(key, hash, loadingValueReference, loader);
        }
      } finally {
        segment.statsRecorder.recordMiss(1);
      }
    } else {
      return waitForLoadingValue(segment, e, key, valueReference);
    }
  }

  static <K, V> V waitForLoadingValue(
      LocalCache.Segment<K, V> segment,
      ReferenceEntry<K, V> e,
      K key,
      LocalCache.ValueReference<K, V> valueReference)
      throws ExecutionException {
    if (!valueReference.isLoading()) {
      throw new AssertionError();
    }

    Preconditions.checkState(!Thread.holdsLock(e), "Recursive load of: %s", key);
    try {
      V value = valueReference.waitForValue();
      if (value == null) {
        throw new InvalidCacheLoadException("CacheLoader returned null for key " + key + ".");
      }
      long now = segment.map.ticker.read();
      segment.recordRead(e, now);
      return value;
    } finally {
      segment.statsRecorder.recordMiss(1);
    }
  }

  static <K, V> @Nullable ReferenceEntry<K, V> getEntry(
      LocalCache.Segment<K, V> segment, Object key, int hash) {
    for (ReferenceEntry<K, V> e = segment.getFirst(hash); e != null; e = e.getNext()) {
      if (e.getHash() != hash) {
        continue;
      }

      K entryKey = e.getKey();
      if (entryKey == null) {
        segment.tryDrainReferenceQueues();
        continue;
      }

      if (segment.map.keyEquivalence.equivalent(key, entryKey)) {
        return e;
      }
    }

    return null;
  }

  static <K, V> @Nullable ReferenceEntry<K, V> getLiveEntry(
      LocalCache.Segment<K, V> segment, Object key, int hash, long now) {
    ReferenceEntry<K, V> e = getEntry(segment, key, hash);
    if (e == null) {
      return null;
    } else if (segment.map.isExpired(e, now)) {
      segment.tryExpireEntries(now);
      return null;
    }
    return e;
  }

  static <K, V> @Nullable V getLiveValue(
      LocalCache.Segment<K, V> segment, ReferenceEntry<K, V> entry, long now) {
    if (entry.getKey() == null) {
      segment.tryDrainReferenceQueues();
      return null;
    }
    V value = entry.getValueReference().get();
    if (value == null) {
      segment.tryDrainReferenceQueues();
      return null;
    }

    if (segment.map.isExpired(entry, now)) {
      segment.tryExpireEntries(now);
      return null;
    }
    return value;
  }

  static <K, V> boolean containsKey(LocalCache.Segment<K, V> segment, Object key, int hash) {
    try {
      if (segment.count != 0) {
        long now = segment.map.ticker.read();
        ReferenceEntry<K, V> e = getLiveEntry(segment, key, hash, now);
        if (e == null) {
          return false;
        }
        return e.getValueReference().get() != null;
      }

      return false;
    } finally {
      segment.postReadCleanup();
    }
  }

  static <K, V> boolean containsValue(LocalCache.Segment<K, V> segment, Object value) {
    try {
      if (segment.count != 0) {
        long now = segment.map.ticker.read();
        AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
        int length = table.length();
        for (int i = 0; i < length; ++i) {
          for (ReferenceEntry<K, V> e = table.get(i); e != null; e = e.getNext()) {
            V entryValue = getLiveValue(segment, e, now);
            if (entryValue == null) {
              continue;
            }
            if (segment.map.valueEquivalence.equivalent(value, entryValue)) {
              return true;
            }
          }
        }
      }

      return false;
    } finally {
      segment.postReadCleanup();
    }
  }
}
