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

import static com.google.common.util.concurrent.Futures.immediateFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import org.jspecify.annotations.Nullable;

final class LocalCacheComputeOperations<K, V> {
  private LocalCacheComputeOperations() {}

  static <K, V> @Nullable V compute(
      LocalCache.Segment<K, V> segment,
      K key,
      int hash,
      BiFunction<? super K, ? super @Nullable V, ? extends @Nullable V> function) {
    ReferenceEntry<K, V> entry;
    LocalCache.ValueReference<K, V> valueReference = null;
    ComputingValueReference<K, V> computingValueReference = null;
    boolean createNewEntry = true;
    V newValue;

    segment.lock();
    try {
      long now = segment.map.ticker.read();
      segment.preWriteCleanup(now);

      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (entry = first; entry != null; entry = entry.getNext()) {
        K entryKey = entry.getKey();
        if (entry.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          valueReference = entry.getValueReference();
          if (segment.map.isExpired(entry, now)) {
            segment.enqueueNotification(
                entryKey,
                hash,
                valueReference.get(),
                valueReference.getWeight(),
                RemovalCause.EXPIRED);
          }

          segment.writeQueue.remove(entry);
          segment.accessQueue.remove(entry);
          createNewEntry = false;
          break;
        }
      }

      computingValueReference = new ComputingValueReference<>(valueReference);
      newValue = computingValueReference.compute(key, function);

      if (entry == null) {
        createNewEntry = true;
        entry = segment.newEntry(key, hash, first);
        entry.setValueReference(computingValueReference);
        table.set(index, entry);
      } else {
        entry.setValueReference(computingValueReference);
      }

      if (newValue != null) {
        if (valueReference != null && newValue == valueReference.get()) {
          computingValueReference.set(newValue);
          entry.setValueReference(valueReference);
          segment.recordWrite(entry, 0, now);
          return newValue;
        }
        try {
          return segment.getAndRecordStats(key, hash, computingValueReference, immediateFuture(newValue));
        } catch (ExecutionException exception) {
          throw new AssertionError("impossible; Futures.immediateFuture can't throw");
        }
      } else if (createNewEntry || valueReference.isLoading()) {
        segment.removeLoadingValue(key, hash, computingValueReference);
        return null;
      } else {
        segment.removeEntry(entry, hash, RemovalCause.EXPLICIT);
        return null;
      }
    } finally {
      segment.unlock();
      segment.postWriteCleanup();
    }
  }

  static final class ComputingValueReference<K, V> extends LocalCache.LoadingValueReference<K, V> {
    ComputingValueReference(LocalCache.ValueReference<K, V> oldValue) {
      super(oldValue);
    }

    @Override
    public boolean isLoading() {
      return false;
    }
  }
}
