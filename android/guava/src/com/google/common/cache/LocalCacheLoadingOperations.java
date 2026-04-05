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

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;

import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;
import org.jspecify.annotations.Nullable;

final class LocalCacheLoadingOperations<K, V> {
  private LocalCacheLoadingOperations() {}

  static <K, V> V loadSync(
      LocalCache.Segment<K, V> segment,
      K key,
      int hash,
      LocalCache.LoadingValueReference<K, V> loadingValueReference,
      CacheLoader<? super K, V> loader)
      throws ExecutionException {
    ListenableFuture<V> loadingFuture = loadingValueReference.loadFuture(key, loader);
    return getAndRecordStats(segment, key, hash, loadingValueReference, loadingFuture);
  }

  static <K, V> ListenableFuture<V> loadAsync(
      LocalCache.Segment<K, V> segment,
      K key,
      int hash,
      LocalCache.LoadingValueReference<K, V> loadingValueReference,
      CacheLoader<? super K, V> loader) {
    ListenableFuture<V> loadingFuture = loadingValueReference.loadFuture(key, loader);
    loadingFuture.addListener(
        () -> {
          try {
            getAndRecordStats(segment, key, hash, loadingValueReference, loadingFuture);
          } catch (Throwable t) {
            LocalCache.logger.log(Level.WARNING, "Exception thrown during refresh", t);
            loadingValueReference.setException(t);
          }
        },
        directExecutor());
    return loadingFuture;
  }

  @CanIgnoreReturnValue
  static <K, V> V getAndRecordStats(
      LocalCache.Segment<K, V> segment,
      K key,
      int hash,
      LocalCache.LoadingValueReference<K, V> loadingValueReference,
      ListenableFuture<V> newValue)
      throws ExecutionException {
    V value = null;
    try {
      value = getUninterruptibly(newValue);
      if (value == null) {
        throw new InvalidCacheLoadException("CacheLoader returned null for key " + key + ".");
      }
      segment.statsRecorder.recordLoadSuccess(loadingValueReference.elapsedNanos());
      segment.storeLoadedValue(key, hash, loadingValueReference, value);
      return value;
    } finally {
      if (value == null) {
        segment.statsRecorder.recordLoadException(loadingValueReference.elapsedNanos());
        segment.removeLoadingValue(key, hash, loadingValueReference);
      }
    }
  }

  static <K, V> V scheduleRefresh(
      LocalCache.Segment<K, V> segment,
      ReferenceEntry<K, V> entry,
      K key,
      int hash,
      V oldValue,
      long now,
      CacheLoader<? super K, V> loader) {
    if (segment.map.refreshes()
        && (now - entry.getWriteTime() > segment.map.refreshNanos)
        && !entry.getValueReference().isLoading()) {
      V newValue = refresh(segment, key, hash, loader, true);
      if (newValue != null) {
        return newValue;
      }
    }
    return oldValue;
  }

  @CanIgnoreReturnValue
  static <K, V> @Nullable V refresh(
      LocalCache.Segment<K, V> segment,
      K key,
      int hash,
      CacheLoader<? super K, V> loader,
      boolean checkTime) {
    LocalCache.LoadingValueReference<K, V> loadingValueReference =
        insertLoadingValueReference(segment, key, hash, checkTime);
    if (loadingValueReference == null) {
      return null;
    }

    ListenableFuture<V> result = loadAsync(segment, key, hash, loadingValueReference, loader);
    if (result.isDone()) {
      try {
        return Uninterruptibles.getUninterruptibly(result);
      } catch (Throwable t) {
        // don't let refresh exceptions propagate; error was already logged
      }
    }
    return null;
  }

  static <K, V> LocalCache.LoadingValueReference<K, V> insertLoadingValueReference(
      LocalCache.Segment<K, V> segment, K key, int hash, boolean checkTime) {
    ReferenceEntry<K, V> e = null;
    segment.lock();
    try {
      long now = segment.map.ticker.read();
      segment.preWriteCleanup(now);

      AtomicReferenceArray<ReferenceEntry<K, V>> table = segment.table;
      int index = hash & (table.length() - 1);
      ReferenceEntry<K, V> first = table.get(index);

      for (e = first; e != null; e = e.getNext()) {
        K entryKey = e.getKey();
        if (e.getHash() == hash
            && entryKey != null
            && segment.map.keyEquivalence.equivalent(key, entryKey)) {
          LocalCache.ValueReference<K, V> valueReference = e.getValueReference();
          if (valueReference.isLoading()
              || (checkTime && (now - e.getWriteTime() < segment.map.refreshNanos))) {
            return null;
          }

          ++segment.modCount;
          LocalCache.LoadingValueReference<K, V> loadingValueReference =
              new LocalCache.LoadingValueReference<>(valueReference);
          e.setValueReference(loadingValueReference);
          return loadingValueReference;
        }
      }

      ++segment.modCount;
      LocalCache.LoadingValueReference<K, V> loadingValueReference =
          new LocalCache.LoadingValueReference<>();
      e = segment.newEntry(key, hash, first);
      e.setValueReference(loadingValueReference);
      table.set(index, e);
      return loadingValueReference;
    } finally {
      segment.unlock();
      segment.postWriteCleanup();
    }
  }
}
