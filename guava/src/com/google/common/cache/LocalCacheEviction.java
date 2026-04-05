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

import org.jspecify.annotations.Nullable;

final class LocalCacheEviction<K, V> {
  private LocalCacheEviction() {}

  @SuppressWarnings("GuardedBy")
  static <K, V> void expireEntries(LocalCache.Segment<K, V> segment, long now) {
    segment.drainRecencyQueue();

    ReferenceEntry<K, V> e;
    while ((e = segment.writeQueue.peek()) != null && segment.map.isExpired(e, now)) {
      if (!segment.removeEntry(e, e.getHash(), RemovalCause.EXPIRED)) {
        throw new AssertionError();
      }
    }
    while ((e = segment.accessQueue.peek()) != null && segment.map.isExpired(e, now)) {
      if (!segment.removeEntry(e, e.getHash(), RemovalCause.EXPIRED)) {
        throw new AssertionError();
      }
    }
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void enqueueNotification(
      LocalCache.Segment<K, V> segment,
      @Nullable K key,
      int hash,
      @Nullable V value,
      int weight,
      RemovalCause cause) {
    segment.totalWeight -= weight;
    if (cause.wasEvicted()) {
      segment.statsRecorder.recordEviction();
    }
    if (segment.map.removalNotificationQueue != LocalCache.DISCARDING_QUEUE) {
      RemovalNotification<K, V> notification = RemovalNotification.create(key, value, cause);
      segment.map.removalNotificationQueue.offer(notification);
    }
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void evictEntries(LocalCache.Segment<K, V> segment, ReferenceEntry<K, V> newest) {
    if (!segment.map.evictsBySize()) {
      return;
    }

    segment.drainRecencyQueue();

    if (newest.getValueReference().getWeight() > segment.maxSegmentWeight) {
      if (!segment.removeEntry(newest, newest.getHash(), RemovalCause.SIZE)) {
        throw new AssertionError();
      }
    }

    while (segment.totalWeight > segment.maxSegmentWeight) {
      ReferenceEntry<K, V> e = getNextEvictable(segment);
      if (!segment.removeEntry(e, e.getHash(), RemovalCause.SIZE)) {
        throw new AssertionError();
      }
    }
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> ReferenceEntry<K, V> getNextEvictable(LocalCache.Segment<K, V> segment) {
    for (ReferenceEntry<K, V> e : segment.accessQueue) {
      int weight = e.getValueReference().getWeight();
      if (weight > 0) {
        return e;
      }
    }
    throw new AssertionError();
  }
}
