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

final class LocalCacheAccessMetadata<K, V> {
  private LocalCacheAccessMetadata() {}

  static <K, V> void recordRead(
      LocalCache.Segment<K, V> segment, ReferenceEntry<K, V> entry, long now) {
    if (segment.map.recordsAccess()) {
      entry.setAccessTime(now);
    }
    segment.recencyQueue.add(entry);
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void recordLockedRead(
      LocalCache.Segment<K, V> segment, ReferenceEntry<K, V> entry, long now) {
    if (segment.map.recordsAccess()) {
      entry.setAccessTime(now);
    }
    segment.accessQueue.add(entry);
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void recordWrite(
      LocalCache.Segment<K, V> segment, ReferenceEntry<K, V> entry, int weight, long now) {
    drainRecencyQueue(segment);
    segment.totalWeight += weight;

    if (segment.map.recordsAccess()) {
      entry.setAccessTime(now);
    }
    if (segment.map.recordsWrite()) {
      entry.setWriteTime(now);
    }
    segment.accessQueue.add(entry);
    segment.writeQueue.add(entry);
  }

  @SuppressWarnings("GuardedBy")
  static <K, V> void drainRecencyQueue(LocalCache.Segment<K, V> segment) {
    ReferenceEntry<K, V> entry;
    while ((entry = segment.recencyQueue.poll()) != null) {
      if (segment.accessQueue.contains(entry)) {
        segment.accessQueue.add(entry);
      }
    }
  }
}
