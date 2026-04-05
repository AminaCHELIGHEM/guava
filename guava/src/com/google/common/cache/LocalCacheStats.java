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
import com.google.common.cache.AbstractCache.StatsCounter;

final class LocalCacheStats implements StatsCounter {
  private final StatsCounter counter;

  LocalCacheStats(StatsCounter counter) {
    this.counter = Preconditions.checkNotNull(counter);
  }

  void recordHit(int count) {
    counter.recordHits(count);
  }

  void recordMiss(int count) {
    counter.recordMisses(count);
  }

  @Override
  public void recordHits(int count) {
    recordHit(count);
  }

  @Override
  public void recordMisses(int count) {
    recordMiss(count);
  }

  @Override
  public void recordLoadSuccess(long nanos) {
    counter.recordLoadSuccess(nanos);
  }

  @Override
  public void recordLoadException(long ns) {
    counter.recordLoadException(ns);
  }

  @Override
  public void recordEviction() {
    counter.recordEviction();
  }

  @Override
  public CacheStats snapshot() {
    return counter.snapshot();
  }
}
