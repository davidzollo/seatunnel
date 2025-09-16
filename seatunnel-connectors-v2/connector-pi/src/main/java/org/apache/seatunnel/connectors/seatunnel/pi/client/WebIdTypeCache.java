/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.pi.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Professional WebID type cache manager for PI Web API optimization.
 *
 * <p>This cache stores the mapping between WebID and its type (Points vs Attributes) to avoid
 * repeated API calls and improve performance.
 *
 * <p>Features: - Thread-safe concurrent access - Cache statistics for monitoring - Memory-efficient
 * storage - Simple LRU-like behavior with size limit
 */
public class WebIdTypeCache {

    /** Maximum cache size to prevent memory issues */
    private static final int DEFAULT_MAX_SIZE = 10000;

    /** Cache storage: WebID -> isAttribute flag */
    private final ConcurrentHashMap<String, Boolean> cache;

    /** Maximum cache size */
    private final int maxSize;

    /** Cache statistics */
    private final AtomicLong hitCount = new AtomicLong(0);

    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong putCount = new AtomicLong(0);

    /** Create WebID type cache with default size */
    public WebIdTypeCache() {
        this(DEFAULT_MAX_SIZE);
    }

    /**
     * Create WebID type cache with specified size
     *
     * @param maxSize maximum number of entries to cache
     */
    public WebIdTypeCache(int maxSize) {
        this.maxSize = maxSize;
        this.cache = new ConcurrentHashMap<>(Math.min(maxSize, 1024));
    }

    /**
     * Get WebID type from cache
     *
     * @param webId the WebID to lookup
     * @return true if WebID is for Attribute, false if for Points, null if not cached
     */
    public Boolean getType(String webId) {
        if (webId == null) {
            return null;
        }

        Boolean result = cache.get(webId);
        if (result != null) {
            hitCount.incrementAndGet();
        } else {
            missCount.incrementAndGet();
        }

        return result;
    }

    /**
     * Cache WebID type information
     *
     * @param webId the WebID to cache
     * @param isAttribute true if WebID is for Attribute, false if for Points
     */
    public void putType(String webId, boolean isAttribute) {
        if (webId == null) {
            return;
        }

        // Improved size management: remove oldest entries when cache is full
        if (cache.size() >= maxSize) {
            // Remove approximately 25% of entries to avoid frequent clearing
            final int removeCount = maxSize / 4;
            final AtomicInteger counter = new AtomicInteger(removeCount);
            cache.entrySet().removeIf(entry -> counter.getAndDecrement() > 0);

            if (cache.size() >= maxSize) {
                // Fallback: clear cache if removal didn't work
                cache.clear();
            }
        }

        cache.put(webId, isAttribute);
        putCount.incrementAndGet();
    }

    /**
     * Check if WebID type is cached
     *
     * @param webId the WebID to check
     * @return true if cached, false otherwise
     */
    public boolean containsType(String webId) {
        return webId != null && cache.containsKey(webId);
    }

    /** Clear all cached entries */
    public void clear() {
        cache.clear();
    }

    /**
     * Get current cache size
     *
     * @return number of cached entries
     */
    public int size() {
        return cache.size();
    }

    /**
     * Get cache hit rate
     *
     * @return hit rate as percentage (0.0 to 1.0)
     */
    public double getHitRate() {
        long hits = hitCount.get();
        long total = hits + missCount.get();
        return total == 0 ? 0.0 : (double) hits / total;
    }

    /**
     * Get cache statistics summary
     *
     * @return formatted statistics string
     */
    public String getStatistics() {
        return String.format(
                "WebIdTypeCache[size=%d, hits=%d, misses=%d, puts=%d, hitRate=%.2f%%]",
                size(), hitCount.get(), missCount.get(), putCount.get(), getHitRate() * 100);
    }
}
