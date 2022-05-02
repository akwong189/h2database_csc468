/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.h2.mvstore.DataUtils;

/**
 * A scan resistant cache that uses keys of type long. It is meant to cache
 * objects that are relatively costly to acquire, for example file content.
 * <p>
 * This implementation is multi-threading safe and supports concurrent access.
 * Null keys or null values are not allowed. The map fill factor is at most 75%.
 * <p>
 * Each entry is assigned a distinct memory size, and the cache will try to use
 * at most the specified amount of memory. The memory unit is not relevant,
 * however it is suggested to use bytes as the unit.
 * <p>
 * This class implements an approximation of the LIRS replacement algorithm
 * invented by Xiaodong Zhang and Song Jiang as described in
 * https://web.cse.ohio-state.edu/~zhang.574/lirs-sigmetrics-02.html with a few
 * smaller changes: An additional queue for non-resident entries is used, to
 * prevent unbound memory usage. The maximum size of this queue is at most the
 * size of the rest of the stack. About 6.25% of the mapped entries are cold.
 * <p>
 * Internally, the cache is split into a number of segments, and each segment is
 * an individual LIRS cache.
 * <p>
 * Accessed entries are only moved to the top of the stack if at least a number
 * of other entries have been moved to the front (8 per segment by default).
 * Write access and moving entries to the top of the stack is synchronized per
 * segment.
 *
 * @author Thomas Mueller
 * @param <V> the value type
 */
public class CacheParent<V> { // Curreently just all copy pasted code from longkeylirs

    /**
     * The maximum memory this cache should use.
     */
    private long maxMemory;

    private final Segment<V>[] segments;

    private final int segmentCount;
    private final int segmentShift;
    private final int segmentMask;
    private final int stackMoveDistance;
    private final int nonResidentQueueSize;
    private final int nonResidentQueueSizeHigh;

    /**
     * Create a new cache with the given memory size.
     *
     * @param config the configuration
     */
    @SuppressWarnings("unchecked")
    public CacheParent(Config config) {
        setMaxMemory(config.maxMemory);
        this.nonResidentQueueSize = config.nonResidentQueueSize;
        this.nonResidentQueueSizeHigh = config.nonResidentQueueSizeHigh;
        DataUtils.checkArgument(
                Integer.bitCount(config.segmentCount) == 1,
                "The segment count must be a power of 2, is {0}", config.segmentCount);
        this.segmentCount = config.segmentCount;
        this.segmentMask = segmentCount - 1;
        this.stackMoveDistance = config.stackMoveDistance;
        segments = new Segment[segmentCount];
        clear();
        // use the high bits for the segment
        this.segmentShift = 32 - Integer.bitCount(segmentMask);
    }

    /**
     * Remove all entries.
     */
    public void clear() {
        long max = getMaxItemSize();
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment<>(max, stackMoveDistance, 8, nonResidentQueueSize,
                                        nonResidentQueueSizeHigh);
        }
    }

    /**
     * Determines max size of the data item size to fit into cache
     * @return data items size limit
     */
    public long getMaxItemSize() {
        return Math.max(1, maxMemory / segmentCount);
    }

    private Entry<V> find(long key) {
        int hash = Segment.getHash(key);
        return getSegment(hash).find(key, hash);
    }

    /**
     * Check whether there is a resident entry for the given key. This
     * method does not adjust the internal state of the cache.
     *
     * @param key the key (may not be null)
     * @return true if there is a resident entry
     */
    public boolean containsKey(long key) {
        Entry<V> e = find(key);
        return e != null && e.value != null;
    }

    /**
     * Get the value for the given key if the entry is cached. This method does
     * not modify the internal state.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V peek(long key) {
        Entry<V> e = find(key);
        return e == null ? null : e.getValue();
    }

    /**
     * Add an entry to the cache using the average memory size.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value, or null if there was no resident entry
     */
    public V put(long key, V value) {
        return put(key, value, sizeOf(value));
    }

    /**
     * Add an entry to the cache. The entry may or may not exist in the
     * cache yet. This method will usually mark unknown entries as cold and
     * known entries as hot.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @param memory the memory used for the given entry
     * @return the old value, or null if there was no resident entry
     */
    public V put(long key, V value, int memory) {
        if (value == null) {
            throw DataUtils.newIllegalArgumentException(
                    "The value may not be null");
        }
        int hash = Segment.getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> s = segments[segmentIndex];
        // check whether resize is required: synchronize on s, to avoid
        // concurrent resizes (concurrent reads read
        // from the old segment)
        synchronized (s) {
            s = resizeIfNeeded(s, segmentIndex);
            return s.put(key, hash, value, memory);
        }
    }

    private Segment<V> resizeIfNeeded(Segment<V> s, int segmentIndex) {
        int newLen = s.getNewMapLen();
        if (newLen == 0) {
            return s;
        }
        // another thread might have resized
        // (as we retrieved the segment before synchronizing on it)
        Segment<V> s2 = segments[segmentIndex];
        if (s == s2) {
            // no other thread resized, so we do
            s = new Segment<>(s, newLen);
            segments[segmentIndex] = s;
        }
        return s;
    }

    /**
     * Get the size of the given value. The default implementation returns 1.
     *
     * @param value the value
     * @return the size
     */
    @SuppressWarnings("unused")
    protected int sizeOf(V value) {
        return 1;
    }

    /**
     * Remove an entry. Both resident and non-resident entries can be
     * removed.
     *
     * @param key the key (may not be null)
     * @return the old value, or null if there was no resident entry
     */
    public V remove(long key) {
        int hash = Segment.getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> s = segments[segmentIndex];
        // check whether resize is required: synchronize on s, to avoid
        // concurrent resizes (concurrent reads read
        // from the old segment)
        synchronized (s) {
            s = resizeIfNeeded(s, segmentIndex);
            return s.remove(key, hash);
        }
    }

    /**
     * Get the memory used for the given key.
     *
     * @param key the key (may not be null)
     * @return the memory, or 0 if there is no resident entry
     */
    public int getMemory(long key) {
        Entry<V> e = find(key);
        return e == null ? 0 : e.getMemory();
    }

    /**
     * Get the value for the given key if the entry is cached. This method
     * adjusts the internal state of the cache sometimes, to ensure commonly
     * used entries stay in the cache.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V get(long key) {
        int hash = Segment.getHash(key);
        Segment<V> s = getSegment(hash);
        Entry<V> e = s.find(key, hash);
        return s.get(e);
    }

    private Segment<V> getSegment(int hash) {
        return segments[getSegmentIndex(hash)];
    }

    private int getSegmentIndex(int hash) {
        return (hash >>> segmentShift) & segmentMask;
    }

    /**
     * Get the currently used memory.
     *
     * @return the used memory
     */
    public long getUsedMemory() {
        long x = 0;
        for (Segment<V> s : segments) {
            x += s.usedMemory;
        }
        return x;
    }

    /**
     * Set the maximum memory this cache should use. This will not
     * immediately cause entries to get removed however; it will only change
     * the limit. To resize the internal array, call the clear method.
     *
     * @param maxMemory the maximum size (1 or larger) in bytes
     */
    public void setMaxMemory(long maxMemory) {
        DataUtils.checkArgument(
                maxMemory > 0,
                "Max memory must be larger than 0, is {0}", maxMemory);
        this.maxMemory = maxMemory;
        if (segments != null) {
            long max = 1 + maxMemory / segments.length;
            for (Segment<V> s : segments) {
                s.setMaxMemory(max);
            }
        }
    }

    /**
     * Get the maximum memory to use.
     *
     * @return the maximum memory
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Get the entry set for all resident entries.
     *
     * @return the entry set
     */
    public synchronized Set<Map.Entry<Long, V>> entrySet() {
        return getMap().entrySet();
    }

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    public Set<Long> keySet() {
        HashSet<Long> set = new HashSet<>();
        for (Segment<V> s : segments) {
            set.addAll(s.keySet());
        }
        return set;
    }

    /**
     * Get the number of non-resident entries in the cache.
     *
     * @return the number of non-resident entries
     */
    public int sizeNonResident() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.queue2Size;
        }
        return x;
    }

    /**
     * Get the length of the internal map array.
     *
     * @return the size of the array
     */
    public int sizeMapArray() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.entries.length;
        }
        return x;
    }

    /**
     * Get the number of hot entries in the cache.
     *
     * @return the number of hot entries
     */
    public int sizeHot() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.mapSize - s.queueSize - s.queue2Size;
        }
        return x;
    }

    /**
     * Get the number of cache hits.
     *
     * @return the cache hits
     */
    public long getHits() {
        long x = 0;
        for (Segment<V> s : segments) {
            x += s.hits;
        }
        return x;
    }

    /**
     * Get the number of cache misses.
     *
     * @return the cache misses
     */
    public long getMisses() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.misses;
        }
        return x;
    }

    /**
     * Get the number of resident entries.
     *
     * @return the number of entries
     */
    public int size() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.mapSize - s.queue2Size;
        }
        return x;
    }

    /**
     * Get the list of keys. This method allows to read the internal state of
     * the cache.
     *
     * @param cold if true, only keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key list
     */
    public List<Long> keys(boolean cold, boolean nonResident) {
        ArrayList<Long> keys = new ArrayList<>();
        for (Segment<V> s : segments) {
            keys.addAll(s.keys(cold, nonResident));
        }
        return keys;
    }

    /**
     * Get the values for all resident entries.
     *
     * @return the entry set
     */
    public List<V> values() {
        ArrayList<V> list = new ArrayList<>();
        for (long k : keySet()) {
            V value = peek(k);
            if (value != null) {
                list.add(value);
            }
        }
        return list;
    }

    /**
     * Check whether the cache is empty.
     *
     * @return true if it is empty
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Check whether the given value is stored.
     *
     * @param value the value
     * @return true if it is stored
     */
    public boolean containsValue(V value) {
        return getMap().containsValue(value);
    }

    /**
     * Convert this cache to a map.
     *
     * @return the map
     */
    public Map<Long, V> getMap() {
        HashMap<Long, V> map = new HashMap<>();
        for (long k : keySet()) {
            V x = peek(k);
            if (x != null) {
                map.put(k, x);
            }
        }
        return map;
    }

    /**
     * Add all elements of the map to this cache.
     *
     * @param m the map
     */
    public void putAll(Map<Long, ? extends V> m) {
        for (Map.Entry<Long, ? extends V> e : m.entrySet()) {
            // copy only non-null entries
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * Loop through segments, trimming the non resident queue.
     */
    public void trimNonResidentQueue() {
        for (Segment<V> s : segments) {
            synchronized (s) {
                s.trimNonResidentQueue();
            }
        }
    }
}
