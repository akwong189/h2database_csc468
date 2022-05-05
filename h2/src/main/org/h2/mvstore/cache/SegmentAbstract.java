package org.h2.mvstore.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.lang.ref.WeakReference;

/**
 * A cache segment
 *
 * @param <V> the value type
 */
abstract class Segment<V> {

    /**
     * The number of (hot, cold, and non-resident) entries in the map.
     */
    int mapSize;

    /**
     * The size of the LIRS queue for resident cold entries.
     */
    int queueSize;

    /**
     * The size of the LIRS queue for non-resident cold entries.
     */
    int queue2Size;

    /**
     * The number of cache hits.
     */
    long hits;

    /**
     * The number of cache misses.
     */
    long misses;

    /**
     * The map array. The size is always a power of 2.
     */
    final Entry<V>[] entries;

    /**
     * The currently used memory.
     */
    long usedMemory;

    /**
     * How many other item are to be moved to the top of the stack before
     * the current item is moved.
     */
    private final int stackMoveDistance;

    /**
     * The maximum memory this cache should use in bytes.
     */
    private long maxMemory;

    /**
     * The bit mask that is applied to the key hash code to get the index in
     * the map array. The mask is the length of the array minus one.
     */
    private final int mask;


    /**
     * Create a new cache segment.
     *  @param maxMemory the maximum memory to use
     * @param stackMoveDistance the number of other entries to be moved to
     *        the top of the stack before moving an entry to the top
     * @param len the number of hash table buckets (must be a power of 2)
     * @param nonResidentQueueSize the non-resident queue size low watermark factor
     * @param nonResidentQueueSizeHigh  the non-resident queue size high watermark factor
     */
    abstract Segment(long maxMemory);


    /**
     * Create a new cache segment from an existing one.
     * The caller must synchronize on the old segment, to avoid
     * concurrent modifications.
     *
     * @param old the old segment
     * @param len the number of hash table buckets (must be a power of 2)
     */

     // Stack and queue can be overriden in other versions
    abstract Segment(Segment<V> old, int len);
    /**
     * Calculate the new number of hash table buckets if the internal map
     * should be re-sized.
     *
     * @return 0 if no resizing is needed, or the new length
     */
    int getNewMapLen() {
        int len = mask + 1;
        if (len * 3 < mapSize * 4 && len < (1 << 28)) {
            // more than 75% usage
            return len * 2;
        } else if (len > 32 && len / 8 > mapSize) {
            // less than 12% usage
            return len / 2;
        }
        return 0;
    }

    private void addToMap(Entry<V> e) {
        int index = getHash(e.key) & mask;
        e.mapNext = entries[index];
        entries[index] = e;
        usedMemory += e.getMemory();
        mapSize++;
    }

    /**
     * Get the value from the given entry.
     * This method adjusts the internal state of the cache sometimes,
     * to ensure commonly used entries stay in the cache.
     *
     * @param e the entry
     * @return the value, or null if there is no resident entry
     */
    synchronized V get(Entry<V> e) {
        V value = e == null ? null : e.getValue();
        if (value == null) {
            // the entry was not found
            // or it was a non-resident entry
            misses++;
        } else {
            access(e);
            hits++;
        }
        return value;
    }

    /**
     * Access an item, moving the entry to the top of the stack or front of
     * the queue if found.
     *
     * @param e entry to record access for
     */

     // Can be rewritten to not have stack and queue stuff, and just access the hashmap
    abstract void access(Entry<V> e);

    /**
     * Add an entry to the cache. The entry may or may not exist in the
     * cache yet. This method will usually mark unknown entries as cold and
     * known entries as hot.
     *
     * @param key the key (may not be null)
     * @param hash the hash
     * @param value the value (may not be null)
     * @param memory the memory used for the given entry
     * @return the old value, or null if there was no resident entry
     */
    synchronized V put(long key, int hash, V value, int memory) {
        Entry<V> e = find(key, hash);
        boolean existed = e != null;
        V old = null;
        if (existed) {
            old = e.getValue();
            remove(key, hash);
        }
        if (memory > maxMemory) {
            // the new entry is too big to fit
            return old;
        }
        e = new Entry<>(key, value, memory);
        int index = hash & mask;
        e.mapNext = entries[index];
        entries[index] = e;
        usedMemory += memory;
        if (usedMemory > maxMemory) {
            // old entries needs to be removed
            evict();
            // if the cache is full, the new entry is
            // cold if possible
            if (stackSize > 0) {
                // the new cold entry is at the top of the queue
                addToQueue(queue, e);
            }
        }
        mapSize++;
        // added entries are always added to the stack
        addToStack(e);
        if (existed) {
            // if it was there before (even non-resident), it becomes hot
            access(e);
        }
        return old;
    }

    /**
     * Remove an entry. Both resident and non-resident entries can be
     * removed.
     *
     * @param key the key (may not be null)
     * @param hash the hash
     * @return the old value, or null if there was no resident entry
     */
    abstract V remove(long key, int hash);

    /**
     * Evict cold entries (resident and non-resident) until the memory limit
     * is reached. The new entry is added as a cold entry, except if it is
     * the only entry.
     */
    private void evict() {
        do {
            evictBlock();
        } while (usedMemory > maxMemory);
    }


    abstract void evictBlock();

    /**
     * Try to find an entry in the map.
     *
     * @param key the key
     * @param hash the hash
     * @return the entry (might be a non-resident)
     */
    Entry<V> find(long key, int hash) {
        int index = hash & mask;
        Entry<V> e = entries[index];
        while (e != null && e.key != key) {
            e = e.mapNext;
        }
        return e;
    }

    /**
     * Get the list of keys. This method allows to read the internal state
     * of the cache.
     *
     * @param cold if true, only keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key list
     */
    abstract synchronized List<Long> keys(boolean cold, boolean nonResident);

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    abstract synchronized Set<Long> keySet();

    /**
     * Set the maximum memory this cache should use. This will not
     * immediately cause entries to get removed however; it will only change
     * the limit. To resize the internal array, call the clear method.
     *
     * @param maxMemory the maximum size (1 or larger) in bytes
     */
    void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }


    /**
     * Get the hash code for the given key. The hash code is
     * further enhanced to spread the values more evenly.
     *
     * @param key the key
     * @return the hash code
     */
    static int getHash(long key) {
        int hash = (int) ((key >>> 32) ^ key);
        // a supplemental secondary hash function
        // to protect against hash codes that don't differ much
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }
}