package org.h2.mvstore.cache;

import java.util.List;
import java.util.Set;

/**
 * A cache segment
 *
 * @param <V> the value type
 */
abstract class SegmentParent<V> {
    /**
     * The maximum memory this cache should use in bytes.
     */
    long maxMemory;
    int mask;
    int mapSize;

    long misses;
    long hits;

    /**
     * The map array. The size is always a power of 2.
     */
    final Entry<V>[] entries;

    /**
     * The currently used memory.
     */
    long usedMemory;

    /**
     * Create a new cache segment.
     *  @param maxMemory the maximum memory to use
     * @param stackMoveDistance the number of other entries to be moved to
     *        the top of the stack before moving an entry to the top
     * @param len the number of hash table buckets (must be a power of 2)
     * @param nonResidentQueueSize the non-resident queue size low watermark factor
     * @param nonResidentQueueSizeHigh  the non-resident queue size high watermark factor
     */
    SegmentParent(long maxMemory, int len) {
        this.maxMemory = maxMemory;
        mask = len - 1;
        setMaxMemory(maxMemory);

        @SuppressWarnings("unchecked")
        Entry<V>[] e = new Entry[len];
        entries = e;
     }

    // getNewMapLen(), addToMap(), get() all added to parent, commented out of Segment LIRS
    
    /**
     * Calculate the new number of hash table buckets if the internal map
     * should be re-sized.
     *
     * @return 0 if no resizing is needed, or the new length
     */
    // int getNewMapLen() {
    //     int len = mask + 1;
    //     if (len * 3 < mapSize * 4 && len < (1 << 28)) {
    //         // more than 75% usage
    //         return len * 2;
    //     } else if (len > 32 && len / 8 > mapSize) {
    //         // less than 12% usage
    //         return len / 2;
    //     }
    //     return 0;
    // }

    // void addToMap(Entry<V> e) {
    //     int index = SegmentParent.getHash(e.key) & mask;
    //     e.mapNext = entries[index];
    //     entries[index] = e;
    //     usedMemory += e.getMemory();
    //     mapSize++;
    // }

    /**
     * Get the value from the given entry.
     * This method adjusts the internal state of the cache sometimes,
     * to ensure commonly used entries stay in the cache.
     *
     * @param e the entry
     * @return the value, or null if there is no resident entry
     */
    // abstract V get(Entry<V> e);// {return null;}
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

    void access(Entry<V> e) {}

    /**
    * Access an item
    *
    * @param e entry to record access for
    */
    // This might just be only for lirs
    // abstract void access(Entry<V> e);

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
    abstract V put(long key, int hash, V value, int memory);// {return null;}

    /**
     * Remove an entry
     *
     * @param key the key (may not be null)
     * @param hash the hash
     * @return the old value, or null if there was no resident entry
     */
    abstract V remove(long key, int hash);// {return null;}


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
    List<Long> keys() {return null;}

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    abstract Set<Long> keySet();

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