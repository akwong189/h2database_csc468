package org.h2.mvstore.cache;

public class RandomSegment extends SegmentParent {

    /**
     * The bit mask that is applied to the key hash code to get the index in
     * the map array. The mask is the length of the array minus one.
     */
    private final int mask;

    RandomSegment(long maxMemory, int len) {
        super(maxMemory, len);

        @SuppressWarnings("unchecked")
        Entry<V>[] e = new Entry[len];
        entries = e;

    }

    RandomSegment(RandomSegment<V> old, int len) {
        

    }


    /**
     * Calculate the new number of hash table buckets if the internal map
     * should be re-sized.
     *
     * @return 0 if no resizing is needed, or the new length
     */

     // This function could be moved to parent
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

    // This function could be moved to parent, lirs also uses
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
        }
        mapSize++;
        // added entries are always added to the stack
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
    synchronized V remove(long key, int hash) {
        return null;
    }

    private void evict() {
        do {
            evictBlock();
        } while (usedMemory > maxMemory);
    }

    private void evictBlock() {
        // pick a random block
    }

    /**
     * Try to find an entry in the map.
     *
     * @param key the key
     * @param hash the hash
     * @return the entry (might be a non-resident)
     */
    Entry<V> find(long key, int hash);

    /**
     * Get the list of keys. This method allows to read the internal state
     * of the cache.
     *
     * @param cold if true, only keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key list
     */
    List<Long> keys(boolean cold, boolean nonResident);

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    Set<Long> keySet();
    
}
