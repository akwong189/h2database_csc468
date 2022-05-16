/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.cache;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.h2.mvstore.DataUtils;

class SegmentRandom<V> extends SegmentParent<V> {

    long hits;
    long misses;

    // long maxMemory;
    // int mask;
    // int mapSize;
    // final Entry<V>[] entries;
    // long usedMemory;
    // All are inheirited vars


    SegmentRandom(long maxMemory, int len) {
        super(maxMemory, len);
    }

    SegmentRandom(Segment<V> old, int len) {
        hits = old.hits;
        misses = old.misses; 
    }

    // abstract void addToMap(Entry<V> e);

    /**
    * Access an item, might only be LIRS
    *
    * @param e entry to record access for
    */
    // void access(Entry<V> e)

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
    synchronized V put(long key, int hash, V value, int memory) {// {return null;}
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
        // if (usedMemory > maxMemory) {
        //     // Pick a random entry, remove
        // }
        mapSize++;
        return old;
    }

    /**
     * Remove an entry
     *
     * @param key the key (may not be null)
     * @param hash the hash
     * @return the old value, or null if there was no resident entry
     */
    synchronized V remove(long key, int hash) {
        int index = hash & mask;
        Entry<V> e = entries[index];
        if (e == null) {
            return null;
        }
        if (e.key == key) {
            entries[index] = e.mapNext;
        } else {
            Entry<V> last;
            do {
                last = e;
                e = e.mapNext;
                if (e == null) {
                    return null;
                }
            } while (e.key != key);
            last.mapNext = e.mapNext;
        }
        V old = e.getValue();
        mapSize--;
        usedMemory -= e.getMemory();

        return old;
    }

    /**
     * Get the list of keys. This method allows to read the internal state
     * of the cache.
     *
     * @param cold if true, only keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key list
     */
    synchronized List<Long> keys(boolean cold, boolean nonResident) {
        ArrayList<Long> keys = new ArrayList<>();

        return keys;
    }

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    synchronized Set<Long> keySet() { 
        HashSet<Long> set = new HashSet<>();
        
        return set;
    }
}