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

/**
 * A cache segment
 *
 * @param <V> the value type
 */
class Segment<V> extends SegmentParent<V> {
    /**
     * The size of the LIRS queue for resident cold entries.
     */
    int queueSize;

    /**
     * The size of the LIRS queue for non-resident cold entries.
     */
    int queue2Size;

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
    // private long maxMemory;

    /**
     * The bit mask that is applied to the key hash code to get the index in
     * the map array. The mask is the length of the array minus one.
     */
    // private final int mask;

    /**
     * Low watermark for the number of entries in the non-resident queue,
     * as a factor of the number of entries in the map.
     */
    private final int nonResidentQueueSize;

    /**
     * High watermark for the number of entries in the non-resident queue,
     * as a factor of the number of entries in the map.
     */
    private final int nonResidentQueueSizeHigh;

    /**
     * The stack of recently referenced elements. This includes all hot
     * entries, and the recently referenced cold entries. Resident cold
     * entries that were not recently referenced, as well as non-resident
     * cold entries, are not in the stack.
     * <p>
     * There is always at least one entry: the head entry.
     */
    private final Entry<V> stack;

    /**
     * The number of entries in the stack.
     */
    private int stackSize;

    /**
     * The queue of resident cold entries.
     * <p>
     * There is always at least one entry: the head entry.
     */
    private final Entry<V> queue;

    /**
     * The queue of non-resident cold entries.
     * <p>
     * There is always at least one entry: the head entry.
     */
    private final Entry<V> queue2;

    /**
     * The number of times any item was moved to the top of the stack.
     */
    private int stackMoveCounter;

    /**
     * Create a new cache segment.
     *  @param maxMemory the maximum memory to use
     * @param stackMoveDistance the number of other entries to be moved to
     *        the top of the stack before moving an entry to the top
     * @param len the number of hash table buckets (must be a power of 2)
     * @param nonResidentQueueSize the non-resident queue size low watermark factor
     * @param nonResidentQueueSizeHigh  the non-resident queue size high watermark factor
     */
    Segment(long maxMemory, int stackMoveDistance, int len,
            int nonResidentQueueSize, int nonResidentQueueSizeHigh) {
        super(maxMemory, len);
        // setMaxMemory(maxMemory);
        this.stackMoveDistance = stackMoveDistance;
        this.nonResidentQueueSize = nonResidentQueueSize;
        this.nonResidentQueueSizeHigh = nonResidentQueueSizeHigh;

        // the bit mask has all bits set
        mask = len - 1;

        // initialize the stack and queue heads
        stack = new Entry<>();
        stack.stackPrev = stack.stackNext = stack;
        queue = new Entry<>();
        queue.queuePrev = queue.queueNext = queue;
        queue2 = new Entry<>();
        queue2.queuePrev = queue2.queueNext = queue2;

        @SuppressWarnings("unchecked")
        Entry<V>[] e = new Entry[len];
        entries = e;
    }

    /**
     * Create a new cache segment from an existing one.
     * The caller must synchronize on the old segment, to avoid
     * concurrent modifications.
     *
     * @param old the old segment
     * @param len the number of hash table buckets (must be a power of 2)
     */
    Segment(Segment<V> old, int len) {
        this(old.maxMemory, old.stackMoveDistance, len,
                old.nonResidentQueueSize, old.nonResidentQueueSizeHigh);
        hits = old.hits;
        misses = old.misses;
        Entry<V> s = old.stack.stackPrev;
        while (s != old.stack) {
            Entry<V> e = new Entry<>(s);
            addToMap(e);
            addToStack(e);
            s = s.stackPrev;
        }
        s = old.queue.queuePrev;
        while (s != old.queue) {
            Entry<V> e = find(s.key, SegmentParent.getHash(s.key));
            if (e == null) {
                e = new Entry<>(s);
                addToMap(e);
            }
            addToQueue(queue, e);
            s = s.queuePrev;
        }
        s = old.queue2.queuePrev;
        while (s != old.queue2) {
            Entry<V> e = find(s.key, SegmentParent.getHash(s.key));
            if (e == null) {
                e = new Entry<>(s);
                addToMap(e);
            }
            addToQueue(queue2, e);
            s = s.queuePrev;
        }
    }

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

    void addToMap(Entry<V> e) {
        int index = SegmentParent.getHash(e.key) & mask;
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
    void access(Entry<V> e) {
        if (e.isHot()) {
            if (e != stack.stackNext && e.stackNext != null) {
                if (stackMoveCounter - e.topMove > stackMoveDistance) {
                    // move a hot entry to the top of the stack
                    // unless it is already there
                    boolean wasEnd = e == stack.stackPrev;
                    removeFromStack(e);
                    if (wasEnd) {
                        // if moving the last entry, the last entry
                        // could now be cold, which is not allowed
                        pruneStack();
                    }
                    addToStack(e);
                }
            }
        } else {
            V v = e.getValue();
            if (v != null) {
                removeFromQueue(e);
                if (e.reference != null) {
                    e.value = v;
                    e.reference = null;
                    usedMemory += e.memory;
                }
                if (e.stackNext != null) {
                    // resident, or even non-resident (weak value reference),
                    // cold entries become hot if they are on the stack
                    removeFromStack(e);
                    // which means a hot entry needs to become cold
                    // (this entry is cold, that means there is at least one
                    // more entry in the stack, which must be hot)
                    convertOldestHotToCold();
                } else {
                    // cold entries that are not on the stack
                    // move to the front of the queue
                    addToQueue(queue, e);
                }
                // in any case, the cold entry is moved to the top of the stack
                addToStack(e);
                // but if newly promoted cold/non-resident is the only entry on a stack now
                // that means last one is cold, need to prune
                pruneStack();
            }
        }
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
        if (e.stackNext != null) {
            removeFromStack(e);
        }
        if (e.isHot()) {
            // when removing a hot entry, the newest cold entry gets hot,
            // so the number of hot entries does not change
            e = queue.queueNext;
            if (e != queue) {
                removeFromQueue(e);
                if (e.stackNext == null) {
                    addToStackBottom(e);
                }
            }
            pruneStack();
        } else {
            removeFromQueue(e);
        }
        return old;
    }

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

    private void evictBlock() {
        // ensure there are not too many hot entries: right shift of 5 is
        // division by 32, that means if there are only 1/32 (3.125%) or
        // less cold entries, a hot entry needs to become cold
        while (queueSize <= ((mapSize - queue2Size) >>> 5) && stackSize > 0) {
            convertOldestHotToCold();
        }
        // the oldest resident cold entries become non-resident
        while (usedMemory > maxMemory && queueSize > 0) {
            Entry<V> e = queue.queuePrev;
            usedMemory -= e.memory;
            removeFromQueue(e);
            e.reference = new WeakReference<>(e.value);
            e.value = null;
            addToQueue(queue2, e);
            // the size of the non-resident-cold entries needs to be limited
            trimNonResidentQueue();
        }
    }

    void trimNonResidentQueue() {
        int residentCount = mapSize - queue2Size;
        int maxQueue2SizeHigh = nonResidentQueueSizeHigh * residentCount;
        int maxQueue2Size = nonResidentQueueSize * residentCount;
        while (queue2Size > maxQueue2Size) {
            Entry<V> e = queue2.queuePrev;
            if (queue2Size <= maxQueue2SizeHigh) {
                WeakReference<V> reference = e.reference;
                if (reference != null && reference.get() != null) {
                    break;  // stop trimming if entry holds a value
                }
            }
            int hash = SegmentParent.getHash(e.key);
            remove(e.key, hash);
        }
    }

    private void convertOldestHotToCold() {
        // the last entry of the stack is known to be hot
        Entry<V> last = stack.stackPrev;
        if (last == stack) {
            // never remove the stack head itself (this would mean the
            // internal structure of the cache is corrupt)
            throw new IllegalStateException();
        }
        // remove from stack - which is done anyway in the stack pruning,
        // but we can do it here as well
        removeFromStack(last);
        // adding an entry to the queue will make it cold
        addToQueue(queue, last);
        pruneStack();
    }

    /**
     * Ensure the last entry of the stack is cold.
     */
    private void pruneStack() {
        while (true) {
            Entry<V> last = stack.stackPrev;
            // must stop at a hot entry or the stack head,
            // but the stack head itself is also hot, so we
            // don't have to test it
            if (last.isHot()) {
                break;
            }
            // the cold entry is still in the queue
            removeFromStack(last);
        }
    }

    // /**
    //  * Try to find an entry in the map.
    //  *
    //  * @param key the key
    //  * @param hash the hash
    //  * @return the entry (might be a non-resident)
    //  */
    // Entry<V> find(long key, int hash) {
    //     int index = hash & mask;
    //     Entry<V> e = entries[index];
    //     while (e != null && e.key != key) {
    //         e = e.mapNext;
    //     }
    //     return e;
    // }

    private void addToStack(Entry<V> e) {
        e.stackPrev = stack;
        e.stackNext = stack.stackNext;
        e.stackNext.stackPrev = e;
        stack.stackNext = e;
        stackSize++;
        e.topMove = stackMoveCounter++;
    }

    private void addToStackBottom(Entry<V> e) {
        e.stackNext = stack;
        e.stackPrev = stack.stackPrev;
        e.stackPrev.stackNext = e;
        stack.stackPrev = e;
        stackSize++;
    }

    /**
     * Remove the entry from the stack. The head itself must not be removed.
     *
     * @param e the entry
     */
    private void removeFromStack(Entry<V> e) {
        e.stackPrev.stackNext = e.stackNext;
        e.stackNext.stackPrev = e.stackPrev;
        e.stackPrev = e.stackNext = null;
        stackSize--;
    }

    private void addToQueue(Entry<V> q, Entry<V> e) {
        e.queuePrev = q;
        e.queueNext = q.queueNext;
        e.queueNext.queuePrev = e;
        q.queueNext = e;
        if (e.value != null) {
            queueSize++;
        } else {
            queue2Size++;
        }
    }

    private void removeFromQueue(Entry<V> e) {
        e.queuePrev.queueNext = e.queueNext;
        e.queueNext.queuePrev = e.queuePrev;
        e.queuePrev = e.queueNext = null;
        if (e.value != null) {
            queueSize--;
        } else {
            queue2Size--;
        }
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
        if (cold) {
            Entry<V> start = nonResident ? queue2 : queue;
            for (Entry<V> e = start.queueNext; e != start;
                    e = e.queueNext) {
                keys.add(e.key);
            }
        } else {
            for (Entry<V> e = stack.stackNext; e != stack;
                    e = e.stackNext) {
                keys.add(e.key);
            }
        }
        return keys;
    }

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    synchronized Set<Long> keySet() {
        HashSet<Long> set = new HashSet<>();
        for (Entry<V> e = stack.stackNext; e != stack; e = e.stackNext) {
            set.add(e.key);
        }
        for (Entry<V> e = queue.queueNext; e != queue; e = e.queueNext) {
            set.add(e.key);
        }
        return set;
    }
}
