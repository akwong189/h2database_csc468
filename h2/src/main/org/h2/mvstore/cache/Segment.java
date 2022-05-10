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
public class Segment<V> extends SegmentParent<V>  {

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


     // The queue variables can be added in the lirs specific version of segment
    Segment(long maxMemory, int stackMoveDistance, int len,
            int nonResidentQueueSize, int nonResidentQueueSizeHigh) {
        super(maxMemory, len);

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
    }

    /**
     * Create a new cache segment from an existing one.
     * The caller must synchronize on the old segment, to avoid
     * concurrent modifications.
     *
     * @param old the old segment
     * @param len the number of hash table buckets (must be a power of 2)
     */

     // Stack and queue can be overriden in other versions
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
            Entry<V> e = find(s.key, getHash(s.key));
            if (e == null) {
                e = new Entry<>(s);
                addToMap(e);
            }
            addToQueue(queue, e);
            s = s.queuePrev;
        }
        s = old.queue2.queuePrev;
        while (s != old.queue2) {
            Entry<V> e = find(s.key, getHash(s.key));
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
    private void access(Entry<V> e) {
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


    // LIRS cache specific, already private
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
    // LIRS cache specific
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
            int hash = getHash(e.key);
            remove(e.key, hash);
        }
    }
    // LIRS cache specific
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
    // LIRS cache specific
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

    // LIRS cache specific
    private void addToStack(Entry<V> e) {
        e.stackPrev = stack;
        e.stackNext = stack.stackNext;
        e.stackNext.stackPrev = e;
        stack.stackNext = e;
        stackSize++;
        e.topMove = stackMoveCounter++;
    }

    // LIRS cache specific
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