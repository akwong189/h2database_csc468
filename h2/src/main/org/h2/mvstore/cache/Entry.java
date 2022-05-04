package org.h2.mvstore.cache;

import java.lang.ref.WeakReference;

/**
 * A cache entry. Each entry is either hot (low inter-reference recency;
 * LIR), cold (high inter-reference recency; HIR), or non-resident-cold. Hot
 * entries are in the stack only. Cold entries are in the queue, and may be
 * in the stack. Non-resident-cold entries have their value set to null and
 * are in the stack and in the non-resident queue.
 *
 * @param <V> the value type
 */
public class Entry<V> {

    /**
     * The key.
     */
    final long key;

    /**
     * The value. Set to null for non-resident-cold entries.
     */
    V value;

    /**
     * Weak reference to the value. Set to null for resident entries.
     */
    WeakReference<V> reference;

    /**
     * The estimated memory used.
     */
    final int memory;

    /**
     * When the item was last moved to the top of the stack.
     */
    int topMove;

    /**
     * The next entry in the stack.
     */
    Entry<V> stackNext;

    /**
     * The previous entry in the stack.
     */
    Entry<V> stackPrev;

    /**
     * The next entry in the queue (either the resident queue or the
     * non-resident queue).
     */
    Entry<V> queueNext;

    /**
     * The previous entry in the queue.
     */
    Entry<V> queuePrev;

    /**
     * The next entry in the map (the chained entry).
     */
    Entry<V> mapNext;


    // Only these 2 functions are used by all, but others can remain if we want to just have 1 entry file
    Entry() {
        this(0L, null, 0);
    }

    Entry(long key, V value, int memory) {
        this.key = key;
        this.memory = memory;
        this.value = value;
    }

    Entry(Entry<V> old) {
        this(old.key, old.value, old.memory);
        this.reference = old.reference;
        this.topMove = old.topMove;
    }

    /**
     * Whether this entry is hot. Cold entries are in one of the two queues.
     *
     * @return whether the entry is hot
     */
    boolean isHot() {
        return queueNext == null;
    }

    V getValue() {
        return value == null ? reference.get() : value;
    }

    int getMemory() {
        return value == null ? 0 : memory;
    }
}