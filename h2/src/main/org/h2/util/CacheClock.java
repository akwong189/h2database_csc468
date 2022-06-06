package org.h2.util;

import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;

import java.util.ArrayList;
import java.util.Collections;

public class CacheClock implements Cache {

    static final String TYPE_NAME = "Clock";

    private final CacheWriter writer;

    private final CacheObject head = new CacheHead();
    private final int mask;
    private CacheObject[] values;
    private int recordCount;

    private CacheObject pointer;

    /**
     * The number of cache buckets.
     */
    private final int len;

    /**
     * The maximum memory, in words (4 bytes each).
     */
    private long maxMemory;

    /**
     * The current memory used in this cache, in words (4 bytes each).
     */
    private long memory;

    public CacheClock(CacheWriter writer, int maxMemoryKb) {
        this.writer = writer;
        this.setMaxMemory(maxMemoryKb);
        this.pointer = head;

        try {
            // Since setMaxMemory() ensures that maxMemory is >=0,
            // we don't have to worry about an underflow.
            long tmpLen = maxMemory / 64;
            if (tmpLen > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            this.len = MathUtils.nextPowerOf2((int) tmpLen);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("This much cache memory is not supported: " + maxMemoryKb + "kb", e);
        }
        this.mask = len - 1;
        clear();
    }

    @Override
    public ArrayList<CacheObject> getAllChanged() {
        ArrayList<CacheObject> list = new ArrayList<>();
        CacheObject rec = head.cacheNext;
        while (rec != head) {
            if (rec.isChanged()) {
                list.add(rec);
            }
            rec = rec.cacheNext;
        }
        return list;
    }

    @Override
    public void clear() {
        head.cacheNext = head.cachePrevious = head;
        // first set to null - avoiding out of memory
        values = null;
        values = new CacheObject[len];
        recordCount = 0;
        memory = len * (long) Constants.MEMORY_POINTER;
    }

    @Override
    public CacheObject get(int pos) {
        return find(pos);
    }

    @Override
    public void put(CacheObject r) {
        if (SysProperties.CHECK) {
            int pos = r.getPos();
            CacheObject old = find(pos);
            if (old != null) {
                throw DbException.getInternalError("try to add a record twice at pos " + pos);
            }
        }

        removeClockIfRequired();
        memory += r.getMemory();

        int index = r.getPos() & mask;
        r.cacheChained = values[index];
        values[index] = r;
        recordCount++;
        addToFront(r);
    }

    @Override
    public CacheObject update(int pos, CacheObject record) {
        CacheObject old = find(pos);
        if (old == null) {
            put(record);
        } else {
            if (old != record) {
                throw DbException.getInternalError("old!=record pos:" + pos + " old:" + old + " new:" + record);
            }
        }
        return old;
    }

    @Override
    public boolean remove(int pos) {
        int index = pos & mask;
        CacheObject rec = values[index];
        if (rec == null) {
            return false;
        }
        if (rec.getPos() == pos) {
            values[index] = rec.cacheChained;
        } else {
            CacheObject last;
            do {
                last = rec;
                rec = rec.cacheChained;
                if (rec == null) {
                    return false;
                }
            } while (rec.getPos() != pos);
            last.cacheChained = rec.cacheChained;
        }
        recordCount--;
        memory -= rec.getMemory();
        removeFromLinkedList(rec);
        if (SysProperties.CHECK) {
            rec.cacheChained = null;
            CacheObject o = find(pos);
            if (o != null) {
                throw DbException.getInternalError("not removed: " + o);
            }
        }
        return true;
    }

    @Override
    public CacheObject find(int pos) {
        CacheObject rec = values[pos & mask];
        while (rec != null && rec.getPos() != pos) {
            rec = rec.cacheChained;
        }
        return rec;
    }

    @Override
    public void setMaxMemory(int size) {
        long newSize = size * 1024L / 4;
        maxMemory = newSize < 0 ? 0 : newSize;
        removeClockIfRequired();
    }

    @Override
    public int getMaxMemory() {
        return (int) (maxMemory * 4L / 1024);
    }

    @Override
    public int getMemory() {
        return (int) (memory * 4L / 1024);
    }

    private void removeClockIfRequired() {
        if (memory >= maxMemory) {
            removeClock();
        }
    }

    private void removeClock() {
        int i = 0;
        ArrayList<CacheObject> changed = new ArrayList<>();
        long mem = memory;
        int rc = recordCount;
        boolean flushed = false;
        CacheObject check = pointer;

        while (true) {
            // edge checks
            if (rc <= Constants.CACHE_MIN_RECORDS) { // count has to be > min records
                break;
            }

            // if changed is empty
            if (changed.isEmpty()) {
                // and memory is not greater than max memory, break out
                if (mem <= maxMemory) {
                    break;
                }
            } else {
                // if memory * 3 is not greater than max memory * 3, break out
                if (mem * 4 <= maxMemory * 3) {
                    break;
                }
            }

            i++; // increase i
            if (i >= recordCount) {
                if (!flushed) {
                    writer.flushLog();
                    flushed = true;
                    i = 0;
                } else {
                    // can't remove any record, because the records can not be
                    // removed hopefully this does not happen frequently, but it
                    // can happen
                    writer.getTrace()
                            .info("cannot remove records, cache size too small? records:" +
                                    recordCount + " memory:" + memory);
                    break;
                }
            }

            // ignore head
            if (check == head) {
//                throw DbException.getInternalError("try to remove head");
                check = head.cacheNext;
                continue;
            }

            // check if it can be removed
            if (!check.canRemove()) {
                continue;
            }

            // check if the block has been read, if not ignore and set to read
            if (!check.beenRead()) {
                System.out.println(check + " has now been read");
                continue;
            }

            // ensure that the record has been read and can move to the next value
            if (changed.contains(check)) {
                continue;
            }

            rc--; // decrement record count
            mem -= check.getMemory(); // decrement memory
            if (check.isChanged()) {
                changed.add(check);
            } else {
                remove(check.getPos());
            }

            check = check.cacheNext;
        }

        // have values that are removed
        if (!changed.isEmpty()) {
            if (!flushed) { // logging
                writer.flushLog();
            }

            // sort changed and init vars
            Collections.sort(changed);
            long max = maxMemory;
            int size = changed.size();

            // log changed values
            try {
                // temporary disable size checking,
                // to avoid stack overflow
                maxMemory = Long.MAX_VALUE;
                for (i = 0; i < size; i++) {
                    CacheObject rec = changed.get(i);
                    writer.writeBack(rec);
                }
            } finally {
                maxMemory = max;
            }

            // remove the records that were changed
            for (i = 0; i < size; i++) {
                CacheObject rec = changed.get(i);
                remove(rec.getPos());
                if (rec.cacheNext != null) {
                    throw DbException.getInternalError();
                }
            }
        }
    }

    private void removeFromLinkedList(CacheObject rec) {
        if (rec == head) {
            throw DbException.getInternalError("try to remove head");
        }
        rec.cachePrevious.cacheNext = rec.cacheNext;
        rec.cacheNext.cachePrevious = rec.cachePrevious;
        // TODO cache: mystery: why is this required? needs more memory if we
        // don't do this
        rec.cacheNext = null;
        rec.cachePrevious = null;
    }

    private void addToFront(CacheObject rec) {
        if (rec == head) {
            throw DbException.getInternalError("try to move head");
        }
        rec.cacheNext = head;
        rec.cachePrevious = head.cachePrevious;
        rec.cachePrevious.cacheNext = rec;
        head.cachePrevious = rec;
    }
}