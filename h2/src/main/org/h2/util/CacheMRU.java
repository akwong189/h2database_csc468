/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;

/**
 * A cache implementation based on the last recently used (LRU) algorithm.
 */
public class CacheMRU implements Cache {

	static final String TYPE_NAME = "MRU";

	private final CacheWriter writer;

	private final CacheObject head = new CacheHead();
	private final int mask;
	private CacheObject[] values;
	private int recordCount;

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

	public CacheMRU(CacheWriter writer, int maxMemoryKb) {
		this.writer = writer;
		this.setMaxMemory(maxMemoryKb);
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
	public void clear() {
		head.cacheNext = head.cachePrevious = head;
		// first set to null - avoiding out of memory
		values = null;
		values = new CacheObject[len];
		recordCount = 0;
		memory = len * (long)Constants.MEMORY_POINTER;
	}

	@Override
	public void put(CacheObject rec) {
		if (SysProperties.CHECK) {
			int pos = rec.getPos();
			CacheObject old = find(pos);
			if (old != null) {
				throw DbException.getInternalError("try to add a record twice at pos " + pos);
			}
		}
		int index = rec.getPos() & mask;
		rec.cacheChained = values[index];
		values[index] = rec;
		recordCount++;
		memory += rec.getMemory();
		removeMostRecentlyUsedIfRequired();
		addToFront(rec);
	}

	@Override
	public CacheObject update(int pos, CacheObject rec) {
		CacheObject old = find(pos);
		if (old == null) {
			put(rec);
		} else {
			if (old != rec) {
				throw DbException.getInternalError("old!=record pos:" + pos + " old:" + old + " new:" + rec);
			}
			removeFromLinkedList(rec);
			addToFront(rec);
		}
		return old;
	}

	private void removeMostRecentlyUsedIfRequired() {
		// a small method, to allow inlining
		if (memory >= maxMemory) {
			removeMostRecentlyUsed();
		}
	}

	private void removeMostRecentlyUsed() {
		int i = 0;
		ArrayList<CacheObject> changed = new ArrayList<>();
		long mem = memory;
		int rc = recordCount;
		boolean flushed = false;
		CacheObject previous = head.cachePrevious;

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

			CacheObject check = previous; // load the next cache block
			previous = check.cachePrevious;   // prepare the next cache check
			i++;                      // increase i
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
			if (check == head) {
				throw DbException.getInternalError("try to remove head");
			}
			// we are not allowed to remove it if the log is not yet written
			// (because we need to log before writing the data)
			// also, can't write it if the record is pinned
			if (!check.canRemove()) {
				removeFromLinkedList(check);
				addToFront(check);
				continue;
			}

			rc--; // decrement record count
			mem -= check.getMemory(); // decrement memory

			if (check.isChanged()) {
				changed.add(check);
//				System.out.printf("Removed %d\n", check.getData());
			} else {
				remove(check.getPos());
			}
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

	private void addToFront(CacheObject rec) {
		if (rec == head) {
			throw DbException.getInternalError("try to move head");
		}
//		System.out.printf("Added %d\n", rec.getData());
		rec.cacheNext = head;
		rec.cachePrevious = head.cachePrevious;
		rec.cachePrevious.cacheNext = rec;
		head.cachePrevious = rec;
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
	public CacheObject get(int pos) {
		CacheObject rec = find(pos);
		if (rec != null) {
//			System.out.printf("Retrieved %d\n", rec.getData());
			removeFromLinkedList(rec);
			addToFront(rec);
		}
		return rec;
	}
	@Override
	public ArrayList<CacheObject> getAllChanged() {
		// if(Database.CHECK) {
		// testConsistency();
		// }
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
	public void setMaxMemory(int maxKb) {
		long newSize = maxKb * 1024L / 4;
		maxMemory = newSize < 0 ? 0 : newSize;
		// can not resize, otherwise existing records are lost
		// resize(maxSize);
		removeMostRecentlyUsedIfRequired();
	}

	@Override
	public int getMaxMemory() {
		return (int) (maxMemory * 4L / 1024);
	}

	@Override
	public int getMemory() {
		// CacheObject rec = head.cacheNext;
		// while (rec != head) {
		// System.out.println(rec.getMemory() + " " +
		// MemoryFootprint.getObjectSize(rec) + " " + rec);
		// rec = rec.cacheNext;
		// }
		return (int) (memory * 4L / 1024);
	}

	public String toString() {
		return TYPE_NAME;
	}
}
