/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.message.Trace;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.*;
import org.h2.value.Value;

/**
 * Tests the cache.
 */
public class TestCacheLarge extends TestDb implements CacheWriter {

    private String out;

    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
//        test.config.traceTest = true;
        test.testFromMain();
    }

    @Override
    public void test() throws Exception {
        testCache();
        testRandomCache();
        testLargerRandomCache();
        testMRUCache();
        testMRUCacheWithGet();
        testClockCache();
    }

    private static long getRealMemory() {
        StringUtils.clearCache();
        Value.clearCache();
        return Utils.getMemoryUsed();
    }

    private void testCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "LRU", 16);
        for (int i = 0; i < 20; i++) {
            c.put(new TestCache.Obj(i, i, 1024));
        }
        assertEquals("flush 0 flush 1 flush 2 flush 3 ", out);
    }

    private void testRandomCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "Random", 16);
        for (int i = 0; i < 20; i++) {
            c.put(new TestCache.Obj(i, i));
        }
    }

    private void testLargerRandomCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "Random", 16);
        for (int i = 0; i < 100; i++) {
            c.put(new TestCache.Obj(i, i));
        }
        c.put(new TestCache.Obj(100, 100, 2048));
        System.out.println(out);
    }

    private void testMRUCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "MRU", 16);
        for (int i = 0; i < 20; i++) {
            c.put(new TestCache.Obj(i, i, 1024));
        }
        assertEquals("flush 15 flush 16 flush 17 flush 18 ", out);
    }

    private void testMRUCacheWithGet() {
        out = "";
        Cache c = new CacheMRU(this, 16);
        for (int i = 0; i < 14; i++) {
            c.put(new TestCache.Obj(i, i, 1024));
        }

        for (int i = 0; i < 5; i++) {
            assertNotNull(c.get(i));
            c.put(new TestCache.Obj(i+14, i, 1024));
        }

        assertEquals("flush 2 flush 3 flush 4 ", out);
    }

    private void testClockCache() {
        out = "";
        Cache c = new CacheClock(this, 16);
        for (int i = 0; i < 20; i++) {
            c.put(new TestCache.Obj(i, i, 1024));
        }
        assertEquals("flush 0 flush 1 flush 2 flush 3 ", out);
    }

    /**
     * A simple cache object
     */
    static class Obj extends CacheObject {
        private int memory;

        Obj(int pos, int data) {
            this(pos, data, 128);
        }

        Obj(int pos, int data, int memory) {
            setPos(pos);
            setData(data);
            this.memory = memory;
        }

        @Override
        public int getMemory() {
            return memory;
        }

        @Override
        public boolean canRemove() {
            return true;
        }

        @Override
        public boolean isChanged() {
            return true;
        }

        @Override
        public String toString() {
            return "[" + getPos() + ", " + getData() + "]";
        }

    }

    @Override
    public void flushLog() {
        out += "flush ";
    }

    @Override
    public Trace getTrace() {
        return null;
    }

    @Override
    public void writeBack(CacheObject entry) {
        out += entry.getPos() + " ";
    }

}