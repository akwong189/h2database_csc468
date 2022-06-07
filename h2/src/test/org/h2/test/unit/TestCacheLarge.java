/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

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
        testLIFOCache();

//        runMultipleBenchmark("Random", 10000, 5);
//        runMultipleBenchmark("LRU", 10000, 5);
//        runMultipleBenchmark("FIFO", 10000, 5);
//        runMultipleBenchmark("MRU", 10000, 5);
//        runMultipleBenchmark("Clock", 10000, 5);
        runMultipleBenchmark("LIFO", 10000, 5);
    }

    private static long getRealMemory(){
        StringUtils.clearCache();
        Value.clearCache();
        return Utils.getMemoryUsed();
    }

    private void runMultipleBenchmark(String cacheType, int size, int runs) {
        long times = 0;
        for (int i = 0; i < runs; i++) {
            times += runBenchmark(CacheLRU.getCache(this, cacheType, 400), size);
        }
        System.out.println(cacheType + " average run time is " + (times/runs) + "ms");
    }

    private long runBenchmark(Cache cache, int size) {
        Random randOperation = new Random(0);
        Random randIndex = new Random(1);
        Random randValue = new Random(2);
        ArrayList<Obj> objects = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            objects.add(new Obj(i, i, 1024));
        }

        // initialize all the first 16 objects in the cache
        for (int index = 0; index < 16; index++) {
            cache.put(objects.get(index));
        }

        long startTime = System.currentTimeMillis();
        for (long i = 0; i < 100000L; ++i) {
            int operation = randOperation.nextInt(4);
            switch (operation) {
                case 0: // perform a put
                    try {
                        cache.put(objects.get(randIndex.nextInt(size)));
                    } catch (Exception e){
                        break;
                    }
                    break;
                case 1: // perform a get
                    cache.get(randIndex.nextInt(size));
                    break;
                case 2: // perform an update
                    int updateIndex = randIndex.nextInt(size);
                    CacheObject updateObj = objects.get(updateIndex);
                    updateObj.setData(randValue.nextInt(100));
                    cache.update(updateIndex, updateObj);
                    break;
                case 3: // perform a remove
                    int removedIndex = randIndex.nextInt(size);
                    cache.remove(removedIndex);
                    break;
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);

        System.out.println(cache + " took " + duration + "ms to complete");
        return duration;
    }

    private void runStatements() throws SQLException {
        Connection conn;
        Statement stat;
        ResultSet rs;
        conn = getConnection("cache;CACHE_SIZE=16384");
        stat = conn.createStatement();
        // test DataOverflow
        stat.execute("create table test(id int)");
        stat.execute("set max_memory_undo 10000");

        ArrayList<Integer> inTest = new ArrayList<>(10000);

        String statement = "";
        Random rand = new Random();

        int sqlStatement = rand.nextInt(3);
        int toInsert = 0;
        int toDelete = 0;
        int toUpdate = 0;

        // Fill table
        for (int i = 0; i < 10000; i++) {
            toInsert = rand.nextInt(100000000);
            statement = "insert into test (id) values (" + Integer.toString(toInsert) + ");";
            inTest.add(toInsert);
        }

        for (int i = 0; i < 1000; i++) {
            sqlStatement = rand.nextInt(50);
            
            if (sqlStatement > 45 && inTest.size() > 0) {
                toInsert = rand.nextInt(inTest.size()); // Reusing variable name here, just a random index
                toDelete = inTest.get(toInsert);
                inTest.remove(toDelete);
                statement = "delete from test where id = " + Integer.toString(toDelete); // Can maybe cause error if deletes too many
            }
            else if (sqlStatement > 40) {
                toInsert = rand.nextInt(inTest.size()); // Reusing variable name here, just a random index
                toUpdate = inTest.get(toInsert);
                toInsert = rand.nextInt(100000000); 
                statement = "update test set id = " + Integer.toString(toInsert) + " where id = " + Integer.toString(toUpdate);
                
                for (int j = 0; j < inTest.size(); j++) { // Change array
                    if (inTest.get(j) == toUpdate) {
                        inTest.set(j, toInsert);
                    }
                }

            }
            else {
                toInsert = rand.nextInt(100000000);
                statement = "insert into test (id) values (" + Integer.toString(toInsert) + ");";
                inTest.add(toInsert);
            }
            stat.execute(statement);
        }
    }

    private void testCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "LRU", 16);
        for (int i = 0; i < 20; i++) {
            c.put(new Obj(i, i, 1024));
        }
        assertEquals("flush 0 flush 1 flush 2 flush 3 ", out);
    }

    private void testRandomCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "Random", 16);
        for (int i = 0; i < 20; i++) {
            c.put(new Obj(i, i));
        }
    }

    private void testLargerRandomCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "Random", 16);
        for (int i = 0; i < 100; i++) {
            c.put(new Obj(i, i));
        }
        c.put(new Obj(100, 100, 2048));
        System.out.println(out);
    }

    private void testMRUCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "MRU", 16);
        for (int i = 0; i < 20; i++) {
            c.put(new Obj(i, i, 1024));
        }
        assertEquals("flush 15 flush 16 flush 17 flush 18 ", out);
    }

    private void testMRUCacheWithGet() {
        out = "";
        Cache c = new CacheMRU(this, 16);
        for (int i = 0; i < 14; i++) {
            c.put(new Obj(i, i, 1024));
        }

        for (int i = 0; i < 5; i++) {
            assertNotNull(c.get(i));
            c.put(new Obj(i+14, i, 1024));
        }

        assertEquals("flush 2 flush 3 flush 4 ", out);
    }

    private void testClockCache() {
        out = "";
        Cache c = new CacheClock(this, 16);
        for (int i = 0; i < 30; i++) {
            c.put(new Obj(i, i, 128));
        }
        assertEquals("flush 0 1 2 3 4 5 6 7 ", out);
    }

    private void testLIFOCache() {
        out = "";
        Cache c = new CacheLIFO(this, 16);
        for (int i = 0; i < 30; i++) {
            c.put(new Obj(i, i, 128));
        }
        assertEquals("flush 19 20 21 22 23 24 25 26 27 ", out);
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