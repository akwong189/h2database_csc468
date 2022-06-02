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
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
//        test.config.traceTest = true;
        test.testFromMain();
    }

    @Override
    public void test() throws Exception {
        testMemoryUsage();
        testLargerRandomCache();
        testCacheDb(false);
        testCacheDb(true);
    }

    private void testMemoryUsage() throws SQLException {
        if (!config.traceTest) {
            return;
        }
        if (config.memory) {
            return;
        }
        deleteDb("cache");
        Connection conn;
        Statement stat;
        ResultSet rs;
        conn = getConnection("cache;CACHE_SIZE=16384");
        stat = conn.createStatement();
        // test DataOverflow
        stat.execute("create table test(id int primary key, data varchar)");
        stat.execute("set max_memory_undo 10000");
        conn.close();
        stat = null;
        conn = null;
        long before = getRealMemory();

        conn = getConnection("cache;CACHE_SIZE=16384;DB_CLOSE_ON_EXIT=FALSE");
        stat = conn.createStatement();

        //  -XX:+HeapDumpOnOutOfMemoryError

        stat.execute(
                "insert into test select x, random_uuid() || space(1) " +
                "from system_range(1, 10000)");

        // stat.execute("create index idx_test_n on test(data)");
        // stat.execute("select data from test where data >= ''");

        rs = stat.executeQuery(
                "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'info.CACHE_SIZE'");
        rs.next();
        int calculated = rs.getInt(1);
        rs = null;
        long afterInsert = getRealMemory();

        conn.close();
        stat = null;
        conn = null;
        long afterClose = getRealMemory();
        trace("Used memory: " + (afterInsert - afterClose) +
                " calculated cache size: " + calculated);
        trace("Before: " + before + " after: " + afterInsert +
                " after closing: " + afterClose);
    }

    private static long getRealMemory() {
        StringUtils.clearCache();
        Value.clearCache();
        return Utils.getMemoryUsed();
    }

    private void runStatements() {
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

        for (int i = 0; i < 99999; i++) {
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
            }
            stat.execute(statement);
        }
    }

    private void testCache() {
        out = "";
        Cache c = CacheLRU.getCache(this, "LRU", 16);
        for (int i = 0; i < 20; i++) {
            c.put(new Obj(i, i));
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

    private void testCacheDb(boolean lru) throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("cache");
        Connection conn = getConnection(
                "cache;CACHE_TYPE=" + (lru ? "LRU" : "SOFT_LRU"));
        Statement stat = conn.createStatement();
        stat.execute("SET CACHE_SIZE 1024");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE TABLE MAIN(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        PreparedStatement prep2 = conn.prepareStatement(
                "INSERT INTO MAIN VALUES(?, ?)");
        int max = 10000;
        for (int i = 0; i < max; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Hello " + i);
            prep.execute();
            prep2.setInt(1, i);
            prep2.setString(2, "World " + i);
            prep2.execute();
        }
        conn.close();
        conn = getConnection("cache");
        stat = conn.createStatement();
        stat.execute("SET CACHE_SIZE 1024");
        Random random = new Random(1);
        for (int i = 0; i < 100; i++) {
            stat.executeQuery("SELECT * FROM MAIN WHERE ID BETWEEN 40 AND 50");
            stat.executeQuery("SELECT * FROM MAIN WHERE ID = " + random.nextInt(max));
            if ((i % 10) == 0) {
                stat.executeQuery("SELECT * FROM TEST");
            }
        }
        conn.close();
        deleteDb("cache");
    }
}