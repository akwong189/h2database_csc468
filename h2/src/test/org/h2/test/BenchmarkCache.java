package org.h2.test;

import org.h2.message.Trace;
import org.h2.test.unit.TestCacheLarge;
import org.h2.util.Cache;
import org.h2.util.CacheLRU;
import org.h2.util.CacheObject;
import org.h2.util.CacheWriter;

import java.util.ArrayList;
import java.util.Random;

class TempCacheWriter implements CacheWriter {
    private String out;

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

class TempObj extends CacheObject {
    private int memory;

    TempObj(int pos, int data) {
        this(pos, data, 128);
    }

    TempObj(int pos, int data, int memory) {
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

public class BenchmarkCache {
    public static void main(String[] args) {
        BenchmarkCache benchmark = new BenchmarkCache();
        benchmark.runMultipleBenchmark("LRU", 10000, 5);
        benchmark.runMultipleBenchmark("MRU", 10000, 5);
        benchmark.runMultipleBenchmark("FIFO", 10000, 5);
        benchmark.runMultipleBenchmark("LIFO", 10000, 5);
        benchmark.runMultipleBenchmark("Random", 10000, 5);
        benchmark.runMultipleBenchmark("Clock", 10000, 5);
    }

    void runMultipleBenchmark(String cacheType, int size, int runs) {
        long times = 0;
        for (int i = 0; i < runs; i++) {
            System.gc();
            CacheWriter writer = new TempCacheWriter();
            times += runBenchmark(CacheLRU.getCache(writer, cacheType, 4000), size);
        }
        System.out.println(cacheType + " average run time is " + (times/runs) + "ms");
    }

    private long runBenchmark(Cache cache, int size) {
        Random randOperation = new Random(0);
        Random randIndex = new Random(1);
        Random randValue = new Random(2);
        ArrayList<TempObj> objects = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            objects.add(new TempObj(i, i, 1024));
        }

        // initialize all the first 16 objects in the cache
        for (int index = 0; index < 16; index++) {
            cache.put(objects.get(index));
        }

        long startTime = System.currentTimeMillis();
        for (long i = 0; i < 750000L; ++i) {
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

}
