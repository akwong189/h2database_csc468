package org.h2.mvstore.cache;


/**`
 * The cache configuration.
 */
public class Config {

    /**
     *  The maximum memory to use (1 or larger).
     */
    public long maxMemory = 1;

    /**
     * The number of cache segments (must be a power of 2).
     */
    public int segmentCount = 16;


    // These last three items are lirs specific

    /**
     * How many other item are to be moved to the top of the stack before
     * the current item is moved.
     */
    public int stackMoveDistance = 32;

    /**
     * Low water mark for the number of entries in the non-resident queue,
     * as a factor of the number of all other entries in the map.
     */
    public final int nonResidentQueueSize = 3;

    /**
     * High watermark for the number of entries in the non-resident queue,
     * as a factor of the number of all other entries in the map
     */
    public final int nonResidentQueueSizeHigh = 12;
}
