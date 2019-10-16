package edu.usfca.cs.dfs.utils;

/**
 * Set of static configurations.
 * @author kedarkhetia
 *
 */
public class Constants {
	
	// Storage Server constants
    public static final String REPLICA_PATH = "replica";
    public static final String COMPRESSED_PATH = "compressed";
    public static final String CHECKSUM_PATH = "checksum";
    public static final String CHECKSUM_SUFFIX = "_checksum";
    public static final int COMPRESS_LEVEL = 3;
    
    // Thread Pools for Client
    public static final int NUMBER_OF_THREADS = 5;
    
    // Number of ChunkSize
    public static final int CHUNK_SIZE_BYTES = 1000000; 
    //public static int CHUNK_SIZE = 3;
    
    // Heartbeat time difference
    public static final int HEARTBEAT_INTERVAL = 5000;
    public static final int HEARTBEAT_CAP = 15000;
}
