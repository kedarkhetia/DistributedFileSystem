package edu.usfca.cs.dfs.utils;

public class Constants {
	
	// Storage Server constants
    public static String REPLICA_PATH = "replica";
    public static String COMPRESSED_PATH = "compressed";
    public static String CHECKSUM_PATH = "checksum";
    public static String CHECKSUM_SUFFIX = "_checksum";
    public static int COMPRESS_LEVEL = 3;
    
    // Thread Pools for Client
    public static int NUMBER_OF_THREADS = 5;
    
    // Number of ChunkSize
    public static int CHUNK_SIZE_BYTES = 1000000; 
    //public static int CHUNK_SIZE = 3;
    
    // Heartbeat time difference
    public static int HEARTBEAT_INTERVAL = 5000;
    public static int HEARTBEAT_CAP = 15000;
}
