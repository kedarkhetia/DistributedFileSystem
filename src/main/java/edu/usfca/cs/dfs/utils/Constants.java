package edu.usfca.cs.dfs.utils;

public class Constants {
	
	// Storage Server constants
    public static String REPLICA_PATH = "replica/";

    // Controller Server
    public static String CONTROLLER_HOSTNAME = "localhost";
    public static int CONTROLLER_PORT = 7777;

    // Storage Server

    // Number of Replicas
    public static int REPLICAS = 3;
    
    // Thread Pools for Client
    public static int NUMBER_OF_THREADS = 3;
    
    // Number of ChunkSize
    public static int CHUNK_SIZE = 1048570;
    
    
    
    // BloomFilter Parameter primary
    public static int PRIMARY_K = 3;
    public static int PRIMARY_M = 100;
    
    // BloomFilter Parameter replica
    public static int REPLICA_K = 3;
    public static int REPLICA_M = 200;
    
    // Response Timeouts
    public static int TIMEOUT = 3000;
}
