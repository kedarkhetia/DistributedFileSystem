package edu.usfca.cs.dfs.utils;

public class Constants {

    // Controller Server
    public static String CONTROLLER_HOSTNAME = "localhost";
    public static int CONTROLLER_PORT = 7777;

    // Storage Server
    public static String STORAGE_HOSTNAME = "localhost";
    public static int STORAGE_PORT = 7778;

    // Number of Replicas
    public static int REPLICAS = 1;
    
    // Thread Pools for Client
    public static int NUMBER_OF_THREADS = 3;
    
    // Number of ChunkSize
    public static int CHUNK_SIZE = 104850;
}
