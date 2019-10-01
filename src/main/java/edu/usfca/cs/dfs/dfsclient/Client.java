package edu.usfca.cs.dfs.dfsclient;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Client {

    public static void main(String args[]) throws IOException, InterruptedException, ExecutionException {
        DistributedFileSystem dfs = new DistributedFileSystem();
        dfs.put("filename.txt");
    }
}
