package edu.usfca.cs.dfs.dfsclient;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Client {

    public static void main(String args[]) throws IOException, InterruptedException, ExecutionException {
        DistributedFileSystem dfs = new DistributedFileSystem();
        boolean flag  = dfs.put("newFile.txt");
        if(flag) {
        	System.out.println("Send data succeccfully");
        }
        else {
        	System.out.println("Send data failed");
        }
        flag = dfs.get("./bigdata/retrived/", "newFile.txt");
        if(flag) {
        	System.out.println("retrived data succeccfully");
        }
        else {
        	System.out.println("retrived data failed");
        }
        dfs.close();
    }
}
