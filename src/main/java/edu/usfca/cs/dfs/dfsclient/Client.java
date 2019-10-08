package edu.usfca.cs.dfs.dfsclient;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Client {

    public static void main(String args[]) throws IOException, InterruptedException, ExecutionException {
        DistributedFileSystem dfs = new DistributedFileSystem();
        boolean flag  = dfs.put("LinkStores.pdf");
        if(flag) {
        	System.out.println("Send data succeccfully");
        }
        else {
        	System.out.println("Send data failed");
        }
        flag = dfs.get("./bigdata/retrived/", "LinkStores.pdf");
        if(flag) {
        	System.out.println("retrived data succeccfully");
        }
        else {
        	System.out.println("retrived data failed");
        }
        System.out.println("ActiveNodes: " + dfs.getActiveNodes());
        System.out.println("Requests Processed: " + dfs.getRequestsServed());
        System.out.println("Available Space: " + dfs.getTotalDiskspace() / 1073741824 + " MB");
        dfs.close();
    }
}
