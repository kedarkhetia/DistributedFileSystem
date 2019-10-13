package edu.usfca.cs.dfs.dfsclient;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import edu.usfca.cs.dfs.messages.Messages;

public class Client {
	private static String RETRIVE_PATH = "./bigdata/retrived/";

    public static void main(String args[]) throws IOException, InterruptedException, ExecutionException {
        DistributedFileSystem dfs = new DistributedFileSystem();
        userInterface(dfs);
    }
    
    public static void userInterface(DistributedFileSystem dfs) throws IOException, InterruptedException, ExecutionException {
    	System.out.println("Welcome to Distributed File System");
    	System.out.println("Note: To put/get any data from DFS your file must be present in the Classpath directory.");
    	@SuppressWarnings("resource")
		Scanner scn = new Scanner(System.in);
    	while(true) {
    		System.out.println();
    		System.out.println("Enter your choice from below supported commands: ");
    		System.out.println("1) Put Data");
    		System.out.println("2) Get Data");
    		System.out.println("3) Get List of Active Storage Nodes");
    		System.out.println("4) Get Total Available space in DFS");
    		System.out.println("5) Get Requests served by Each Node");
    		System.out.println("6) Exit");
    		System.out.print("Enter your choice: ");
    		int choice = scn.nextInt();
    		switch(choice) {
    		case 1:
    			System.out.print("Enter Filename: ");
    			String putFilename = scn.next();
    			File putfile = new File(putFilename);
    			if(!putfile.exists()) {
    				System.out.println("File with filename: " + putFilename + " Doesn't exist!");
    				break;
    			}
    			if(dfs.put(putFilename)) {
    				System.out.println("File successfully stored in DFS");
    			}
    			else {
    				System.out.println("Some error occured! Failed to store provided file in DFS");
    			}
    			break;
    		case 2:
    			System.out.print("Enter Filename: ");
    			String getfilename = scn.next();
    			if(dfs.get(RETRIVE_PATH, getfilename)) {
    				System.out.println("File successfully got data from DFS");
    			}
    			else {
    				System.out.println("Some error occured! Failed to get provided file name from DFS");
    			}
    			break;
    			
    		case 3:
    			List<Messages.StorageNode> activeNodes = dfs.getActiveNodes();
    			for(int i=0; i < activeNodes.size(); i++) {
    				System.out.println(i + ") " + activeNodes.get(i).getHost() + ":" + activeNodes.get(i).getPort());
    			}
    			break;
    		case 4:
    			System.out.println("Total Available Space: " + dfs.getTotalDiskspace() / 1073741824 + " GBs");
    			break;
    		case 5:
    			Map<Messages.StorageNode, Long> requestMap = dfs.getRequestsServed();
    			int count = 0;
    			for(Messages.StorageNode node : requestMap.keySet()) {
    				System.out.println(count + ") " + "Total Requests served by host: " 
    						+ node.getHost() + ":" + node.getPort() + " is " + requestMap.get(node));
    				count++;
    			}
    			break;
    		case 6: 
    			dfs.close();
    			return;
    		default:
    			System.out.println("Unidentified command, please see the selection below.");
    		}
    		
    	}
    }
}
