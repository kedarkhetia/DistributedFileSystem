package edu.usfca.cs.dfs.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;


public class Storage {

    private static Map<String, String> argsMap = new HashMap<>();
    private static String STORAGE_KEY = "-storagePath";
    private static String CONTORLLER_HOSTNAME = "-controllerHostname";
    private static String CONTROLLER_PORT = "-controllerPort";

    public static void main(String args[]) throws InterruptedException, IOException {
    	for(int i=0; i<args.length; i+=2) {
    		argsMap.put(args[i], args[i+1]);
    	}
    	if(argsMap.size() == 3 && argsMap.containsKey(STORAGE_KEY) && 
    			argsMap.containsKey(CONTORLLER_HOSTNAME) && argsMap.containsKey(CONTROLLER_PORT)) {
    		StorageHandlers.STORAGE_PATH = argsMap.get(STORAGE_KEY);
    		StorageHandlers.clearStoragePath(new File(StorageHandlers.STORAGE_PATH), false);
    		startStorageServer(argsMap.get(CONTORLLER_HOSTNAME), Integer.parseInt(argsMap.get(CONTROLLER_PORT)));
    	} 
    	else {
    		System.out.println("Failed to start server: Incomplete/Invalid arguments passed");
    	}
    }
    
    public static void startStorageServer(String controllerHost, int controllerPort) throws IOException, InterruptedException {
    	int port = 7775;
    	StorageServer s = new StorageServer(port);
    	StorageHandlers.startHeartbeat(InetAddress.getLocalHost().getHostAddress(), port);
        s.start();
    }
    
}
