package edu.usfca.cs.dfs.storage;

import edu.usfca.cs.dfs.messages.Messages;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import edu.usfca.cs.dfs.clients.Client;

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
    		StorageHandlers.clearStoragePath(new File(StorageHandlers.STORAGE_PATH));
    		startStorageServer(argsMap.get(CONTORLLER_HOSTNAME), Integer.parseInt(argsMap.get(CONTROLLER_PORT)));
    	} 
    	else {
    		System.out.println("Failed to start server: Incomplete/Invalid arguments passed");
    	}
    }
    
    public static void startStorageServer(String controllerHost, int controllerPort) throws IOException, InterruptedException {
    	int port = 7774;
    	StorageServer s = new StorageServer(port);
        Client controllerClient = new Client(controllerHost, controllerPort);
        Messages.ProtoMessage msgWrapper = Messages.ProtoMessage
                .newBuilder()
                .setController(Messages.Controller
                        .newBuilder()
                        .setStorageNode(Messages.StorageNode
                                .newBuilder()
                                .setHost(InetAddress.getLocalHost().getHostAddress())
                                .setPort(port)
                                .build())
                        .build())
                .build();
        controllerClient.sendMessage(msgWrapper);
        Thread respThread = new Thread(new Runnable() {
        	@Override
        	public void run() {
        		synchronized(MessageDispatcher.REGISTER_FLAG) {
        			if(!MessageDispatcher.REGISTER_FLAG.get()) {
						try {
							MessageDispatcher.REGISTER_FLAG.wait(3000);
							controllerClient.disconnect();
							MessageDispatcher.REGISTER_FLAG.set(false);
							StorageHandlers.startHeartbeat(InetAddress.getLocalHost().getHostAddress(), port);
						} catch (InterruptedException | UnknownHostException e) {
							e.printStackTrace();
						}
        			}
        		}
        	}
        });
        respThread.run();
        s.start();
    }
    
}
