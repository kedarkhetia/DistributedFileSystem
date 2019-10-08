package edu.usfca.cs.dfs.dfsclient;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.StorageFeedback.Builder;


public class MessageDispatcher {
	
	public static volatile List<Messages.StorageNode> locations = new LinkedList<>();
	public static volatile HashMap<String, List<Messages.StorageNode>> chunkToLocation = new HashMap<>();
	public static volatile HashMap<String, Messages.DownloadFile> chunkToData = new HashMap<>();
	public static volatile HashMap<String, Messages.StorageFeedback> storageFeedback = new HashMap<>(); 
	

    public static Messages.ProtoMessage dispatch(Messages.Client message) throws InvalidMessageException {
        if (message.hasStorageLocationResponse()) {
        	synchronized(locations) {
        		locations.addAll(message.getStorageLocationResponse().getLocationsList());
                locations.notifyAll();
        	}
        }
        else if (message.hasStoredLocationResponse()) {
        	synchronized(chunkToLocation) {
        		chunkToLocation.put(message.getStoredLocationResponse().getFilename(),
        				message.getStoredLocationResponse().getLocationsList());
        		chunkToLocation.notifyAll();
        	}
        }
        else if (message.hasDownloadFile()) {
        	synchronized(chunkToData) {
        		chunkToData.put(message.getDownloadFile().getStoreChunk().getFileName(), 
        				message.getDownloadFile());
            	chunkToData.notifyAll();
            	System.out.println(message.getDownloadFile().getStoreChunk().getFileName());
        	}
        }
        else if (message.hasStorageFeedback()) {
        	synchronized(storageFeedback) {
        		storageFeedback.put(message.getStorageFeedback().getFilename(), message.getStorageFeedback());
        		System.out.println("Above statement is saying truth!");
        		storageFeedback.notifyAll();
        	}
        }
        return null;
    }
    
}
