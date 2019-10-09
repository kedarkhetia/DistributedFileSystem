package edu.usfca.cs.dfs.dfsclient;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.StorageFeedback.Builder;


public class MessageDispatcher {
	
	public static volatile List<Messages.StorageNode> locations = new LinkedList<>();
	public static volatile Map<String, List<Messages.StoredLocationType>> chunkToLocation = new HashMap<>();
	public static volatile Map<String, Messages.DownloadFile> chunkToData = new HashMap<>();
	public static volatile Map<String, Messages.StorageFeedback> storageFeedback = new HashMap<>();
	public static volatile List<Messages.StorageNode> activeNodes = new LinkedList<>();
	public static volatile AtomicLong totalDiskspace = new AtomicLong(-1);
	public static volatile List<Messages.RequestPerNode> requestsServed = new LinkedList<>();
	

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
        				message.getStoredLocationResponse().getStoredLocationTypeList());
        		chunkToLocation.notifyAll();
        	}
        }
        else if (message.hasDownloadFile()) {
        	synchronized(chunkToData) {
        		chunkToData.put(message.getDownloadFile().getStoreChunk().getFileName(), 
        				message.getDownloadFile());
            	chunkToData.notifyAll();
            	//System.out.println(message.getDownloadFile().getStoreChunk().getFileName());
        	}
        }
        else if (message.hasStorageFeedback()) {
        	synchronized(storageFeedback) {
        		storageFeedback.put(message.getStorageFeedback().getFilename(), message.getStorageFeedback());
        		//System.out.println("Above statement is saying truth!");
        		storageFeedback.notifyAll();
        	}
        }
        else if (message.hasActiveNodes()) {
        	synchronized(activeNodes) {
        		activeNodes.addAll(message.getActiveNodes().getActiveNodesList());
        		activeNodes.notifyAll();
        	}
        }
        else if (message.hasTotalDiskSpace()) {
        	synchronized(totalDiskspace) {
        		totalDiskspace.lazySet(message.getTotalDiskSpace().getDiskSpace());
        		totalDiskspace.notifyAll();
        	}
        }
        else if (message.hasRequestServed()) {
        	synchronized(requestsServed) {
        		requestsServed.addAll(message.getRequestServed().getRequestsPerNodeList());
        		requestsServed.notifyAll();
        	}
        }
        return null;
    }
    
}
