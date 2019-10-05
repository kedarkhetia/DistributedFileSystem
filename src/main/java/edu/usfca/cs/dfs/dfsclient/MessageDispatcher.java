package edu.usfca.cs.dfs.dfsclient;

import java.util.LinkedList;
import java.util.List;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.StorageNode;


public class MessageDispatcher {
	
	public static volatile List<Messages.StorageNode> locations = new LinkedList<>();

    public static Messages.ProtoMessage dispatch(Messages.Client message) throws InvalidMessageException {
        if(message.hasStorageLocationResponse()) {
        	synchronized(locations) {
        		locations.addAll(message.getStorageLocationResponse().getLocationsList());
                locations.notifyAll();
        	}
        }
        return null;
    }
    
}
