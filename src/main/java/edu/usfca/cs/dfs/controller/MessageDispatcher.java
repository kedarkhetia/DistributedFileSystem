package edu.usfca.cs.dfs.controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;

public class MessageDispatcher {
	public static AtomicLong storageSpace = new AtomicLong(-1);

    public static Messages.ProtoMessage dispatch(Messages.Controller message) throws InvalidMessageException, InterruptedException, ExecutionException {
        if(message.hasStorageNode()) {
            return ControllerHandlers.register(message.getStorageNode());
        }
        else if (message.hasStorageLocationRequest()) {
            return ControllerHandlers.getStorageLocations(message.getStorageLocationRequest());
        }
        else if (message.hasStorageSpace()) {
        	synchronized(storageSpace) {
        		storageSpace.set(message.getStorageSpace().getAvailableSpace());
        		storageSpace.notifyAll();
        	}
        }
        else if (message.hasStoreProof()) {
        	ControllerHandlers.updateBloomFilter(message.getStoreProof());
        }
        else if (message.hasStoredLocationRequest()) {
        	return ControllerHandlers.getStoredLocations(message.getStoredLocationRequest());
        }
        return null;
    }

}
