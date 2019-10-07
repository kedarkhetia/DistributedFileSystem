package edu.usfca.cs.dfs.controller;

import java.util.concurrent.ExecutionException;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;

public class MessageDispatcher {

    public static Messages.ProtoMessage dispatch(Messages.Controller message) throws InvalidMessageException, InterruptedException, ExecutionException {
        if(message.hasStorageNode()) {
            return ControllerHandlers.register(message.getStorageNode());
        }
        else if (message.hasStorageLocationRequest()) {
            return ControllerHandlers.getStorageLocations(message.getStorageLocationRequest());
        }
        else if (message.hasHeartbeat()) {
        	ControllerHandlers.setHeartbeat(message.getHeartbeat());
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
