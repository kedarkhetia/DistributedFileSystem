package edu.usfca.cs.dfs.controller;

import java.util.concurrent.ExecutionException;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;

/**
 * Identifies the message type and either set's a respective variable
 * or deligates the request to handlers.
 * @author kedarkhetia
 *
 */
public class MessageDispatcher {

    public static Messages.ProtoMessage dispatch(Messages.Controller message) throws InvalidMessageException, InterruptedException, ExecutionException {
        if (message.hasStorageLocationRequest()) {
            return ControllerHandlers.getStorageLocations(message.getStorageLocationRequest());
        }
        else if (message.hasHeartbeat()) {
        	ControllerHandlers.setHeartbeat(message.getHeartbeat());
        }
        else if (message.hasStoreProof()) {
        	return ControllerHandlers.updateBloomFilter(message.getStoreProof());
        }
        else if (message.hasStoredLocationRequest()) {
        	return ControllerHandlers.getStoredLocations(message.getStoredLocationRequest());
        }
        else if (message.hasContorllerEmptyMessage()) {
        	return ControllerHandlers.getMetaInfo(message.getContorllerEmptyMessage());
        }
        return null;
    }

}
