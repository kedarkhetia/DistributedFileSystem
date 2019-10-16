package edu.usfca.cs.dfs.storage;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
	public static List<Messages.StoredLocationType> locations = new LinkedList<>();
	public static Map<String, Messages.StorageFeedback> storageFeedbackMap = new HashMap<>();
	public static volatile Messages.DownloadFile.Builder downloadFileBuilder = Messages.DownloadFile.newBuilder();

    public static Messages.ProtoMessage dispatch(Messages.Storage message) 
    		throws InvalidMessageException, IOException, InterruptedException, ExecutionException, NoSuchAlgorithmException {
        if (message.hasUploadFile()) {
        	return StorageHandlers.retrive(message.getUploadFile());
        }
        else if (message.hasStoreChunk()) {
        	return StorageHandlers.store(message.getStoreChunk());
        }
        else if (message.hasStorageFeedback()) {
        	synchronized(storageFeedbackMap) {
        		storageFeedbackMap.put(message.getStorageFeedback().getFilename(), message.getStorageFeedback());
        		storageFeedbackMap.notifyAll();
        	}
        }
        else if (message.hasStoredLocationResponse()) {
        	synchronized(locations) {
        		locations.addAll(message.getStoredLocationResponse().getStoredLocationTypeList());
        		locations.notifyAll();
        	}
        }
        else if (message.hasDownloadFile()) {
        	synchronized(downloadFileBuilder) {
        		downloadFileBuilder
        		.setFileFound(message.getDownloadFile().getFileFound())
        		.setStoreChunk(message.getDownloadFile().getStoreChunk());
        		downloadFileBuilder.notifyAll();
        	}
        }
        else if (message.hasReplicate()) {
        	StorageHandlers.replicate(message.getReplicate());
        	System.out.println(message.getReplicate());
        }
        
        return null;
    }
    
}
