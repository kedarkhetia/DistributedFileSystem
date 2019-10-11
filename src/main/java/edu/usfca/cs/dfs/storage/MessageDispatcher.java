package edu.usfca.cs.dfs.storage;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.StorageFeedback.Builder;

public class MessageDispatcher {
	public static volatile Messages.StorageFeedback.Builder storageFeedback = Messages.StorageFeedback.newBuilder();
	public static List<Messages.StoredLocationType> locations = new LinkedList<>();
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
        	synchronized(storageFeedback) {
        		storageFeedback
        		.setIsStored(message.getStorageFeedback().getIsStored())
        		.setFilename(message.getStorageFeedback().getFilename());
        		storageFeedback.notifyAll();
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
        
        return null;
    }
    
}
