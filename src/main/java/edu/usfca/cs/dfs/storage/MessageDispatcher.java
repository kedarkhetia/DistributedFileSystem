package edu.usfca.cs.dfs.storage;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.StorageFeedback.Builder;

public class MessageDispatcher {
	public static volatile Builder storageFeedback = Messages.StorageFeedback.newBuilder();

    public static Messages.ProtoMessage dispatch(Messages.Storage message) throws InvalidMessageException, IOException, InterruptedException, ExecutionException {
        if (message.hasUploadFile()) {
        	return StorageHandlers.retrive(message.getUploadFile());
        }
        else if (message.hasStoreChunk()) {
        	return StorageHandlers.store(message.getStoreChunk());
        }
        else if(message.hasStorageFeedback()) {
        	synchronized(storageFeedback) {
        		storageFeedback
        		.setIsStored(message.getStorageFeedback().getIsStored())
        		.setFilename(message.getStorageFeedback().getFilename());
        		storageFeedback.notifyAll();
        	}
        }
        
        return null;
    }
    
}
