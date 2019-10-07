package edu.usfca.cs.dfs.storage;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;

public class MessageDispatcher {
	
	public static AtomicBoolean REGISTER_FLAG = new AtomicBoolean(false);

    public static Messages.ProtoMessage dispatch(Messages.Storage message) throws InvalidMessageException, IOException {
        if(message.hasStoreNodeResponse()) {
        	synchronized(REGISTER_FLAG) {
        		REGISTER_FLAG.set(message.getStoreNodeResponse().getFlag());
        		REGISTER_FLAG.notifyAll();
        	}
        }
        else if (message.hasUploadFile()) {
        	return StorageHandlers.retrive(message.getUploadFile());
        }
        else if (message.hasStoreChunk()) {
        	StorageHandlers.store(message.getStoreChunk());
        }
        
        return null;
    }
    
}
