package edu.usfca.cs.dfs.storage;

import java.io.IOException;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;

public class MessageDispatcher {

    public static Messages.ProtoMessage dispatch(Messages.Storage message) throws InvalidMessageException, IOException {
        if(message.hasStoreNodeResponse()) {
            if(message.getStoreNodeResponse().getFlag()) {
                System.out.println("Registered successfully to ControllerNode!");
            }
        }
        else if (message.hasStoreChunk()) {
        	StorageHandlers.store(message.getStoreChunk());
        }
        return null;
    }
}
