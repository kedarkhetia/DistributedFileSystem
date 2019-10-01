package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.exceptions.InvalidMessageException;
import edu.usfca.cs.dfs.messages.Messages;

public class MessageDispatcher {

    public static Messages.ProtoMessage dispatch(Messages.Controller message) throws InvalidMessageException {
        if(message.hasStorageNode()) {
            return ControllerHandlers.register(message.getStorageNode());
        }
        else if (message.hasStorageLocationRequest()) {
            return ControllerHandlers.getStorageLocations(message.getStorageLocationRequest());
        }
        return null;
    }

}
