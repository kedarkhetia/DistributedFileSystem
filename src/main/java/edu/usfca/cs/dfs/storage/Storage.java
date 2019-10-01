package edu.usfca.cs.dfs.storage;

import edu.usfca.cs.dfs.messages.Messages;

import java.io.IOException;

import edu.usfca.cs.dfs.clients.Client;
import edu.usfca.cs.dfs.utils.Constants;

public class Storage {

    public static volatile boolean flag = false;

    public static void main(String args[]) throws InterruptedException, IOException {
    	StorageServer s = new StorageServer(7775);
    	s.start();
        Client controllerClient = new Client(Constants.CONTROLLER_HOSTNAME, Constants.CONTROLLER_PORT);
        Messages.ProtoMessage msgWrapper = Messages.ProtoMessage
                .newBuilder()
                .setController(Messages.Controller
                        .newBuilder()
                        .setStorageNode(Messages.StorageNode
                                .newBuilder()
                                .setHost(Constants.STORAGE_HOSTNAME)
                                .setPort(7775)
                                .build())
                        .build())
                .build();
        controllerClient.sendMessage(msgWrapper);
        //controllerClient.disconnect();
    }
}
