package edu.usfca.cs.dfs.dfsclient;

import edu.usfca.cs.dfs.clients.Client;
import edu.usfca.cs.dfs.messages.Messages;

public class StorageClientProxy {
    private Client client;

    public StorageClientProxy(String hostname, int port) {
        this.client = new Client(hostname, port);
    }
    
    public void upload(Messages.StoreChunk chunk) {
        Messages.ProtoMessage msg = Messages.ProtoMessage.newBuilder()
                .setStorage(Messages.Storage.newBuilder()
                		.setStoreChunk(chunk)
                		.build())
                .build();
        client.sendMessage(msg);
    }
    
    public void disconnect() {
    	client.disconnect();
    }

}
