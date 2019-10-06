package edu.usfca.cs.dfs.clients;

import edu.usfca.cs.dfs.messages.Messages;

public class StorageClientProxy {
    private Client client;

    public StorageClientProxy(String hostname, int port) {
        this.client = new Client(hostname, port);
    }
    
    public void upload(Messages.StoreChunk chunk) {
        client.sendMessage(Messages.ProtoMessage.newBuilder()
                .setStorage(Messages.Storage.newBuilder()
                		.setStoreChunk(chunk)
                		.build())
                .build());
    }
    
    public void getStorageSpace(Messages.StorageEmptyMessage storageSpace) { 
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
    			.setStorage(Messages.Storage.newBuilder()
    					.setMessageType(storageSpace)
    					.build())
    			.build());
    }
    
    public void download(Messages.UploadFile uploadFile) {
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
    			.setStorage(Messages.Storage.newBuilder()
    					.setUploadFile(uploadFile)
    					.build())
    			.build()); 
    }
    
    public void disconnect() {
    	client.disconnect();
    }

}
