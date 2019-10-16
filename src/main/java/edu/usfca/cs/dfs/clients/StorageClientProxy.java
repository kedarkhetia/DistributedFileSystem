package edu.usfca.cs.dfs.clients;

import edu.usfca.cs.dfs.messages.Messages;

/**
 * Implements client proxy for communication with Storage Server.
 * It contains message wrappers for different kind of message types.
 * @author kedarkhetia
 *
 */
public class StorageClientProxy {
    private Client client;

    public StorageClientProxy(String hostname, int port, int chunkSize) {
        this.client = new Client(hostname, port, chunkSize);
    }
    
    public void upload(Messages.StoreChunk chunk) {
        client.sendMessage(Messages.ProtoMessage.newBuilder()
                .setStorage(Messages.Storage.newBuilder()
                		.setStoreChunk(chunk)
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
    
    public void replicate(Messages.Replicate replicate) {
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
    			.setStorage(Messages.Storage.newBuilder()
    					.setReplicate(replicate)
    					.build())
    			.build()); 
    }
    
    public void disconnect() {
    	client.disconnect();
    }

}
