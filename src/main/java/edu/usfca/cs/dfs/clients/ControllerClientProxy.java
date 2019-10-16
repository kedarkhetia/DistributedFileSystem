package edu.usfca.cs.dfs.clients;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

/**
 * Implements client proxy for communication with Controller Server.
 * It contains message wrappers for different kind of message types.
 * @author kedarkhetia
 *
 */
public class ControllerClientProxy {
    private Client client;

    public ControllerClientProxy(String hostname, int port, int chunkSize) {
        this.client = new Client(hostname, port, chunkSize);
    }
    
    public void getStorageLocations(String filename) { 
        client.sendMessage(Messages.ProtoMessage.newBuilder()
                .setController(Messages.Controller.newBuilder()
                        .setStorageLocationRequest(Messages.StorageLocationRequest.newBuilder()
                                .setFilename(filename).build())
                        .build())
                .build());
    }
    
    public void sendStorageProof(String filename, Messages.StorageType storageType, 
    		Messages.StorageNode node) {
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
                .setController(Messages.Controller.newBuilder()
                        .setStoreProof(Messages.StoreProof.newBuilder()
                        		.setFilename(filename)
                        		.setNode(node)
                        		.setStorageType(storageType))
                        .build())
                .build());
    	
    }
    
    public void getStoredLocations(String filename, Messages.NodeType nodeType) {
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
    			.setController(Messages.Controller.newBuilder()
    					.setStoredLocationRequest(Messages.StoredLocationRequest.newBuilder()
    							.setFilename(filename)
    							.setNodeType(nodeType)
    							.build())
    					.build())
    			.build());
    }
    
    public void sendHeartbeat(long availableSpace, long processedRequest, Messages.StorageNode node) {
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
    			.setController(Messages.Controller.newBuilder()
    					.setHeartbeat(Messages.Heartbeat.newBuilder()
    							.setAvailableSpace(availableSpace)
    							.setProcessedRequests(processedRequest)
    							.setStorageNode(node)
    							.build())
    					.build())
    			.build());
    }
    
    public void getActiveNodes() {
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
    			.setController(Messages.Controller.newBuilder()
    					.setContorllerEmptyMessage(Messages.ControllerEmptyMessage.newBuilder()
    							.setRequestType(Messages.ControllerEmptyMessage.RequestType.ACTIVE_NODES)
    							.build())
    					.build())
    			.build());
    }
    
    public void getTotalDiskspace() {
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
    			.setController(Messages.Controller.newBuilder()
    					.setContorllerEmptyMessage(Messages.ControllerEmptyMessage.newBuilder()
    							.setRequestType(Messages.ControllerEmptyMessage.RequestType.TOTAL_DISKSPACE)
    							.build())
    					.build())
    			.build());
    }
    
    public void getProcessedRequest() {
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
    			.setController(Messages.Controller.newBuilder()
    					.setContorllerEmptyMessage(Messages.ControllerEmptyMessage.newBuilder()
    							.setRequestType(Messages.ControllerEmptyMessage.RequestType.REQUESTS_SERVED)
    							.build())
    					.build())
    			.build());
    }
    
    public void disconnect() {
    	client.disconnect();
    }
}
