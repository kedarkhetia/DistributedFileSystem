package edu.usfca.cs.dfs.clients;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

public class ControllerClientProxy {
    private Client client;

    public ControllerClientProxy() {
        this.client = new Client(Constants.CONTROLLER_HOSTNAME, Constants.CONTROLLER_PORT);
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
    
    public void getStoredLocations(String filename, int i) {
    	client.sendMessage(Messages.ProtoMessage.newBuilder()
    			.setController(Messages.Controller.newBuilder()
    					.setStoredLocationRequest(Messages.StoredLocationRequest.newBuilder()
    							.setFilename(filename)
    							.setChunkId(i)
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
