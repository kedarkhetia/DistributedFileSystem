package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.ActiveNodes.Builder;
import edu.usfca.cs.dfs.messages.Messages.ControllerEmptyMessage;
import edu.usfca.cs.dfs.messages.Messages.ProtoMessage;
import edu.usfca.cs.dfs.utils.BloomFilter;
import edu.usfca.cs.dfs.utils.Constants;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ControllerHandlers {

    private static Random random = new Random();
    private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
    private static volatile Map<Messages.StorageNode, HeartbeatModel> heartbeatMap = new ConcurrentHashMap<>();
    
    public static Config CONFIG;
    

    public static Messages.ProtoMessage getStorageLocations(Messages.StorageLocationRequest request) throws InterruptedException, ExecutionException {
        //System.out.println(nodeList);
    	List<Messages.StorageNode> nodeList = new LinkedList<>(heartbeatMap.keySet());
    	int primaryIndex = random.nextInt(nodeList.size());
        Messages.StorageNode primary = nodeList.get(primaryIndex);
        while(!hasStorageSpace(request.getSize(), primary)) {
        	primaryIndex = random.nextInt(nodeList.size());
        	primary = nodeList.get(primaryIndex);
        }
        Messages.StorageLocationResponse.Builder locationBuilder = Messages.StorageLocationResponse.newBuilder();
        LinkedList<Messages.StorageNode> locations = new LinkedList<>();
        locations.add(primary);
        int i = (primaryIndex + 1) % nodeList.size();
        while(i != primaryIndex && locations.size() < CONFIG.getReplicaCount()) {
        	Messages.StorageNode replica = nodeList.get(i);
        	if(hasStorageSpace(request.getSize(), replica)) {
        		locations.add(replica);
        	}
        	i = (i+1) % nodeList.size();
        }
    	locationBuilder.addAllLocations(locations);
        return Messages.ProtoMessage.newBuilder().setClient( Messages.Client.newBuilder()
                .setStorageLocationResponse(locationBuilder.build()).build()).build();
    }
    
    private static boolean hasStorageSpace(long size, Messages.StorageNode storageNode) throws InterruptedException, ExecutionException {
    	HeartbeatModel heartbeat = heartbeatMap.get(storageNode);
    	return size < heartbeat.getAvailableSpace();
    }
    
    public static void setHeartbeat(Messages.Heartbeat heartbeat) {
    	//System.out.println("Received Heartbeat From: " + heartbeat.getStorageNode().getHost() + ":" + heartbeat.getStorageNode().getPort());
    	if(!heartbeatMap.containsKey(heartbeat.getStorageNode())) {
			HeartbeatModel newHeartbeat = new HeartbeatModel();
			newHeartbeat.setReplica(new BloomFilter(CONFIG.getReplicaK(), CONFIG.getReplicaM()));
			newHeartbeat.setPrimary(new BloomFilter(CONFIG.getPrimaryK(), CONFIG.getPrimaryM()));
			heartbeatMap.put(heartbeat.getStorageNode(), newHeartbeat);
		}
		HeartbeatModel heartbeatModel = heartbeatMap.get(heartbeat.getStorageNode());
		heartbeatModel.setAvailableSpace(heartbeat.getAvailableSpace());
		heartbeatModel.setProcessedRequests(heartbeat.getProcessedRequests());
		heartbeatModel.setTimestamp(System.currentTimeMillis());
    }
    
    public static Messages.ProtoMessage updateBloomFilter(Messages.StoreProof storeProof) {
    	HeartbeatModel heartbeat = heartbeatMap.get(storeProof.getNode());
    	boolean respFlag;
    	if(heartbeat == null) {
    		respFlag = false;
    	} 
    	else {
    		if(storeProof.getStorageType() == Messages.StoreProof.StorageType.PRIMARY) {
        		heartbeat.getPrimary().put(storeProof.getFilename().getBytes());
        		//System.out.println(storeProof.getNode().getHost() + storeProof.getNode().getPort() + " Primary: " + heartbeat.getPrimary());
        	} else {
        		heartbeat.getReplica().put(storeProof.getFilename().getBytes());
        		//System.out.println(storeProof.getNode().getHost() + storeProof.getNode().getPort() + " Replica: " + heartbeat.getReplica());
        	}
    		respFlag = true;
    	}
    	return Messages.ProtoMessage.newBuilder()
    			.setStorage(Messages.Storage.newBuilder()
    					.setStorageFeedback(Messages.StorageFeedback.newBuilder()
    							.setIsStored(respFlag)
    							.setFilename(storeProof.getFilename())
    							.build())
    					.build())
    			.build();
    }
    
    public static Messages.ProtoMessage getStoredLocations(Messages.StoredLocationRequest storedLocationRequest) {
    	String filename = storedLocationRequest.getFilename();
    	List<Messages.StorageNode> locations = new LinkedList<>();
    	for(Messages.StorageNode node : heartbeatMap.keySet()) {
    		if(heartbeatMap.get(node).getPrimary().get(filename.getBytes())) {
    			locations.add(node);
    		}
    		else if(heartbeatMap.get(node).getReplica().get(filename.getBytes())) {
    			locations.add(node);
    		}
    	}
    	Messages.StoredLocationResponse.Builder locationBuilder = Messages.StoredLocationResponse.newBuilder();
    	locationBuilder.addAllLocations(locations);
    	locationBuilder.setFilename(storedLocationRequest.getFilename());
    	return Messages.ProtoMessage.newBuilder()
    			.setClient(Messages.Client.newBuilder()
    					.setStoredLocationResponse(locationBuilder.build()))
    			.build();
    }
    
    public static void monitorHeartBeats() {
    	threadPool.execute(new Runnable() {
    		@Override
    		public void run() {
    			long cap = 2000; // Cap for 2000 milliseconds 
    			while(true) {
    				List<Messages.StorageNode> removeNodes = new LinkedList<>();
        			for(Messages.StorageNode node : heartbeatMap.keySet()) {
        				HeartbeatModel heartbeat = heartbeatMap.get(node);
        				if(System.currentTimeMillis() - heartbeat.getTimestamp() > Constants.HEARTBEAT_INTERVAL + cap) {
        					removeNodes.add(node);
        				}
        			}
        			for(Messages.StorageNode node : removeNodes) {
        				heartbeatMap.remove(node);
        				//System.out.println("Heartbeat not received, Removing node: " + node);
        			}
        			try {
						Thread.sleep(1000); // check every second
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
    			}
    		}
     	});
    }

	public static Messages.ProtoMessage getMetaInfo(ControllerEmptyMessage contorllerEmptyMessage) {
		if(contorllerEmptyMessage.getRequestType() == Messages.ControllerEmptyMessage.RequestType.ACTIVE_NODES) {
			return getActiveNodes();		
		}
		else if(contorllerEmptyMessage.getRequestType() == Messages.ControllerEmptyMessage.RequestType.TOTAL_DISKSPACE) {
			return getTotalDiskSpace();
		}
		else if(contorllerEmptyMessage.getRequestType() == Messages.ControllerEmptyMessage.RequestType.REQUESTS_SERVED) {
			return getRequestsServed();	
		}
		return null;
	}
	
	public static Messages.ProtoMessage getActiveNodes() {
		return Messages.ProtoMessage.newBuilder()
				.setClient(Messages.Client.newBuilder()
						.setActiveNodes(Messages.ActiveNodes.newBuilder()
								.addAllActiveNodes(new LinkedList<Messages.StorageNode>(heartbeatMap.keySet()))
								.build())
						.build())
				.build();
	}
	
	public static Messages.ProtoMessage getTotalDiskSpace() {
		long totalDiskSpace = 0;
		for(Messages.StorageNode storageNode : heartbeatMap.keySet()) {
			totalDiskSpace += heartbeatMap.get(storageNode).getAvailableSpace();
		}
		return Messages.ProtoMessage.newBuilder()
				.setClient(Messages.Client.newBuilder()
						.setTotalDiskSpace(Messages.TotalDiskSpace.newBuilder()
								.setDiskSpace(totalDiskSpace)
								.build())
						.build())
				.build();
	}
	
	public static Messages.ProtoMessage getRequestsServed() {
		List<Messages.RequestPerNode> requestsPerNode = new LinkedList<>();
		for(Messages.StorageNode storageNode : heartbeatMap.keySet()) {
			requestsPerNode.add(Messages.RequestPerNode.newBuilder()
					.setNode(storageNode)
					.setRequests(heartbeatMap.get(storageNode).getProcessedRequests())
					.build());
		}
		return Messages.ProtoMessage.newBuilder()
				.setClient(Messages.Client.newBuilder()
						.setRequestServed(Messages.RequestsServed.newBuilder()
								.addAllRequestsPerNode(requestsPerNode)
								.build())
						.build())
				.build();
	}
}
