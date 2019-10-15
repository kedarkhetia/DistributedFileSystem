package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.ControllerEmptyMessage;
import edu.usfca.cs.dfs.utils.BloomFilter;
import edu.usfca.cs.dfs.utils.Config;
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

    public static synchronized Messages.ProtoMessage getStorageLocations(Messages.StorageLocationRequest request) throws InterruptedException, ExecutionException {
        //System.out.println(nodeList);
    	List<Messages.StorageNode> locations = getStoredLocations(request.getFilename());
    	if(locations.isEmpty()) {
    		List<Messages.StorageNode> nodeList = new LinkedList<>(heartbeatMap.keySet());
        	int primaryIndex = random.nextInt(nodeList.size());
            Messages.StorageNode primary = nodeList.get(primaryIndex);
            while(!hasStorageSpace(request.getSize(), primary)) {
            	primaryIndex = random.nextInt(nodeList.size());
            	primary = nodeList.get(primaryIndex);
            }
            locations = new LinkedList<>();
            locations.add(primary);
            List<Messages.StorageNode> replicas = heartbeatMap.get(primary).getReplicaList();
            if(replicas != null) {
            	for(Messages.StorageNode replica : replicas) {
            		if(hasStorageSpace(request.getSize(), replica)) {
            			locations.add(replica);
            			if(locations.size() == Controller.config.getReplicaCount()) {
            				break;
            			}
            		}
            	}	
            } else {
            	replicas = new LinkedList<>();
            }
            int i = (primaryIndex + 1) % nodeList.size();
            while(i != primaryIndex && locations.size() < Controller.config.getReplicaCount()) {
            	Messages.StorageNode replica = nodeList.get(i);
            	if(!locations.contains(replica) && hasStorageSpace(request.getSize(), replica)) {
                		locations.add(replica);
                		replicas.add(replica);
                		i = (i+1) % nodeList.size();
                }
            }
            heartbeatMap.get(primary).setReplicaList(replicas);
    	}
        return Messages.ProtoMessage.newBuilder().setClient(Messages.Client.newBuilder()
                	.setStorageLocationResponse(Messages.StorageLocationResponse.newBuilder()
                		.addAllLocations(locations)
                		.build())
                	.build())
        		.build();
    }
    
    private static synchronized boolean hasStorageSpace(long size, Messages.StorageNode storageNode) throws InterruptedException, ExecutionException {
    	HeartbeatModel heartbeat = heartbeatMap.get(storageNode);
    	return size < heartbeat.getAvailableSpace();
    }
    
    public static synchronized void setHeartbeat(Messages.Heartbeat heartbeat) {
    	//System.out.println("Received Heartbeat From: " + heartbeat.getStorageNode().getHost() + ":" + heartbeat.getStorageNode().getPort());
    	if(!heartbeatMap.containsKey(heartbeat.getStorageNode())) {
			HeartbeatModel newHeartbeat = new HeartbeatModel();
			newHeartbeat.setReplica(new BloomFilter(Controller.config.getReplicaK(), Controller.config.getReplicaM()));
			newHeartbeat.setPrimary(new BloomFilter(Controller.config.getPrimaryK(), Controller.config.getPrimaryM()));
			heartbeatMap.put(heartbeat.getStorageNode(), newHeartbeat);
		}
		HeartbeatModel heartbeatModel = heartbeatMap.get(heartbeat.getStorageNode());
		heartbeatModel.setAvailableSpace(heartbeat.getAvailableSpace());
		heartbeatModel.setProcessedRequests(heartbeat.getProcessedRequests());
		heartbeatModel.setTimestamp(System.currentTimeMillis());
    }
    
    public static synchronized Messages.ProtoMessage updateBloomFilter(Messages.StoreProof storeProof) {
    	HeartbeatModel heartbeat = heartbeatMap.get(storeProof.getNode());
    	if(storeProof.getStorageType() == Messages.StorageType.PRIMARY && heartbeat != null) {
    		heartbeat.getPrimary().put(storeProof.getFilename().getBytes());
    		System.out.println(storeProof.getNode().getHost() + storeProof.getNode().getPort() + " Primary: " + heartbeat.getPrimary());
    	} else {
    		heartbeat.getReplica().put(storeProof.getFilename().getBytes());
    		//System.out.println(storeProof.getNode().getHost() + storeProof.getNode().getPort() + " Replica: " + heartbeat.getReplica());
    	}
    	return Messages.ProtoMessage.newBuilder()
    			.setStorage(Messages.Storage.newBuilder()
    					.setStorageFeedback(Messages.StorageFeedback.newBuilder()
    							.setIsStored(true)
    							.setFilename(storeProof.getFilename())
    							.build())
    					.build())
    			.build();
    }
    
    private static synchronized List<Messages.StorageNode> getStoredLocations(String filename) {
    	List<Messages.StorageNode> nodes = new LinkedList<>();
    	for(Messages.StorageNode node : heartbeatMap.keySet()) {
    		if(heartbeatMap.get(node).getPrimary().get(filename.getBytes())) {
    			nodes.add(node);
    		}
    	}
    	for(Messages.StorageNode node : heartbeatMap.keySet()) {
    		if(heartbeatMap.get(node).getReplica().get(filename.getBytes())) {
    			nodes.add(node);
    		}
    	}
    	return nodes;
    }
    
    public static synchronized Messages.ProtoMessage getStoredLocations(Messages.StoredLocationRequest storedLocationRequest) {
    	String filename = storedLocationRequest.getFilename();
    	List<Messages.StoredLocationType> storageLocationTypes = new LinkedList<>();
    	List<Messages.StorageNode> nodes = getStoredLocations(filename);
    	boolean flag = true;
    	for(Messages.StorageNode node : nodes) {
    		if(flag) {
    			storageLocationTypes.add(Messages.StoredLocationType.newBuilder()
    					.setLocation(node)
    					.setStorageType(Messages.StorageType.PRIMARY)
    					.build());
    			flag = false;
    		}
    		else {
    			storageLocationTypes.add(Messages.StoredLocationType.newBuilder()
    					.setLocation(node)
    					.setStorageType(Messages.StorageType.REPLICA)
    					.build());
    		}
    	}
    	Messages.StoredLocationResponse.Builder locationBuilder = Messages.StoredLocationResponse.newBuilder();
    	locationBuilder.addAllStoredLocationType(storageLocationTypes);
    	locationBuilder.setFilename(storedLocationRequest.getFilename());
    	if(storedLocationRequest.getNodeType() == Messages.NodeType.CLIENT) {
    		return Messages.ProtoMessage.newBuilder()
        			.setClient(Messages.Client.newBuilder()
        					.setStoredLocationResponse(locationBuilder.build()))
        			.build();
    	}
    	else {
    		return Messages.ProtoMessage.newBuilder()
        			.setStorage(Messages.Storage.newBuilder()
        					.setStoredLocationResponse(locationBuilder.build()))
        			.build();
    	}
    }
    
    public static synchronized void monitorHeartBeats() {
    	threadPool.execute(new Runnable() {
    		@Override
    		public void run() {
    			long cap = 3000; // Cap for 3000 milliseconds 
    			while(true) {
    				List<Messages.StorageNode> removeNodes = new LinkedList<>();
        			for(Messages.StorageNode node : heartbeatMap.keySet()) {
        				HeartbeatModel heartbeat = heartbeatMap.get(node);
        				if(System.currentTimeMillis() - heartbeat.getTimestamp() > Constants.HEARTBEAT_INTERVAL + cap) {
        					removeNodes.add(node);
        				}
        			}
        			for(Messages.StorageNode node : removeNodes) {
        				//System.out.println(heartbeatMap.get(node).getReplicaList());
        				List<Messages.StorageNode> dependentNodes = getDependentNodes(node);
        				Messages.StorageNode replacement = getReplacementNode(node, dependentNodes);
        				if(replacement == null) {
        					 System.out.println("No replacement node found, data will be lost!");
        				} 
        				else {
        					replace(node, replacement, dependentNodes, Messages.StorageType.PRIMARY);
        					if(heartbeatMap.get(node).getReplicaList() != null) {
        						replace(node, replacement, heartbeatMap.get(node).getReplicaList(), 
            							Messages.StorageType.REPLICA);
        					}
        				}
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
    
    private static synchronized void replace(Messages.StorageNode node, Messages.StorageNode replacementNode, 
    		List<Messages.StorageNode> dependents, Messages.StorageType storageType) {
    	for(Messages.StorageNode dependent : dependents) {
    		replicate(Messages.Replicate.newBuilder()
    				.setFromNode(dependent)
    				.setToNode(replacementNode)
    				.setForNode(node)
    				.setStorageType(storageType)
    				.setNodeType(Messages.NodeType.STORAGE)
    				.build());
    		if(storageType == Messages.StorageType.PRIMARY) {
    			Collections.replaceAll(heartbeatMap.get(dependent).getReplicaList(), node, replacementNode);
    		}
    		else {
    			List<Messages.StorageNode> replicaList = heartbeatMap.get(replacementNode).getReplicaList();
    			if(replicaList == null) {
    				replicaList = new LinkedList<>();
    				replicaList.add(dependent);
    				heartbeatMap.get(replacementNode).setReplicaList(replicaList);
    			}
    			else {
    				replicaList.add(dependent);
    			}
    		}
    	}
    }
    
    private static synchronized void replicate(Messages.Replicate replicate) {
    	threadPool.execute(new Runnable() {
    		@Override
    		public void run() {
    			Messages.StorageNode node = replicate.getFromNode();
    			StorageClientProxy storageClientProxy = new StorageClientProxy(node.getHost(), node.getPort(), 
    					Controller.config.getChunkSize());
    			storageClientProxy.replicate(replicate);
    			storageClientProxy.disconnect();
    		}
    	});
    }
    
    private static synchronized List<Messages.StorageNode> getDependentNodes(Messages.StorageNode storageNode) {
    	List<Messages.StorageNode> replicaList = new LinkedList<>();
    	for(Messages.StorageNode node : heartbeatMap.keySet()) {
    		List<Messages.StorageNode> replicaListForNode = heartbeatMap.get(node).getReplicaList();
    		if(replicaListForNode != null && replicaListForNode.contains(storageNode)) {
    			//System.out.println("Dependent nodes: " + node);
    			replicaList.add(node);
    		}
    	}
    	return replicaList;
    }
    
    private static synchronized Messages.StorageNode getReplacementNode(Messages.StorageNode storageNode, 
    		List<Messages.StorageNode> nodes) {
    	List<Messages.StorageNode> noEligible = new LinkedList<>();
    	noEligible.add(storageNode);
    	if(heartbeatMap.get(storageNode).getReplicaList() != null)
    		noEligible.addAll(heartbeatMap.get(storageNode).getReplicaList());
    	for(Messages.StorageNode node : nodes) {
    		noEligible.add(node);
    		if(heartbeatMap.get(node).getReplicaList() != null)
    			noEligible.addAll(heartbeatMap.get(node).getReplicaList());
    	}
    	List<Messages.StorageNode> storageNodeList = new LinkedList<>(heartbeatMap.keySet());
    	int i = random.nextInt(storageNodeList.size());
    	int count = 0;
    	while(noEligible.contains(storageNodeList.get(i))) {
    		i = random.nextInt(storageNodeList.size());
    		count++;
    		if(count == heartbeatMap.size())
    			return null;
    	}
    	return storageNodeList.get(i);
    }

	public static synchronized Messages.ProtoMessage getMetaInfo(ControllerEmptyMessage contorllerEmptyMessage) {
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
	
	public static synchronized Messages.ProtoMessage getActiveNodes() {
		return Messages.ProtoMessage.newBuilder()
				.setClient(Messages.Client.newBuilder()
						.setActiveNodes(Messages.ActiveNodes.newBuilder()
								.addAllActiveNodes(new LinkedList<Messages.StorageNode>(heartbeatMap.keySet()))
								.build())
						.build())
				.build();
	}
	
	public static synchronized Messages.ProtoMessage getTotalDiskSpace() {
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
	
	public static synchronized Messages.ProtoMessage getRequestsServed() {
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
