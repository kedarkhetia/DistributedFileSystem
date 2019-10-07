package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.BloomFilter;
import edu.usfca.cs.dfs.utils.Constants;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ControllerHandlers {

    private static List<Messages.StorageNode> nodeList = new LinkedList<>();
    private static Random random = new Random();
    private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
    private static Map<Messages.StorageNode, BloomFilter> primaryBloomFilter = new HashMap<>();
    private static Map<Messages.StorageNode, BloomFilter> replicaBloomFilter = new HashMap<>();
    
    public static Config CONFIG;

    public static Messages.ProtoMessage register(Messages.StorageNode storageNode) {
        if(!nodeList.contains(storageNode)) {
            nodeList.add(storageNode);
            primaryBloomFilter.put(storageNode, new BloomFilter(CONFIG.getPrimaryK(), CONFIG.getPrimaryM()));
            replicaBloomFilter.put(storageNode, new BloomFilter(CONFIG.getReplicaK(), CONFIG.getReplicaM()));
            System.out.println("Added new storage node!");
        } else {
            System.out.println("Node already exist!");
        }
        return Messages.ProtoMessage.newBuilder()
                .setStorage(Messages.Storage.newBuilder()
                        .setStoreNodeResponse(Messages.StorageNodeResponse
                                .newBuilder().setFlag(true))
                        .build())
                .build();
    }
    
    // ToDo: deregister node when not able to connect.

    public static Messages.ProtoMessage getStorageLocations(Messages.StorageLocationRequest request) throws InterruptedException, ExecutionException {
        System.out.println(nodeList);
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
        	if(hasStorageSpace(request.getSize(), nodeList.get(i))) {
        		Messages.StorageNode replica = nodeList.get(i);
        		locations.add(replica);
        	}
        	i = (i+1) % nodeList.size();
        }
    	locationBuilder.addAllLocations(locations);
        Messages.Client clientMessage = Messages.Client.newBuilder()
                .setStorageLocationResponse(locationBuilder.build()).build();
        Messages.ProtoMessage msg = Messages.ProtoMessage.newBuilder().setClient(clientMessage).build();
        return msg;
    }
    
    private static boolean hasStorageSpace(long size, Messages.StorageNode storageNode) throws InterruptedException, ExecutionException {
    	StorageClientProxy storageClientProxy = new StorageClientProxy(storageNode.getHost(), storageNode.getPort());
    	storageClientProxy.getStorageSpace(Messages.StorageEmptyMessage.newBuilder()
    			.setMessageType(Messages.StorageEmptyMessage.MessageType.AVAILABLE_SPACE).build());
    	return size < getHasStorageNode().get();
    }
    
    private static Future<Long> getHasStorageNode() {
    	return threadPool.submit(() -> {
    		synchronized(MessageDispatcher.storageSpace) {
    			if(MessageDispatcher.storageSpace.get() == -1) {
    				try {
						MessageDispatcher.storageSpace.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
    			}
    			Long result = MessageDispatcher.storageSpace.get();
    			MessageDispatcher.storageSpace.set(-1);
    			return result;
    		}
    	});
    }
    
    public static void updateBloomFilter(Messages.StoreProof storeProof) {
    	if(storeProof.getStorageType() == Messages.StoreProof.StorageType.PRIMARY) {
    		primaryBloomFilter.get(storeProof.getNode()).put(storeProof.getFilename().getBytes());
    		System.out.println(storeProof.getNode().getHost() + storeProof.getNode().getPort() + " Primary: " + primaryBloomFilter);
    	} else {
    		replicaBloomFilter.get(storeProof.getNode()).put(storeProof.getFilename().getBytes());
    		System.out.println(storeProof.getNode().getHost() + storeProof.getNode().getPort() + " Replica: " + replicaBloomFilter);
    	}
    }
    
    public static Messages.ProtoMessage getStoredLocations(Messages.StoredLocationRequest storedLocationRequest) {
    	String filename = storedLocationRequest.getFilename();
    	List<Messages.StorageNode> locations = new LinkedList<>();
    	for(Messages.StorageNode node : primaryBloomFilter.keySet()) {
    		if(primaryBloomFilter.get(node).get(filename.getBytes())) {
    			locations.add(node);
    		}
    	}
    	for(Messages.StorageNode node : replicaBloomFilter.keySet()) {
    		if(replicaBloomFilter.get(node).get(filename.getBytes())) {
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
}
