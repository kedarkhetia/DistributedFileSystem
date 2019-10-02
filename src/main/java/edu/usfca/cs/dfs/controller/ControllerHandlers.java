package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

import java.util.*;

public class ControllerHandlers {

    private static List<Messages.StorageNode> nodeList = new LinkedList<>();
    private static HashMap<String, List<Messages.StorageNode>> replicaMap = new HashMap<>(); 
    private static Random random = new Random();

    public static Messages.ProtoMessage register(Messages.StorageNode storageNode) {
        if(!nodeList.contains(storageNode)) {
            nodeList.add(storageNode);
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

    public static Messages.ProtoMessage getStorageLocations(Messages.StorageLocationRequest request) {
        int i = random.nextInt(nodeList.size());
        Messages.StorageNode primary = nodeList.get(i);
        Messages.StorageLocationResponse.Builder locationBuilder = Messages.StorageLocationResponse.newBuilder();
        if(!replicaMap.containsKey(primary.getHost()+primary.getPort())) {
        	LinkedList<Messages.StorageNode> locations = new LinkedList<>();
        	locations.add(primary);
        	System.out.print(primary.getHost()+primary.getPort() + " -> (");
        	while(locations.size() != Constants.REPLICAS) {
            	// ToDo: check if the random selected storage node has enough space to store chunk or not!
        		int randint = random.nextInt(nodeList.size());
                if(!locations.contains(nodeList.get(randint))) {
                	locations.add(nodeList.get(randint));
                	System.out.print(nodeList.get(randint).getHost()+nodeList.get(randint).getPort() + " ");
                }
            }
        	System.out.println(")");
        	replicaMap.put(primary.getHost()+primary.getPort(), locations);
        }
    	locationBuilder.addAllLocations(replicaMap.get(primary.getHost()+primary.getPort()));
        Messages.Client clientMessage = Messages.Client.newBuilder()
                .setStorageLocationResponse(locationBuilder.build()).build();
        Messages.ProtoMessage msg = Messages.ProtoMessage.newBuilder().setClient(clientMessage).build();
        return msg;
    }
}
