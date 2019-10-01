package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

import java.util.*;

public class ControllerHandlers {

    private static List<Messages.StorageNode> nodeList = new LinkedList<>();

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
        Set<Integer> replicaIndex = new HashSet<>();
        Random random = new Random();
        while(replicaIndex.size() != Constants.REPLICAS) {
        	// ToDo: check if the random selected storage node has enough space to store chunk or not!
            replicaIndex.add(random.nextInt(nodeList.size()));
        }
        Messages.StorageLocationResponse.Builder locationBuilder = Messages.StorageLocationResponse.newBuilder();
        for(int i : replicaIndex) {
            locationBuilder.addLocations(nodeList.get(i));
        }
        Messages.Client clientMessage = Messages.Client.newBuilder()
                .setStorageLocationResponse(locationBuilder.build()).build();
        Messages.ProtoMessage msg = Messages.ProtoMessage.newBuilder().setClient(clientMessage).build();
        return msg;
    }
}
