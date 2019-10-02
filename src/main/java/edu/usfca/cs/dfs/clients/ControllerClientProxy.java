package edu.usfca.cs.dfs.clients;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

public class ControllerClientProxy {
    private static ControllerClientProxy controllerClientProxy;
    private Client client;

    public static ControllerClientProxy newControllerClientProxy() {
        if(controllerClientProxy == null) {
            controllerClientProxy = new ControllerClientProxy();
        }
        return controllerClientProxy;
    }

    private ControllerClientProxy() {
        this.client = new Client(Constants.CONTROLLER_HOSTNAME, Constants.CONTROLLER_PORT);
    }

    public void getStorageLocations(String filename) {
        Messages.ProtoMessage msg = Messages.ProtoMessage.newBuilder()
                .setController(Messages.Controller.newBuilder()
                        .setStorageLocationRequest(Messages.StorageLocationRequest.newBuilder()
                                .setFilename(filename).build())
                        .build())
                .build();
        client.sendMessage(msg);
    }
    
    public void disconnect() {
    	client.disconnect();
    }
}
