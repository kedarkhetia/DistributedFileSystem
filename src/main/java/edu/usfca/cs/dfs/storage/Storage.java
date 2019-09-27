package edu.usfca.cs.dfs.storage;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.storage.client.ContorllerClient;

public class Storage {

    public static void main(String args[]) {
        ContorllerClient contorllerClient = new ContorllerClient("localhost", 7777);
        Messages.ProtoMessage msgWrapper = Messages.ProtoMessage
                .newBuilder()
                .setController(Messages.Controller
                        .newBuilder()
                        .setControllerMessage("Hey There!")
                        .build())
                .build();

        contorllerClient.sendMessage(msgWrapper);
        contorllerClient.disconnect();
    }
}
