package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.net.ServerMessageRouter;

import java.io.IOException;

public class ControllerServer {
    private ServerMessageRouter messageRouter;
    private int port;

    public ControllerServer(int port) {
        messageRouter = new ServerMessageRouter();
        this.port = port;
    }

    public void start()
            throws IOException {
        messageRouter.listen(port);
        System.out.println("Listening for connections on port " + port);
    }
}
