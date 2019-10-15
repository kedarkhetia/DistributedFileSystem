package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.net.ServerMessageRouter;

import java.io.IOException;

public class ControllerServer {
    private ServerMessageRouter messageRouter;
    private int port;

    public ControllerServer(int port, int chunkSize) {
        messageRouter = new ServerMessageRouter(chunkSize);
        this.port = port;
    }

    public void start()
            throws IOException {
        messageRouter.listen(port);
        System.out.println("Listening for connections on port " + port);
    }
}
