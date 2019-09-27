package edu.usfca.cs.dfs.controller.net;

import edu.usfca.cs.dfs.net.ServerMessageRouter;

import java.io.IOException;

public class Server {
    private ServerMessageRouter messageRouter;
    private int port;

    public Server(int port) {
        ControllerInboundHandler controllerInboundHandler = new ControllerInboundHandler();
        messageRouter = new ServerMessageRouter(controllerInboundHandler);
        this.port = port;
    }

    public void start()
            throws IOException {
        messageRouter.listen(port);
        System.out.println("Listening for connections on port 7777");
    }
}
