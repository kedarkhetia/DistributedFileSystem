package edu.usfca.cs.dfs.storage;

import java.io.IOException;

import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class StorageServer {
	private ServerMessageRouter messageRouter;
    private int port;

    public StorageServer(int port, int chunkSize) {
        messageRouter = new ServerMessageRouter(chunkSize);
        this.port = port;
    }

    public void start()
            throws IOException {
        messageRouter.listen(port);
        System.out.println("Listening for connections on port " + port);
    }
}
