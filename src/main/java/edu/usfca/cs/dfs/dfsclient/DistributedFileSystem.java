package edu.usfca.cs.dfs.dfsclient;

import edu.usfca.cs.dfs.messages.Messages;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class DistributedFileSystem {

    public DistributedFileSystem() {}

    public void put(String filename) throws IOException, InterruptedException, ExecutionException {
    	List<Messages.StoreChunk> chunks = PutHelper.getChunks(filename);
		for(Messages.StoreChunk chunk : chunks) {
			ControllerClientProxy.newControllerClientProxy().getStorageLocations(filename);
			List<Messages.StorageNode> locations = PutHelper.getStorageNodes().get();
			PutHelper.storeInStorage(chunk, locations);
		}
    }
    
}
