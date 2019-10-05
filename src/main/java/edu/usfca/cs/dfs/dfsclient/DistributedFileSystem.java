package edu.usfca.cs.dfs.dfsclient;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

public class DistributedFileSystem {
	
	private static int chunkSize = Constants.CHUNK_SIZE;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);

    public DistributedFileSystem() {}

    public void put(String filename) throws IOException, InterruptedException, ExecutionException {
    	Path path = Paths.get(filename);
        try(BufferedReader in = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
            int readBytes;
            byte[] data;
            int count = (int) Files.size(path) / chunkSize;
            int bufCount = 0;
            int i = 0;
            ControllerClientProxy controllerClient = new ControllerClientProxy();
            for(i=0; i<count; i++) {
                data = new byte[chunkSize];
                bufCount = 0;
                while(bufCount < data.length && (readBytes = in.read()) != -1) {
                    data[bufCount] = (byte) readBytes;
                    bufCount++;
                }
                String chunkedFileName = filename + "_chunk" + i;
                controllerClient.getStorageLocations(chunkedFileName);
                List<Messages.StorageNode> locations = getStorageNodes().get();
                storeInStorage(chunkedFileName, i, ByteString.copyFrom(data), locations);
            }
            int restBuffer = (int) Files.size(path) - (chunkSize * count);
            bufCount = 0;
            data = new byte[restBuffer];
            while(bufCount < restBuffer && (readBytes = in.read()) != -1) {
                data[bufCount] = (byte) readBytes;
                bufCount++;
            }
            String chunkedFileName = filename + "_chunk" + i;
            controllerClient.getStorageLocations(chunkedFileName);
            List<Messages.StorageNode> locations = getStorageNodes().get();
            storeInStorage(chunkedFileName, i, ByteString.copyFrom(Arrays.copyOf(data, bufCount)), locations);
            controllerClient.disconnect();
        }
		
    }
    
    private void storeInStorage(String filename, int id, ByteString data, List<Messages.StorageNode> locations) {
		threadPool.execute(new Runnable() {
			@Override
			public void run() {
				Messages.StorageNode location = locations.get(0);
				StorageClientProxy storageClient = 
						new StorageClientProxy(location.getHost(), location.getPort());
				storageClient.upload(Messages.StoreChunk
						.newBuilder()
						.setFileName(filename)
						.setChunkId(id)
						.setData(data)
						.addAllStorageLocations(locations)
						.build());
				storageClient.disconnect();
			}
		});
	}
    
    private Future<List<Messages.StorageNode>> getStorageNodes() {
		return threadPool.submit(() -> {
			synchronized(MessageDispatcher.locations) {
				if(MessageDispatcher.locations.isEmpty()) {
					try {
						MessageDispatcher.locations.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				LinkedList<Messages.StorageNode> copy = new LinkedList<>(MessageDispatcher.locations);
				MessageDispatcher.locations = new LinkedList<Messages.StorageNode>();
				return copy;
			}
        });
	}
    
    public void close() throws InterruptedException {
    	threadPool.shutdown();
    	while (!threadPool.awaitTermination(24L, TimeUnit.HOURS)) {
    	    System.out.println("Waiting! Awaiting Distributed File System Termination!");
    	}
    }
    
}
