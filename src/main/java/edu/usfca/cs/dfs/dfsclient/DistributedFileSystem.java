package edu.usfca.cs.dfs.dfsclient;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

public class DistributedFileSystem {
	
	private static int chunkSize = Constants.CHUNK_SIZE;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
	private Map<String, Integer> fileToChunkMap = new HashMap<>();
	private static String CHUNK_SUFFIX = "_chunk";

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
                String chunkedFileName = filename + CHUNK_SUFFIX + i;
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
            if(bufCount != 0) {
            	fileToChunkMap.put(filename, count+1);
            } else {
            	fileToChunkMap.put(filename, count);
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
    	while (!threadPool.awaitTermination(24, TimeUnit.HOURS)) {
    	    System.out.println("Waiting! Awaiting Distributed File System Termination!");
    	}
    	System.out.println("DSF Shutdown Successfully!");
    }
    
    public boolean get(String storagePath, String filename) throws InterruptedException, ExecutionException, IOException {
    	int chunkCount = fileToChunkMap.get(filename);
    	Path path = Paths.get(storagePath+filename);
    	Files.deleteIfExists(path);
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
		List<Future<Messages.DownloadFile>> writeTaskCallbacks = new LinkedList<>();
		try(BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"), StandardOpenOption.WRITE)) {
			for(int i=0; i < chunkCount; i++) {
	    		String chunkName = filename + CHUNK_SUFFIX + i;
	    		List<Messages.StorageNode> locations = getStoredNodes(chunkName, i).get();
	    		writeTaskCallbacks.add(getDataFromLocations(locations, chunkName, i, writer));	
	    	}
			for(Future<Messages.DownloadFile> callback : writeTaskCallbacks) {
				Messages.DownloadFile downloadedChunk = callback.get();
				if(downloadedChunk == null) {
					return false;
				}
				String data = downloadedChunk.getStoreChunk().getData().toStringUtf8();
				writer.write(data, 0, data.length());
			}
		}
		return true;
    }
    
    private Future<Messages.DownloadFile> getDataFromLocations(List<Messages.StorageNode> locations, String chunkName, int chunkIndex, BufferedWriter writer) {
    	return threadPool.submit(() -> {
    		for(int j=0; j<locations.size(); j++) { 
    			Messages.DownloadFile downloadedChunk = getStoredData(chunkName, locations.get(j)).get();
    			if(downloadedChunk.getFileFound()) {
    				return downloadedChunk;
    			}
    		}
    		return null;
    	});
    }
    
    private Future<Messages.DownloadFile> getStoredData(String chunkName, Messages.StorageNode node) {
    	return threadPool.submit(() -> {
    		StorageClientProxy storageClientProxy = new StorageClientProxy(node.getHost(), 
					node.getPort());
			storageClientProxy.download(Messages.UploadFile.newBuilder()
					.setFilename(chunkName)
					.setStorageNode(node).build());
    		synchronized(MessageDispatcher.chunkToData) {
				while(!MessageDispatcher.chunkToData.containsKey(chunkName)) {
					try {
						MessageDispatcher.chunkToData.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				Messages.DownloadFile data = MessageDispatcher.chunkToData.remove(chunkName);
				storageClientProxy.disconnect();
				MessageDispatcher.chunkToData.notifyAll();
				return data;
			}
    	});
    }
    
    private Future<List<Messages.StorageNode>> getStoredNodes(String chunkName, int chunkId) {
    	return threadPool.submit(() -> {
    		ControllerClientProxy controllerClientProxy = new ControllerClientProxy();
    		controllerClientProxy.getStoredLocations(chunkName, chunkId);
    		synchronized(MessageDispatcher.chunkToLocation) {
				while(!MessageDispatcher.chunkToLocation.containsKey(chunkName)) {
					try {
						MessageDispatcher.chunkToLocation.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				List<Messages.StorageNode> copy = (List<Messages.StorageNode>)
						MessageDispatcher.chunkToLocation.remove(chunkName);
				MessageDispatcher.chunkToLocation.notifyAll();
				controllerClientProxy.disconnect();
				return copy;
			}
    	});
    }
    
}
