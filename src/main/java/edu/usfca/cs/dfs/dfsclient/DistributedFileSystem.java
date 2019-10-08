package edu.usfca.cs.dfs.dfsclient;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.StorageNode;
import edu.usfca.cs.dfs.utils.Constants;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

public class DistributedFileSystem {
	
	private static int chunkSize = Constants.CHUNK_SIZE;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
	private Map<String, Integer> fileToChunkMap = new HashMap<>();
	private static String CHUNK_SUFFIX = "_chunk";

    public DistributedFileSystem() {}

    public boolean put(String filename) throws IOException, InterruptedException, ExecutionException {
    	List<Future<Messages.StorageFeedback>> storageFeedbacks = new LinkedList<>();
    	Path path = Paths.get(filename);
    	int count = (int) Files.size(path) / chunkSize;
        int i = 0;
    	ControllerClientProxy controllerClient = new ControllerClientProxy();
		@SuppressWarnings("resource")
		RandomAccessFile aFile = new RandomAccessFile(filename, "r");
    	FileChannel inChannel = aFile.getChannel();
    	for(i=0; i<count; i++) {
    		sendData(inChannel, filename, i, chunkSize, controllerClient, storageFeedbacks);
    	}
        int restBuffer =  (int) (Files.size(path) % chunkSize);
        if(restBuffer != 0) {
        	sendData(inChannel, filename, i, restBuffer, controllerClient, storageFeedbacks);
        	fileToChunkMap.put(filename, count+1);
        } else {
        	fileToChunkMap.put(filename, count);
        }
        for(Future<Messages.StorageFeedback> feedback : storageFeedbacks) {
        	if(!feedback.get().getIsStored()) {
        		controllerClient.disconnect();
        		return false;
        	}
        }
        controllerClient.disconnect();
        return true;
    }
    
    private void sendData(FileChannel inChannel, String filename, int i, int chunkSize, ControllerClientProxy client, List<Future<Messages.StorageFeedback>> storageFeedbacks) throws IOException, InterruptedException, ExecutionException {
    	ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
		inChannel.read(buffer);
		byte[] data = new byte[chunkSize];
		buffer.flip();
		buffer.get(data);
		String chunkedFileName = filename + CHUNK_SUFFIX + i;
		client.getStorageLocations(chunkedFileName);
        List<Messages.StorageNode> locations = getStorageNodes().get();
        storageFeedbacks.add(storeInStorage(chunkedFileName, i, ByteString.copyFrom(data), locations));
		buffer.clear();
    }
    
    private Future<List<Messages.StorageNode>> getStorageNodes() {
		return threadPool.submit(() -> {
			synchronized(MessageDispatcher.locations) {
				if(MessageDispatcher.locations.isEmpty()) {
					MessageDispatcher.locations.wait();
				}
				List<Messages.StorageNode> copy = MessageDispatcher.locations;
				MessageDispatcher.locations = new LinkedList<Messages.StorageNode>();
				return copy;
			}
        });
	}
    
    private Future<Messages.StorageFeedback> storeInStorage(String filename, int id, ByteString data, List<Messages.StorageNode> locations) {
		return threadPool.submit(() -> {
			Messages.StorageNode location = locations.get(0);
			Messages.StoreChunk chunk = Messages.StoreChunk.newBuilder().setFileName(filename).setChunkId(id).setData(data).addAllStorageLocations(locations).build();
			Messages.StorageFeedback feedback = getStorageFeedback(location, chunk).get();
			System.out.println("File: " + filename + " isStored: " + feedback.getIsStored());
			return feedback;
		});
	}
    
    private Future<Messages.StorageFeedback> getStorageFeedback(Messages.StorageNode location, Messages.StoreChunk chunk) {
    	return threadPool.submit(() -> {
    		StorageClientProxy storageClient = new StorageClientProxy(location.getHost(), location.getPort());
			storageClient.upload(chunk);
			synchronized(MessageDispatcher.storageFeedback) {
				while(!MessageDispatcher.storageFeedback.containsKey(chunk.getFileName())) {
					MessageDispatcher.storageFeedback.wait();
				}
				Messages.StorageFeedback feedback = MessageDispatcher.storageFeedback.remove(chunk.getFileName());
				MessageDispatcher.storageFeedback.notifyAll();
				storageClient.disconnect();
				return feedback;
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
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
		List<Future<Messages.DownloadFile>> writeTaskCallbacks = new LinkedList<>();
		for(int i=0; i < chunkCount; i++) {
    		String chunkName = filename + CHUNK_SUFFIX + i;
    		List<Messages.StorageNode> locations = getStoredNodes(chunkName, i).get();
    		writeTaskCallbacks.add(getDataFromLocations(locations, chunkName, i));	
    	}
		try(BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.ISO_8859_1)) {
			for(Future<Messages.DownloadFile> callback : writeTaskCallbacks) {
				Messages.DownloadFile downloadedChunk = callback.get();
				if(downloadedChunk == null) {
					return false;
				}
				String data = downloadedChunk.getStoreChunk().getData().toString(StandardCharsets.ISO_8859_1);
				writer.write(data, 0, data.length());
			}
		}
		return true;
    }
    
    private Future<Messages.DownloadFile> getDataFromLocations(List<Messages.StorageNode> locations, String chunkName, int chunkIndex) {
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
					MessageDispatcher.chunkToData.wait();
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
					MessageDispatcher.chunkToLocation.wait();
				}
				List<Messages.StorageNode> copy = (List<Messages.StorageNode>)
						MessageDispatcher.chunkToLocation.remove(chunkName);
				MessageDispatcher.chunkToLocation.notifyAll();
				controllerClientProxy.disconnect();
				return copy;
			}
    	});
    }
    
    public List<Messages.StorageNode> getActiveNodes() throws InterruptedException, ExecutionException {
    	ControllerClientProxy clientControllerProxy = new ControllerClientProxy();
    	clientControllerProxy.getActiveNodes();
    	Future<List<Messages.StorageNode>> storageNodeList = threadPool.submit(() -> {
    		synchronized(MessageDispatcher.activeNodes) {
    			if(MessageDispatcher.activeNodes.isEmpty()) {
    				MessageDispatcher.activeNodes.wait();
        		}
    			List<StorageNode> activeNodes = MessageDispatcher.activeNodes;
    			MessageDispatcher.activeNodes = new LinkedList<>();
    			clientControllerProxy.disconnect();
    			return activeNodes;
    		}
    	});
    	return storageNodeList.get();
    }
    
    public long getTotalDiskspace() throws InterruptedException, ExecutionException {
    	ControllerClientProxy clientControllerProxy = new ControllerClientProxy();
    	clientControllerProxy.getTotalDiskspace();
    	Future<Long> totalDiskspace = threadPool.submit(() -> {
    		synchronized(MessageDispatcher.totalDiskspace) {
    			if(MessageDispatcher.totalDiskspace.get() == -1) {
    				MessageDispatcher.totalDiskspace.wait();
        		}
    			Long activeNodes = MessageDispatcher.totalDiskspace.get();
    			MessageDispatcher.totalDiskspace.set(-1);
    			clientControllerProxy.disconnect();
    			return activeNodes;
    		}
    	});
    	return totalDiskspace.get();
    }
    
    public Map<Messages.StorageNode, Long> getRequestsServed() throws InterruptedException, ExecutionException {
    	ControllerClientProxy clientControllerProxy = new ControllerClientProxy();
    	clientControllerProxy.getProcessedRequest();
    	Future<List<Messages.RequestPerNode>> totalRequestServed = threadPool.submit(() -> {
    		synchronized(MessageDispatcher.requestsServed) {
    			if(MessageDispatcher.requestsServed.isEmpty()) {
    				MessageDispatcher.requestsServed.wait();
        		}
    			List<Messages.RequestPerNode> requestsServed = MessageDispatcher.requestsServed;
    			MessageDispatcher.requestsServed = new LinkedList<>();
    			clientControllerProxy.disconnect();
    			return requestsServed;
    		}
    	});
    	List<Messages.RequestPerNode> requestsPerNode = totalRequestServed.get();
    	Map<Messages.StorageNode, Long> requestServedPerNode = new HashMap<>();
    	for(Messages.RequestPerNode requestPerNode : requestsPerNode) {
    		requestServedPerNode.put(requestPerNode.getNode(), requestPerNode.getRequests());
    	}
    	return requestServedPerNode;
    }
}
