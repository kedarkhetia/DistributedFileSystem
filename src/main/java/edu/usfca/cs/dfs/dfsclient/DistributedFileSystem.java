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

import com.google.protobuf.ByteString;

public class DistributedFileSystem {
	
	private static int chunkSize = Constants.CHUNK_SIZE;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
	private static String CHUNK_SUFFIX = "_chunk";
	private static int TIME_OUT = 3000;

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
    		sendData(inChannel, filename, i, chunkSize, count, controllerClient, storageFeedbacks);
    	}
        int restBuffer =  (int) (Files.size(path) % chunkSize);
        if(restBuffer != 0) {
        	sendData(inChannel, filename, i, restBuffer, count+1, controllerClient, storageFeedbacks);
        }
        for(Future<Messages.StorageFeedback> feedback : storageFeedbacks) {
        	if(!feedback.get().getIsStored()) {
        		controllerClient.disconnect();
        		return false;
        	}
        }
        aFile.close();
        controllerClient.disconnect();
        return true;
    }
    
    private void sendData(FileChannel inChannel, String filename, int i, int chunkSize, int totalChunks,
    		ControllerClientProxy client, List<Future<Messages.StorageFeedback>> storageFeedbacks) 
    				throws IOException, InterruptedException, ExecutionException {
    	ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
		inChannel.read(buffer);
		byte[] data = new byte[chunkSize];
		buffer.flip();
		buffer.get(data);
		String chunkedFileName = filename + CHUNK_SUFFIX + i;
		Messages.Data.Builder dataBuilder = Messages.Data.newBuilder();
		dataBuilder.setData(ByteString.copyFrom(data));
		if(i == 0) {
			dataBuilder.setChunks(totalChunks);
		}
		client.getStorageLocations(chunkedFileName);
        List<Messages.StorageNode> locations = getStorageNodes().get();
        storageFeedbacks.add(storeInStorage(chunkedFileName, dataBuilder.build(), locations));
		buffer.clear();
    }
    
    private Future<List<Messages.StorageNode>> getStorageNodes() {
		return threadPool.submit(() -> {
			synchronized(MessageDispatcher.locations) {
				if(MessageDispatcher.locations.isEmpty()) {
					MessageDispatcher.locations.wait(TIME_OUT);
				}
				List<Messages.StorageNode> copy = MessageDispatcher.locations;
				MessageDispatcher.locations = new LinkedList<Messages.StorageNode>();
				return copy;
			}
        });
	}
    
    private Future<Messages.StorageFeedback> storeInStorage(String filename, Messages.Data data, List<Messages.StorageNode> locations) {
		return threadPool.submit(() -> {
			Messages.StorageNode primary = locations.get(0);
			locations.remove(0);
			Messages.StoreChunk chunk = Messages.StoreChunk.newBuilder()
					.setFileName(filename)
					.setData(data)
					.setPrimary(primary)
					.addAllReplicas(locations)
					.setStorageType(Messages.StorageType.PRIMARY)
					.build();
			Messages.StorageFeedback feedback = getStorageFeedback(primary, chunk).get();
			//System.out.println("File: " + filename + " isStored: " + feedback.getIsStored());
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
    	int chunkCount = 1;
    	Path path = Paths.get(storagePath+filename);
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
		List<Future<Messages.DownloadFile>> writeTaskCallbacks = new LinkedList<>();
		for(int i=0; i < chunkCount; i++) {
    		String chunkName = filename + CHUNK_SUFFIX + i;
    		List<Messages.StoredLocationType> storedLocationType = getStoredNodes(chunkName).get();
    		Future<Messages.DownloadFile> downloadFileFuture = getDataFromLocations(storedLocationType, chunkName, i);
    		if(i == 0) {
    			if(downloadFileFuture.get() == null) {
    				return false;
    			}
    			chunkCount = downloadFileFuture.get().getStoreChunk().getData().getChunks();
    		}
    		writeTaskCallbacks.add(downloadFileFuture);	
    	}
		try(BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.ISO_8859_1)) {
			for(Future<Messages.DownloadFile> callback : writeTaskCallbacks) {
				Messages.DownloadFile downloadedChunk = callback.get();
				if(downloadedChunk == null) {
					return false;
				}
				String data = downloadedChunk.getStoreChunk().getData().getData().toString(StandardCharsets.ISO_8859_1);
				writer.write(data, 0, data.length());
			}
		}
		return true;
    }
    
    private Future<Messages.DownloadFile> getDataFromLocations(List<Messages.StoredLocationType> storedLocationType, String chunkName, int chunkIndex) {
    	return threadPool.submit(() -> {
    		//System.out.println("----------------------");
    		//System.out.println(storedLocationType);
    		for(int j=0; j<storedLocationType.size(); j++) {
    			//System.out.println(storedLocationType.get(j).getStorageType());
    			Messages.DownloadFile downloadedChunk = getStoredData(chunkName, storedLocationType.get(j)).get(); 
    			if(downloadedChunk.getFileFound()) {
    				return downloadedChunk;
    			}
    		}
    		return null;
    	});
    }
    
    private Future<Messages.DownloadFile> getStoredData(String chunkName, Messages.StoredLocationType storedLocationType) {
    	return threadPool.submit(() -> {
    		Messages.StorageNode node = storedLocationType.getLocation();
    		StorageClientProxy storageClientProxy = new StorageClientProxy(node.getHost(), 
					node.getPort());
			storageClientProxy.download(Messages.UploadFile.newBuilder()
					.setFilename(chunkName)
					.setStorageType(storedLocationType.getStorageType())
					.setStorageNode(node)
					.setNodeType(Messages.NodeType.CLIENT)
					.build());
    		synchronized(MessageDispatcher.chunkToData) {
				while(!MessageDispatcher.chunkToData.containsKey(chunkName)) {
					MessageDispatcher.chunkToData.wait(TIME_OUT);
				}
				Messages.DownloadFile data = MessageDispatcher.chunkToData.remove(chunkName);
				//System.out.println(chunkName);
				storageClientProxy.disconnect();
				MessageDispatcher.chunkToData.notifyAll();
				return data;
			}
    	});
    }
    
    private Future<List<Messages.StoredLocationType>> getStoredNodes(String chunkName) {
    	return threadPool.submit(() -> {
    		ControllerClientProxy controllerClientProxy = new ControllerClientProxy();
    		controllerClientProxy.getStoredLocations(chunkName, Messages.NodeType.CLIENT);
    		synchronized(MessageDispatcher.chunkToLocation) {
				while(!MessageDispatcher.chunkToLocation.containsKey(chunkName)) {
					MessageDispatcher.chunkToLocation.wait(TIME_OUT);
				}
				List<Messages.StoredLocationType> copy = (List<Messages.StoredLocationType>)
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
    				MessageDispatcher.activeNodes.wait(TIME_OUT);
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
    				MessageDispatcher.totalDiskspace.wait(TIME_OUT);
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
    				MessageDispatcher.requestsServed.wait(TIME_OUT);
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
