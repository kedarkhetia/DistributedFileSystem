package edu.usfca.cs.dfs.dfsclient;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.StorageNode;
import edu.usfca.cs.dfs.utils.Constants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

/**
 * Business logic for DFS Client.
 * @author kedarkhetia
 *
 */
public class DistributedFileSystem {
	private final static Logger log = LogManager.getLogger(DistributedFileSystem.class);
	
	private int chunkSize = Constants.CHUNK_SIZE_BYTES * Client.config.getChunkSize();
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
	private static String CHUNK_SUFFIX = "_chunk";

    public DistributedFileSystem() {}

    /**
     * Chunks and puts the data in Distributed File System.
     * @param filename
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public synchronized boolean put(String filename) throws IOException, InterruptedException, ExecutionException {
    	log.info("Got put request for file: " + filename);
    	List<Future<Messages.StorageFeedback>> storageFeedbacks = new LinkedList<>();
    	Path path = Paths.get(filename);
    	int count = (int) (Files.size(path) / chunkSize);
    	int carry = (int) (Files.size(path) % chunkSize) == 0 ? 0 : 1;
    	log.info("File divided in " + (count+carry) + " chunks.");
        int i = 0;
    	ControllerClientProxy controllerClient = new ControllerClientProxy(Client.config.getControllerHost(), 
    			Client.config.getControllerPort(), Client.config.getChunkSize());
		RandomAccessFile rFile = new RandomAccessFile(filename, "r");
    	for(i=0; i<count; i++) {
    		sendData(rFile, filename, i, chunkSize, count+carry, controllerClient, storageFeedbacks);
    	}
        int restBuffer =  (int) (Files.size(path) % chunkSize);
        if(restBuffer != 0) {
        	sendData(rFile, filename, i, restBuffer, count+carry, controllerClient, storageFeedbacks);
        }
        for(Future<Messages.StorageFeedback> feedback : storageFeedbacks) {
        	if(!feedback.get().getIsStored()) {
        		controllerClient.disconnect();
        		log.info("Storage failed for " + feedback.get().getFilename());
        		return false;
        	}
        }
        rFile.close();
        controllerClient.disconnect();
        return true;
    }
    
    /**
     * Reads data in buffer, creates data message, get locations from 
     * controller and sends the data to storage node.
     * @param inChannel
     * @param filename
     * @param i
     * @param size
     * @param totalChunks
     * @param client
     * @param storageFeedbacks
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private synchronized void sendData(RandomAccessFile rFile, String filename, int i, int size, int totalChunks,
    		ControllerClientProxy client, List<Future<Messages.StorageFeedback>> storageFeedbacks) 
    				throws IOException, InterruptedException, ExecutionException {
    	byte[] data = new byte[size];
    	rFile.read(data);
		String chunkedFileName = filename + CHUNK_SUFFIX + i;
		Messages.Data.Builder dataBuilder = Messages.Data.newBuilder();
		dataBuilder.setData(ByteString.copyFrom(data));
		dataBuilder.setSize(size);
		if(i == 0) {
			dataBuilder.setChunks(totalChunks);
		}
		client.getStorageLocations(chunkedFileName);
        List<Messages.StorageNode> locations = filterStorageNodes(getStorageNodes().get(), chunkedFileName);
        log.info("Storing chunk: " + chunkedFileName + " at " + locations);
        storageFeedbacks.add(storeInStorage(chunkedFileName, dataBuilder.build(), locations));
    }
    
    private List<Messages.StorageNode> filterStorageNodes(List<Messages.StorageNode> nodes, String chunkName) throws InterruptedException, ExecutionException {
    	List<Messages.StorageNode> locations = new LinkedList<>();
    	for(int i=0; i<nodes.size(); i++) {
    		Messages.DownloadFile file = getStoredData(chunkName, Messages.StoredLocationType.newBuilder()
        			.setLocation(nodes.get(i))
        			.setStorageType(Messages.StorageType.PRIMARY)
        		.build()).get();
    		if(file.hasStoreChunk()) {
    			locations.add(nodes.get(i));
    			nodes.remove(i);
    			break;
    		}
    	}
    	for(int i=0; i<nodes.size(); i++) {
    		Messages.DownloadFile file = getStoredData(chunkName, Messages.StoredLocationType.newBuilder()
        			.setLocation(nodes.get(i))
        			.setStorageType(Messages.StorageType.REPLICA)
        		.build()).get();
    		if(file.hasStoreChunk()) {
    			locations.add(nodes.get(i));
    			nodes.remove(i);
    		}
    		if(locations.size() == Client.config.getReplicaCount()) {
    			return locations;
    		}
    	}
    	while(nodes.size() != 0 && locations.size() < Client.config.getReplicaCount()) {
    		locations.add(nodes.get(0));
    		nodes.remove(0);
    	}
    	log.info("Final List of Nodes: " + locations);
    	return locations;
    }
    
    /**
     * Reads location data from message dispatcher. 
     * @return
     */
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
    
    /**
     * Prepares chunk to send to storage.
     * @param filename
     * @param data
     * @param locations
     * @return
     */
    private synchronized Future<Messages.StorageFeedback> storeInStorage(String filename, Messages.Data data, 
    		List<Messages.StorageNode> locations) {
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
			return feedback;
		});
	}
    
    /**
     * Sends chunk to storage and gets back feedback of storage.
     * @param location
     * @param chunk
     * @return
     */
    private Future<Messages.StorageFeedback> getStorageFeedback(Messages.StorageNode location, Messages.StoreChunk chunk) {
    	return threadPool.submit(() -> {
    		StorageClientProxy storageClient = new StorageClientProxy(location.getHost(), location.getPort(), Client.config.getChunkSize());
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
    
    /**
     * Terminates threadpool.
     * @throws InterruptedException
     */
    public synchronized void close() throws InterruptedException {
    	threadPool.shutdown();
    	while (!threadPool.awaitTermination(24, TimeUnit.HOURS)) {
    	    log.info("Awaiting Distributed File System Termination!");
    	}
    	log.info("DSF Shutdown Successfully!");
    }
    
    /**
     * Get data from Distributed File System based on filename.
     * @param storagePath
     * @param filename
     * @return
     * @throws InterruptedException
     * @throws IOException
     * @throws ExecutionException
     */
    public synchronized boolean get(String storagePath, String filename) throws InterruptedException, IOException, ExecutionException {
    	log.info("Getting data for " + filename);
    	int chunkCount = 1;
    	Path path = Paths.get(storagePath+filename);
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
		for(int i=0; i < chunkCount; i++) {
    		String chunkName = filename + CHUNK_SUFFIX + i;
    		List<Messages.StoredLocationType> storedLocationTypes = getStoredNodes(chunkName).get();
    		Future<Integer> chunkCountFuture = getDataFromLocations(storedLocationTypes, chunkName, 
    				i, storagePath+filename);
    		if(chunkCountFuture.get() != null && chunkCountFuture.get() == -1) {
    			log.info("Cannot get data from storageLocations.");
				return false;
			}
    		if(i == 0) {
    			chunkCount = chunkCountFuture.get();
    		}
    	}
		return true;
    }
    
    /**
     * Stores data received from storage locations.
     * @param storedLocationType
     * @param chunkName
     * @param chunkIndex
     * @param path
     * @return
     */
    private Future<Integer> getDataFromLocations(List<Messages.StoredLocationType> storedLocationType, 
    		String chunkName, int chunkIndex, String path) {
    	return threadPool.submit(() -> {
    		Messages.DownloadFile downloadedChunk = null;
    		int j = 0;
    		for(j=0; j<storedLocationType.size(); j++) {
    			downloadedChunk = getStoredData(chunkName, storedLocationType.get(j)).get(); 
    			if(downloadedChunk.getFileFound()) {
    				RandomAccessFile file = new RandomAccessFile(path, "rw");
    				file.seek((long)chunkIndex * chunkSize);
    				FileChannel inChannel = file.getChannel();
    				byte[] data = downloadedChunk.getStoreChunk().getData().getData().toByteArray();
    				int length = downloadedChunk.getStoreChunk().getData().getSize();
    				inChannel.write(ByteBuffer.wrap(data, 0, length));
    				file.close();
    				break;
    			}
    		}
    		if(j == storedLocationType.size()) 
    			return -1;
    		if(downloadedChunk != null && chunkIndex == 0) 
    			return downloadedChunk.getStoreChunk().getData().getChunks();
    		return null;
    	});
    }
    
    /**
     * Request storage location for data.
     * @param chunkName
     * @param storedLocationType
     * @return
     */
    private Future<Messages.DownloadFile> getStoredData(String chunkName, Messages.StoredLocationType storedLocationType) {
    	return threadPool.submit(() -> {
    		Messages.StorageNode node = storedLocationType.getLocation();
    		StorageClientProxy storageClientProxy = new StorageClientProxy(node.getHost(), 
					node.getPort(), Client.config.getChunkSize());
			storageClientProxy.download(Messages.UploadFile.newBuilder()
					.setFilename(chunkName)
					.setStorageType(storedLocationType.getStorageType())
					.setStorageNode(node)
					.setNodeType(Messages.NodeType.CLIENT)
					.build());
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
    
    /**
     * Requests controller to get probable storage locations.
     * @param chunkName
     * @return
     */
    private Future<List<Messages.StoredLocationType>> getStoredNodes(String chunkName) {
    	return threadPool.submit(() -> {
    		ControllerClientProxy controllerClientProxy = new ControllerClientProxy(Client.config.getControllerHost(), 
        			Client.config.getControllerPort(), Client.config.getChunkSize());
    		controllerClientProxy.getStoredLocations(chunkName, Messages.NodeType.CLIENT);
    		synchronized(MessageDispatcher.chunkToLocation) {
				while(!MessageDispatcher.chunkToLocation.containsKey(chunkName)) {
					MessageDispatcher.chunkToLocation.wait();
				}
				List<Messages.StoredLocationType> copy = (List<Messages.StoredLocationType>)
						MessageDispatcher.chunkToLocation.remove(chunkName);
				MessageDispatcher.chunkToLocation.notifyAll();
				controllerClientProxy.disconnect();
				return copy;
			}
    	});
    }
    
    /**
     * Gets the list of active nodes from controller.
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public synchronized List<Messages.StorageNode> getActiveNodes() throws InterruptedException, ExecutionException {
    	ControllerClientProxy clientControllerProxy = new ControllerClientProxy(Client.config.getControllerHost(), 
    			Client.config.getControllerPort(), Client.config.getChunkSize());
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
    
    /**
     * Gets total diskspace in the system.
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public synchronized long getTotalDiskspace() throws InterruptedException, ExecutionException {
    	ControllerClientProxy clientControllerProxy = new ControllerClientProxy(Client.config.getControllerHost(), 
    			Client.config.getControllerPort(), Client.config.getChunkSize());
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
    
    /**
     * Gets number of request served by each node.
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public synchronized Map<Messages.StorageNode, Long> getRequestsServed() throws InterruptedException, ExecutionException {
    	ControllerClientProxy clientControllerProxy = new ControllerClientProxy(Client.config.getControllerHost(), 
    			Client.config.getControllerPort(), Client.config.getChunkSize());
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
