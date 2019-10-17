package edu.usfca.cs.dfs.storage;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.luben.zstd.Zstd;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.Data;
import edu.usfca.cs.dfs.utils.Constants;

public class StorageHandlers {
	private final static Logger log = LogManager.getLogger(StorageHandlers.class);
	
	private static long processedRequest = 0;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
	private static Messages.StorageNode selfNode;
	private static int TIME_OUT = 3000;
	
	/**
	 * Stores store chunk data to storage node. 
	 * It also determines path (like checksum and compressed) 
	 * to store the data.
	 * @param chunk
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public static synchronized Messages.ProtoMessage store(Messages.StoreChunk chunk) 
			throws InterruptedException, ExecutionException, IOException, NoSuchAlgorithmException {
		Messages.StorageType storageType = chunk.getStorageType();
		log.info("Filename: " + chunk.getFileName());
		String pathString = Storage.config.getStoragePath();
		Messages.StorageNode primary = chunk.getPrimary();
		List<Messages.StorageNode> locations = chunk.getReplicasList();
		Messages.StorageNode location = primary;
		if(storageType == Messages.StorageType.PRIMARY) {
			if(selfNode == null) selfNode = location;
			pathString += location.getHost() + location.getPort() + "/";
		}
		if(storageType == Messages.StorageType.REPLICA) {
			location = locations.get(0);
			pathString += location.getHost() + location.getPort() + "/";
			locations = new LinkedList<>(locations);
			locations.remove(location);
			pathString += Constants.REPLICA_PATH + "/" + primary.getHost() + primary.getPort() + "/";
		}
		boolean compressFlag = canCompress(chunk.getData().toByteArray());
		byte[] data = chunk.getData().toByteArray();
		if(compressFlag) {
			pathString += Constants.COMPRESSED_PATH + "/";
			data = compress(chunk.getData().toByteArray());
		}
		storeData(pathString, chunk, data);
		Messages.StorageFeedback feedback = getStorageFeedback(chunk, location).get();
		processedRequest++;
		if(!locations.isEmpty()) {
			replicate(locations, chunk, primary);
		}
		if(storageType == Messages.StorageType.PRIMARY && chunk.getNodeType() == Messages.NodeType.CLIENT) {
			return getFeedback(feedback);
		}
		return null;
	}
	
	/**
	 * Writes the data to file system.
	 * @param pathString
	 * @param chunk
	 * @param data
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	private static synchronized void storeData(String pathString, Messages.StoreChunk chunk, byte[] data) throws IOException, NoSuchAlgorithmException {
		Path path = Paths.get(pathString+chunk.getFileName());
		createFilesAndDirs(path);
		Path checksumPath = Paths.get(pathString + Constants.CHECKSUM_PATH + "/" + chunk.getFileName() + Constants.CHECKSUM_SUFFIX);
		createFilesAndDirs(checksumPath);
		byte[] checksum = checksum(data);
		Files.write(checksumPath, checksum);
		Files.write(path, data);
	}
	
	/**
	 * replicate data to other storage nodes.
	 * @param locations
	 * @param chunk
	 * @param primary
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static synchronized void replicate(List<Messages.StorageNode> locations, Messages.StoreChunk chunk, 
			Messages.StorageNode primary) throws InterruptedException, ExecutionException {
		Messages.StorageNode location = locations.get(0);
		log.info("Forwarding data to: " + locations);
		Messages.StoreChunk newChunk = Messages.StoreChunk.newBuilder()
			.setData(chunk.getData())
			.setFileName(chunk.getFileName())
			.setPrimary(primary)
			.addAllReplicas(locations)
			.setStorageType(Messages.StorageType.REPLICA)
		.build();
		sendToReplicas(newChunk, location);
	}
	
	/**
	 * Creates files and directories if it doen't exist.
	 * @param path
	 * @throws IOException
	 */
	private static synchronized void createFilesAndDirs(Path path) throws IOException {
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
	}
	
	/**
	 * Prepares feedback message.
	 * @param feedback
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static synchronized Messages.ProtoMessage getFeedback(Messages.StorageFeedback feedback) throws InterruptedException, ExecutionException {
			return Messages.ProtoMessage.newBuilder()
					.setClient(Messages.Client.newBuilder()
							.setStorageFeedback(feedback)
							.build())
					.build();
	}
	
	/**
	 * Sends storage proof to controller and receives 
	 * acknowledgement from controller once their bloom
	 * filter is updated. 
	 * @param chunk
	 * @param location
	 * @return
	 */
	private static synchronized Future<Messages.StorageFeedback> getStorageFeedback(Messages.StoreChunk chunk,
			Messages.StorageNode location) {
		ControllerClientProxy controllerProxy = new ControllerClientProxy(Storage.config.getControllerHost(),
				Storage.config.getControllerPort(), Storage.config.getChunkSize());
		controllerProxy.sendStorageProof(chunk.getFileName(), chunk.getStorageType(), location);
		return threadPool.submit(() -> {
			synchronized(MessageDispatcher.storageFeedbackMap) {
				while(!MessageDispatcher.storageFeedbackMap.containsKey(chunk.getFileName())) {
					MessageDispatcher.storageFeedbackMap.wait(TIME_OUT);
				}
				Messages.StorageFeedback feedback = MessageDispatcher.storageFeedbackMap.remove(chunk.getFileName());
				MessageDispatcher.storageFeedbackMap.notifyAll();
				controllerProxy.disconnect();
				return feedback;
			}
		});
	}
	
	/**
	 * Send the store chunk to replicas.
	 * @param storeChunk
	 * @param location
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static synchronized void sendToReplicas(Messages.StoreChunk storeChunk, Messages.StorageNode location) 
			throws InterruptedException, ExecutionException {
		StorageClientProxy storageClientProxy = new StorageClientProxy(location.getHost(), location.getPort(), 
				Storage.config.getChunkSize());
		storageClientProxy.upload(storeChunk);
		log.info("Updated for node: " + location.getHost()+":"+location.getPort() + storeChunk.getFileName());
		storageClientProxy.disconnect();
	}
	
	/**
	 * Replicates the data if request is received from Contorller 
	 * to execute replication.
	 * @param replicate
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static synchronized void replicate(Messages.Replicate replicate) throws IOException, InterruptedException, ExecutionException {
		Messages.StorageNode forNode = replicate.getForNode();
		Messages.StorageNode fromNode = replicate.getFromNode();
		Messages.StorageNode toNode = replicate.getToNode();
		String directoryPath = Storage.config.getStoragePath() + fromNode.getHost() + fromNode.getPort() + "/";
		if(replicate.getStorageType() == Messages.StorageType.REPLICA) {
			String curDirPath = directoryPath + Constants.REPLICA_PATH + "/" + forNode.getHost() + forNode.getPort() + "/";
			String newDirPath = directoryPath + Constants.REPLICA_PATH + "/" + toNode.getHost() + toNode.getPort() + "/";
			File curDir = new File(curDirPath);
			File newDir = new File(newDirPath);
			curDir.renameTo(newDir);
			directoryPath = newDirPath;
		}
		File directory = new File(directoryPath);
		replicateAllFiles(directory, replicate);
	}
	
	/**
	 * Recursively replicates all the file to storage other storage node.
	 * Used when there a certain node failes and current node is either storing
	 * its replica or primary data.
	 * @param directory
	 * @param replicate
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static synchronized void replicateAllFiles(File directory, Messages.Replicate replicate) 
			throws IOException, InterruptedException, ExecutionException {
		if(directory == null || directory.listFiles() == null) return;
		for(File file : directory.listFiles()) {
			if(file.isDirectory()) {
				if(!file.getAbsolutePath().endsWith(Constants.CHECKSUM_PATH) && 
						!file.getAbsolutePath().endsWith(Constants.REPLICA_PATH))
						replicateAllFiles(file, replicate);
			}
			else {
				byte[] data = readFile(file);
				if(directory.getAbsolutePath().endsWith(Constants.COMPRESSED_PATH)) {
					data = decompress(data);
				}
				Messages.StorageType storageType = replicate.getStorageType();
				if(storageType == Messages.StorageType.PRIMARY) {
					replicatToReplica(replicate, file, data);
				}
				else {
					replicateToPrimary(replicate, file, data);
				}
			}
		}
	}
	
	/**
	 * Rplicates from current node's replica to destination node's primary.
	 * @param replicate
	 * @param file
	 * @param data
	 * @throws InvalidProtocolBufferException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static synchronized void replicateToPrimary(Messages.Replicate replicate, File file, byte[] data) throws InvalidProtocolBufferException, InterruptedException, ExecutionException {
		Messages.StorageType storageType = Messages.StorageType.PRIMARY;
		List<Messages.StorageNode> locations = new LinkedList<>();
		Messages.StoreChunk chunk = Messages.StoreChunk.newBuilder()
				.setData(Data.parseFrom(data))
				.setFileName(file.getName())
				.setPrimary(replicate.getToNode())
				.addAllReplicas(locations)
				.setStorageType(storageType)
				.setNodeType(replicate.getNodeType())
			.build();
		log.info("Replicating Replica to Primary for file: " + chunk.getFileName() + " from "
				+ replicate.getFromNode().getHost() + ":" + replicate.getFromNode().getPort() + " to "
						+ replicate.getToNode().getHost() + ":" + replicate.getToNode().getPort());
		sendToReplicas(chunk, replicate.getToNode());
	}
	
	/**
	 * Replicates data from current node's primary to destination node's replica.
	 * @param replicate
	 * @param file
	 * @param data
	 * @throws InvalidProtocolBufferException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static synchronized void replicatToReplica(Messages.Replicate replicate, File file, byte[] data) 
			throws InvalidProtocolBufferException, InterruptedException, ExecutionException {
		Messages.StorageType storageType = Messages.StorageType.REPLICA;
		List<Messages.StorageNode> locations = new LinkedList<>();
		locations.add(replicate.getToNode());
		Messages.StoreChunk chunk = Messages.StoreChunk.newBuilder()
				.setData(Data.parseFrom(data))
				.setFileName(file.getName())
				.setPrimary(replicate.getFromNode())
				.addAllReplicas(locations)
				.setStorageType(storageType)
				.setNodeType(replicate.getNodeType())
			.build();
		log.info("Replicating Primary to Replica for file: " + chunk.getFileName() + " from "
				+ replicate.getFromNode().getHost() + ":" + replicate.getFromNode().getPort() + " to "
						+ replicate.getToNode().getHost() + ":" + replicate.getToNode().getPort());
		sendToReplicas(chunk, replicate.getToNode());
	}
	
	/**
	 * Generates the checksum for current file.
	 * @param data
	 * @return
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	private static synchronized byte[] checksum(byte[] data) throws IOException, NoSuchAlgorithmException {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		byte[] checksum = digest.digest(data);
		return checksum;
	}
	
	/**
	 * Compresses the data.
	 * @param data
	 * @return
	 */
	private static synchronized byte[] compress(byte[] data) {
		return Zstd.compress(data, Constants.COMPRESS_LEVEL);
	}
	
	/**
	 * Decompresses data.
	 * @param data
	 * @return
	 */
	private static synchronized byte[] decompress(byte[] data) {
		long decompressedSize = Zstd.decompressedSize(data);
		return Zstd.decompress(data, (int) decompressedSize);
	}
	
	/**
	 * checks if it is feasible to compress the data or not.
	 * @param input
	 * @return
	 */
	private static synchronized boolean canCompress(byte[] input) {
        if (input.length == 0) {
            return false;
        }
        int[] charCounts = new int[256];
        for (byte b : input) {
            charCounts[b & 0xFF]++;
        }
        double entropy = 0.0;
        for (int i = 0; i < 256; ++i) {
            if (charCounts[i] == 0.0) {
                continue;
            }

            double freq = (double) charCounts[i] / input.length;
            entropy -= freq * (Math.log(freq) / Math.log(2));
        }
        return (1d - (entropy / 8)) > 0.6d;
    }
	
	/**
	 * Start sending heartbeat to contorller node.
	 * @param selfHostName
	 * @param selfPort
	 * @throws InterruptedException
	 */
	public static synchronized void startHeartbeat(String selfHostName, int selfPort) throws InterruptedException {
		Thread heartbeatThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while(true) {
					ControllerClientProxy contorllerClientProxy = new ControllerClientProxy(Storage.config.getControllerHost(),
							Storage.config.getControllerPort(), Storage.config.getChunkSize());
					try {
						contorllerClientProxy.sendHeartbeat(Files.getFileStore(Paths.get(Storage.config.getStoragePath())).getUsableSpace(), 
								processedRequest, Messages.StorageNode.newBuilder()
									.setHost(selfHostName)
									.setPort(selfPort)
									.build());
						Thread.sleep(Constants.HEARTBEAT_INTERVAL);
					} catch (IOException | InterruptedException e) {
						log.error("Exception occured in startHeartbeat: " + e);
					}
					contorllerClientProxy.disconnect();
				}
			}
		});
		heartbeatThread.start();
	}
	
	/**
	 * Get storageFile path.
	 * @param directory
	 * @param filename
	 * @return
	 */
	private static synchronized File getFilePath(File directory, String filename) {
		File file = new File(directory, filename);
		if(file.exists()) {
			return directory;
		}
		File[] subdirs = directory.listFiles(new FileFilter() {
		    public boolean accept(File file) {
		        return file.isDirectory();
		    }
		});
		if(subdirs == null) return null;
		for(File subdir : subdirs) {
			File newfile = getFilePath(subdir, filename);
			if(newfile != null) {
				return newfile;
			}
		}
		return null;
	}
	
	/**
	 * Retrive data from current sotrage node.
	 * @param uploadFile
	 * @return
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static synchronized Messages.ProtoMessage retrive(Messages.UploadFile uploadFile) 
			throws IOException, NoSuchAlgorithmException, InterruptedException, ExecutionException {
		String filePath = Storage.config.getStoragePath() + uploadFile.getStorageNode().getHost()
				+ uploadFile.getStorageNode().getPort() + "/";
		if(uploadFile.getStorageType() == Messages.StorageType.REPLICA) {
			filePath += Constants.REPLICA_PATH + "/";
		}
		File directory = new File(filePath);
		log.info("Retriving file: " + uploadFile.getFilename() + " at location: " + directory.getAbsolutePath());
		directory = getFilePath(directory, uploadFile.getFilename());
		if(directory == null) {
			return fileNotFound(uploadFile);
		}
		File file = new File(directory, uploadFile.getFilename());
		byte[] data = readFile(file);
		if(!verifyChecksum(directory, uploadFile.getFilename(), data)) {
			log.info("Calling replication for filename: " + uploadFile.getFilename());
			fixStorage(uploadFile.getFilename(), directory);
			return fileNotFound(uploadFile);
		}
		if(directory.getAbsolutePath().endsWith(Constants.COMPRESSED_PATH)) {
			data = decompress(data);
		}
		processedRequest++;
		return sendFile(data, uploadFile);
	}
	
	/**
	 * Fix corrupted files by requesting it from replica nodes. 
	 * @param chunkName
	 * @param directory
	 */
	private static synchronized void fixStorage(String chunkName, File directory) {
		threadPool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					log.info("Fixing file: " + chunkName);
					List<Messages.StoredLocationType> storageLocationType = getStoredNodes(chunkName).get();
					for(Messages.StoredLocationType node : storageLocationType) {
						if(!(node.getLocation().getHost().equals(selfNode.getHost()) && node.getLocation().getPort() == selfNode.getPort())) {
							Messages.DownloadFile downloadedFile = getStoredData(chunkName, node).get();
							if(downloadedFile.getFileFound()) {
								byte[] data = downloadedFile.getStoreChunk().getData().toByteArray();
								if(directory.getAbsolutePath().endsWith(Constants.COMPRESSED_PATH)) {
									data = compress(data);
								}
								Path path = Paths.get(directory.getAbsolutePath() + "/" + chunkName);
								createFilesAndDirs(path);
								Path checksumPath = Paths.get(directory.getAbsolutePath() + "/" + Constants.CHECKSUM_PATH + "/" + chunkName + Constants.CHECKSUM_SUFFIX);
								createFilesAndDirs(checksumPath);
								byte[] checksum = checksum(data);
								Files.write(path, data);
								Files.write(checksumPath, checksum);
								return;
							}
						}
					}
				} catch (InterruptedException | ExecutionException | NoSuchAlgorithmException | IOException e) {
					log.info("Exception occured in fixStorage: " + e);
				}
			}
		});
	}
	
	/**
	 * Get stored data from replica storage node.
	 * @param chunkName
	 * @param storedLocationType
	 * @return
	 */
	private static synchronized Future<Messages.DownloadFile> getStoredData(String chunkName, Messages.StoredLocationType storedLocationType) {
    	return threadPool.submit(() -> {
    		Messages.StorageNode node = storedLocationType.getLocation();
    		StorageClientProxy storageClientProxy = new StorageClientProxy(node.getHost(), 
					node.getPort(), Storage.config.getChunkSize());
			storageClientProxy.download(Messages.UploadFile.newBuilder()
					.setFilename(chunkName)
					.setStorageType(storedLocationType.getStorageType())
					.setStorageNode(node)
					.setNodeType(Messages.NodeType.STORAGE)
					.build());
    		synchronized(MessageDispatcher.downloadFileBuilder) {
    			if(MessageDispatcher.downloadFileBuilder.build().getStoreChunk().getFileName().isEmpty()) {
					MessageDispatcher.downloadFileBuilder.wait(TIME_OUT);
				}
				Messages.DownloadFile data = MessageDispatcher.downloadFileBuilder.build();
				MessageDispatcher.downloadFileBuilder = Messages.DownloadFile.newBuilder();
				storageClientProxy.disconnect();
				return data;
			}
    	});
    }
	
	/**
	 * getStorageNodes from controller based on chunkName.
	 * @param chunkName
	 * @return
	 */
	private static synchronized Future<List<Messages.StoredLocationType>> getStoredNodes(String chunkName) {
    	return threadPool.submit(() -> {
    		ControllerClientProxy controllerClientProxy = new ControllerClientProxy(Storage.config.getControllerHost(),
    				Storage.config.getControllerPort(), Storage.config.getChunkSize());
    		controllerClientProxy.getStoredLocations(chunkName, Messages.NodeType.STORAGE);
    		synchronized(MessageDispatcher.locations) {
				if(MessageDispatcher.locations.isEmpty()) {
					MessageDispatcher.locations.wait(TIME_OUT);
				}
				List<Messages.StoredLocationType> copy = new LinkedList<>(MessageDispatcher.locations);
				MessageDispatcher.locations = new LinkedList<>();
				controllerClientProxy.disconnect();
				return copy;
			}
    	});
    }
	
	/**
	 * Read file from file system.
	 * @param file
	 * @return
	 * @throws IOException
	 */
	private static synchronized byte[] readFile(File file) throws IOException {
		byte[] data = new byte[(int) file.length()];
		if(file.exists()) {
			log.info("Reading file: " + file.getAbsolutePath());
		}
		RandomAccessFile rFile = new RandomAccessFile(file, "r");
		rFile.read(data);
		rFile.close();
		return data;
	}
	
	/**
	 * Verifies the checksum of the data.
	 * @param directory
	 * @param filename
	 * @param data
	 * @return
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	private static synchronized boolean verifyChecksum(File directory, String filename, byte[] data) throws IOException, NoSuchAlgorithmException {
		File file = new File(directory, Constants.CHECKSUM_PATH + "/" + filename + Constants.CHECKSUM_SUFFIX);
		log.info("Looking for checksum on path: " + file.getAbsolutePath());
		byte[] expectedChecksum = readFile(file);
		log.info("Expected checksum for file: " + filename + " is " + new String(expectedChecksum));
		byte[] obtainedChecksum = checksum(data); 
		log.info("Obtained checksum for file: " + filename + " is " + new String(obtainedChecksum));
		return Arrays.equals(expectedChecksum, obtainedChecksum);
	}

	/**
	 * Creates file not found message.
	 * @param uploadFile
	 * @return
	 */
	private static synchronized Messages.ProtoMessage fileNotFound(Messages.UploadFile uploadFile) {
		return Messages.ProtoMessage.newBuilder()
				.setClient(Messages.Client.newBuilder()
						.setDownloadFile(Messages.DownloadFile.newBuilder()
								.setFileFound(false)
								.setStoreChunk(Messages.StoreChunk.newBuilder()
										.setFileName(uploadFile.getFilename())
										.build())
								.build())
						.build())
				.build();
	}
	
	/**
	 * Send file client / storage based on the node type.
	 * @param data
	 * @param uploadFile
	 * @return
	 * @throws InvalidProtocolBufferException
	 */
	private static synchronized Messages.ProtoMessage sendFile(byte[] data, Messages.UploadFile uploadFile) throws InvalidProtocolBufferException {
		if(uploadFile.getNodeType() == Messages.NodeType.CLIENT) {
			return Messages.ProtoMessage.newBuilder()
					.setClient(Messages.Client.newBuilder()
							.setDownloadFile(Messages.DownloadFile.newBuilder()
									.setFileFound(true)
									.setStoreChunk(Messages.StoreChunk.newBuilder()
											.setFileName(uploadFile.getFilename())
											.setData(Messages.Data.parseFrom(data))
											.build())
									.build())
							.build())
					.build();
		}
		else {
			return Messages.ProtoMessage.newBuilder()
					.setStorage(Messages.Storage.newBuilder()
							.setDownloadFile(Messages.DownloadFile.newBuilder()
									.setFileFound(true)
									.setStoreChunk(Messages.StoreChunk.newBuilder()
											.setFileName(uploadFile.getFilename())
											.setData(Messages.Data.parseFrom(data))
											.build())
									.build())
							.build())
					.build();
		}
	}
	
	/**
	 * Clears storage path for given directory.
	 * @param directory
	 * @param isDelete
	 */
	public static synchronized void clearStoragePath(File directory, boolean isDelete) {
		File[] allContents = directory.listFiles();
	    if (allContents != null) {
	        for (File file : allContents) {
	        	clearStoragePath(file, true);
	        }
	    }
	    if(isDelete)
	    	directory.delete();
	}

}
