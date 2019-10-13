package edu.usfca.cs.dfs.storage;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.github.luben.zstd.Zstd;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.Data;
import edu.usfca.cs.dfs.utils.Constants;

public class StorageHandlers {
	
	public static String STORAGE_PATH;
	private static long processedRequest = 0;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
	private static Messages.StorageNode selfNode;
	private static int TIME_OUT = 3000;
	
	public static synchronized Messages.ProtoMessage store(Messages.StoreChunk chunk) 
			throws InterruptedException, ExecutionException, IOException, NoSuchAlgorithmException {
		Messages.StorageType storageType = chunk.getStorageType();
		System.out.println("Filename: " + chunk.getFileName());
		String pathString = STORAGE_PATH;
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
		Path path = Paths.get(pathString+chunk.getFileName());
		createFilesAndDirs(path);
		Path checksumPath = Paths.get(pathString + Constants.CHECKSUM_PATH + "/" + chunk.getFileName() + Constants.CHECKSUM_SUFFIX);
		createFilesAndDirs(checksumPath);
		byte[] checksum = checksum(data);
		Files.write(checksumPath, checksum, StandardOpenOption.CREATE);
		Files.write(path, data, StandardOpenOption.CREATE);
		Messages.StorageFeedback feedback = getStorageFeedback(chunk, location).get();
		processedRequest++;
		if(!locations.isEmpty()) {
			location = locations.get(0);
			//System.out.println("Forwarding data to: " + locations);
			Messages.StoreChunk newChunk = Messages.StoreChunk.newBuilder()
				.setData(chunk.getData())
				.setFileName(chunk.getFileName())
				.setPrimary(primary)
				.addAllReplicas(locations)
				.setStorageType(Messages.StorageType.REPLICA)
			.build();
			sendToReplicas(newChunk, location);
		}
		if(storageType == Messages.StorageType.PRIMARY && chunk.getNodeType() == Messages.NodeType.CLIENT) {
			return getFeedback(feedback);
		}
		return null; // ToDo: Feedback to storage if time permits.
	}
	
	private static synchronized void createFilesAndDirs(Path path) throws IOException {
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
	}
	
	private static synchronized Messages.ProtoMessage getFeedback(Messages.StorageFeedback feedback) throws InterruptedException, ExecutionException {
			return Messages.ProtoMessage.newBuilder()
					.setClient(Messages.Client.newBuilder()
							.setStorageFeedback(feedback)
							.build())
					.build();
	}
	
	private static synchronized Future<Messages.StorageFeedback> getStorageFeedback(Messages.StoreChunk chunk,
			Messages.StorageNode location) {
		ControllerClientProxy controllerProxy = new ControllerClientProxy();
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
	
	public static synchronized void sendToReplicas(Messages.StoreChunk storeChunk, Messages.StorageNode location) throws InterruptedException, ExecutionException {
		StorageClientProxy storageClientProxy = new StorageClientProxy(location.getHost(), location.getPort());
		storageClientProxy.upload(storeChunk);
		//Messages.StorageFeedback feedback = getStorageFeedback(storeChunk, location, storeChunk.getStorageType()).get();
		//System.out.println("Updated for node: " + location.getHost()+":"+location.getPort() + storeChunk.getFileName());
		storageClientProxy.disconnect();
		//return feedback;
	}
	
	public static synchronized void replicate(Messages.Replicate replicate) throws IOException, InterruptedException, ExecutionException {
		Messages.StorageNode forNode = replicate.getForNode();
		Messages.StorageNode fromNode = replicate.getFromNode();
		Messages.StorageNode toNode = replicate.getToNode();
		String directoryPath = STORAGE_PATH + fromNode.getHost() + fromNode.getPort() + "/";
		System.out.println(directoryPath);
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
				List<Messages.StorageNode> locations = new LinkedList<>();
				Messages.StorageType storageType = replicate.getStorageType();
				Messages.StoreChunk chunk;
				if(storageType == Messages.StorageType.PRIMARY) {
					storageType = Messages.StorageType.REPLICA;
					locations.add(replicate.getToNode());
					chunk = Messages.StoreChunk.newBuilder()
							.setData(Data.parseFrom(data))
							.setFileName(file.getName())
							.setPrimary(replicate.getFromNode())
							.addAllReplicas(locations)
							.setStorageType(storageType)
							.setNodeType(replicate.getNodeType())
						.build();
					System.out.println("Replicating Primary to Replica for file: " + chunk.getFileName() + " from "
							+ replicate.getFromNode().getHost() + ":" + replicate.getFromNode().getPort() + " to "
									+ replicate.getToNode().getHost() + ":" + replicate.getToNode().getPort());
					sendToReplicas(chunk, replicate.getToNode());
				}
				else {
					storageType = Messages.StorageType.PRIMARY;
					//locations.add(replicate.getFromNode());
					chunk = Messages.StoreChunk.newBuilder()
							.setData(Data.parseFrom(data))
							.setFileName(file.getName())
							.setPrimary(replicate.getToNode())
							.addAllReplicas(locations)
							.setStorageType(storageType)
							.setNodeType(replicate.getNodeType())
						.build();
//					System.out.println("Replicating Replica to Primary for file: " + chunk.getFileName() + " from "
//							+ replicate.getFromNode().getHost() + ":" + replicate.getFromNode().getPort() + " to "
//									+ replicate.getToNode().getHost() + ":" + replicate.getToNode().getPort());
					sendToReplicas(chunk, replicate.getToNode());
				}
			}
		}
		
	}
	
	private static synchronized byte[] checksum(byte[] data) throws IOException, NoSuchAlgorithmException {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		byte[] checksum = digest.digest(data);
		//System.out.println("Checksum for file: " + filename + " is " + new String(checksum));
		return checksum;
	}
	
	private static synchronized byte[] compress(byte[] data) {
		return Zstd.compress(data, Constants.COMPRESS_LEVEL);
	}
	
	private static synchronized byte[] decompress(byte[] data) {
		long decompressedSize = Zstd.decompressedSize(data);
		return Zstd.decompress(data, (int) decompressedSize);
	}
	
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
	
	public static synchronized void startHeartbeat(String selfHostName, int selfPort) throws InterruptedException {
		Thread heartbeatThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while(true) {
					ControllerClientProxy contorllerClientProxy = new ControllerClientProxy();
					try {
						contorllerClientProxy.sendHeartbeat(Files.getFileStore(Paths.get(STORAGE_PATH)).getUsableSpace(), 
								processedRequest, Messages.StorageNode.newBuilder()
									.setHost(selfHostName)
									.setPort(selfPort)
									.build());
						Thread.sleep(Constants.HEARTBEAT_INTERVAL);
					} catch (IOException | InterruptedException e) {
						e.printStackTrace();
					}
					contorllerClientProxy.disconnect();
				}
			}
		});
		heartbeatThread.start();
	}
	
	private static synchronized File getFilePath(File directory, String filename) {
		File file = new File(directory, filename);
		if(file.exists()) {
			//System.out.println(directory.getAbsolutePath() + " " + filename);
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
				//System.out.println(directory.getAbsolutePath() + " " + filename);
				return newfile;
			}
		}
		return null;
	}
	
	public static synchronized Messages.ProtoMessage retrive(Messages.UploadFile uploadFile) 
			throws IOException, NoSuchAlgorithmException, InterruptedException, ExecutionException {
		String filePath = STORAGE_PATH + uploadFile.getStorageNode().getHost()
				+ uploadFile.getStorageNode().getPort() + "/";
		if(uploadFile.getStorageType() == Messages.StorageType.REPLICA) {
			filePath += Constants.REPLICA_PATH + "/";
		}
		//System.out.println(uploadFile.getStorageType());
		File directory = new File(filePath);
		directory = getFilePath(directory, uploadFile.getFilename());
		if(directory == null) {
			return fileNotFound(uploadFile);
		}
		File file = new File(directory, uploadFile.getFilename());
		byte[] data = readFile(file);
		if(!verifyChecksum(directory, uploadFile.getFilename(), data)) {
			//System.out.println("Calling replication for filename: " + uploadFile.getFilename());
			fixStorage(uploadFile.getFilename(), directory);
			return fileNotFound(uploadFile);
		}
		if(directory.getAbsolutePath().endsWith(Constants.COMPRESSED_PATH)) {
			data = decompress(data);
		}
		processedRequest++;
		return sendFile(data, uploadFile);
	}
	
	private static synchronized void fixStorage(String chunkName, File directory) {
		threadPool.submit(new Runnable() {
			@Override
			public void run() {
				try {
					List<Messages.StoredLocationType> storageLocationType = getStoredNodes(chunkName).get();
					for(Messages.StoredLocationType node : storageLocationType) {
						if(!(node.getLocation().getHost().equals(selfNode.getHost()) && node.getLocation().getPort() == selfNode.getPort())) {
							//System.out.println("location: " + node.getLocation() + " selfNode: " + selfNode);
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
								Files.write(path, data, StandardOpenOption.CREATE);
								Files.write(checksumPath, checksum, StandardOpenOption.CREATE);
								return;
							}
						}
					}
				} catch (InterruptedException | ExecutionException | NoSuchAlgorithmException | IOException e) {
					e.printStackTrace();
				}
				//System.out.println(storageLocationType);
			}
		});
	}
	
	private static synchronized Future<Messages.DownloadFile> getStoredData(String chunkName, Messages.StoredLocationType storedLocationType) {
    	return threadPool.submit(() -> {
    		Messages.StorageNode node = storedLocationType.getLocation();
    		StorageClientProxy storageClientProxy = new StorageClientProxy(node.getHost(), 
					node.getPort());
			storageClientProxy.download(Messages.UploadFile.newBuilder()
					.setFilename(chunkName)
					.setStorageType(storedLocationType.getStorageType())
					.setStorageNode(node)
					.setNodeType(Messages.NodeType.STORAGE)
					.build());
    		synchronized(MessageDispatcher.downloadFileBuilder) {
    			//System.out.println("Check is Empty: " + MessageDispatcher.downloadFileBuilder.build().getStoreChunk().getFileName().isEmpty());
				if(MessageDispatcher.downloadFileBuilder.build().getStoreChunk().getFileName().isEmpty()) {
					MessageDispatcher.downloadFileBuilder.wait(TIME_OUT);
				}
				Messages.DownloadFile data = MessageDispatcher.downloadFileBuilder.build();
				//System.out.println(data);
				MessageDispatcher.downloadFileBuilder = Messages.DownloadFile.newBuilder();
				storageClientProxy.disconnect();
				return data;
			}
    	});
    }
	
	private static synchronized Future<List<Messages.StoredLocationType>> getStoredNodes(String chunkName) {
    	return threadPool.submit(() -> {
    		ControllerClientProxy controllerClientProxy = new ControllerClientProxy();
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
	
	private static synchronized byte[] readFile(File file) throws IOException {
		byte[] data = new byte[(int) file.length()];
		if(file.exists()) {
			System.out.println("Exists: " + file.getAbsolutePath());
		}
		RandomAccessFile aFile = new RandomAccessFile(file, "r");
    	FileChannel inChannel = aFile.getChannel();
    	ByteBuffer buffer = ByteBuffer.allocate((int) file.length());
		inChannel.read(buffer);
		buffer.flip();
		buffer.get(data);
		aFile.close();
		return data;
	}
	
	private static synchronized boolean verifyChecksum(File directory, String filename, byte[] data) throws IOException, NoSuchAlgorithmException {
		File file = new File(directory, Constants.CHECKSUM_PATH + "/" + filename + Constants.CHECKSUM_SUFFIX);
		//System.out.println("Looking for checksum on path: " + file.getAbsolutePath());
		byte[] expectedChecksum = readFile(file);
		//System.out.println("Expected checksum for file: " + filename + " is " + new String(expectedChecksum));
		byte[] obtainedChecksum = checksum(data); 
		//System.out.println("Obtained checksum for file: " + filename + " is " + new String(obtainedChecksum));
		return Arrays.equals(expectedChecksum, obtainedChecksum);
	}

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
