package edu.usfca.cs.dfs.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.Controller;
import edu.usfca.cs.dfs.messages.Messages.StorageFeedback.Builder;
import edu.usfca.cs.dfs.utils.Constants;

public class StorageHandlers {
	
	public static String STORAGE_PATH;
	private static long processedRequest = 0;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
	
	public static Messages.ProtoMessage store(Messages.StoreChunk chunk) throws IOException, InterruptedException, ExecutionException {
		List<Messages.StorageNode> locations = chunk.getStorageLocationsList();
		Messages.StorageNode location = locations.get(0);
		Messages.StoreProof.StorageType storageType = Messages.StoreProof.StorageType.PRIMARY;
		// ToDo: Remove this before sumbitting
		String pathString = STORAGE_PATH + location.getHost() + location.getPort() + "/"; 
		// ToDo: Uncomment this before submitting
		// String pathString = STORAGE_PATH;
		if(locations.size() < Constants.REPLICAS) {
			pathString += Constants.REPLICA_PATH;
			storageType = Messages.StoreProof.StorageType.REPLICA;
		}
		locations = new LinkedList<>(locations);
		Path path = Paths.get(pathString+chunk.getFileName());
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
		byte[] data = chunk.getData().toByteArray();
		Files.write(path, data, StandardOpenOption.CREATE);
		Messages.ProtoMessage msg = null;
		if(locations.size() == Constants.REPLICAS) {
			msg = Messages.ProtoMessage.newBuilder()
					.setClient(Messages.Client.newBuilder()
							.setStorageFeedback(getStorageFeedback(chunk, location, storageType).get())
							.build())
					.build();
		}
		locations.remove(0);
		if(!locations.isEmpty()) {
			StorageClientProxy storageClientProxy = new StorageClientProxy(location.getHost(), location.getPort());
			storageClientProxy.upload(Messages.StoreChunk.newBuilder()
					.setChunkId(chunk.getChunkId())
					.setData(chunk.getData())
					.setFileName(chunk.getFileName())
					.addAllStorageLocations(locations)
					.build());
			storageClientProxy.disconnect();
		}
		processedRequest++;
		return msg;
		
	}
	
	private static Future<Messages.StorageFeedback> getStorageFeedback(Messages.StoreChunk chunk
			, Messages.StorageNode location, Messages.StoreProof.StorageType storageType) {
		ControllerClientProxy controllerProxy = new ControllerClientProxy();
		controllerProxy.sendStorageProof(chunk.getFileName(), storageType, location);
		return threadPool.submit(() -> {
			synchronized(MessageDispatcher.storageFeedback) {
				if(!MessageDispatcher.storageFeedback.getIsStored()) {
					MessageDispatcher.storageFeedback.wait();
				}
				Messages.StorageFeedback feedback = MessageDispatcher.storageFeedback.build();
				MessageDispatcher.storageFeedback.setIsStored(false);
				controllerProxy.disconnect();
				return feedback;
			}
		});
	}
	
	public static void startHeartbeat(String selfHostName, int selfPort) throws InterruptedException {
		Thread heartbeatThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while(true) {
					ControllerClientProxy contorllerClientProxy = new ControllerClientProxy();
					try {
						contorllerClientProxy.sendHeartbeat(Files.getFileStore(Paths.get(STORAGE_PATH)).getUsableSpace()
								, processedRequest, Messages.StorageNode.newBuilder()
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
	
	public static Messages.ProtoMessage retrive(Messages.UploadFile uploadFile) throws IOException {
		System.out.println("Received Retrive request for, " + uploadFile.getFilename());
		String filePath = STORAGE_PATH + uploadFile.getStorageNode().getHost()
				+ uploadFile.getStorageNode().getPort() + "/";
		File file = new File(filePath + uploadFile.getFilename());
		if(!file.exists()) {
			file = new File(filePath + Constants.REPLICA_PATH + uploadFile.getFilename());
			if(!file.exists()) {
				return fileNotFound(uploadFile);
			} else {
				filePath += Constants.REPLICA_PATH;
			}
		}
		Path path = Paths.get(filePath + uploadFile.getFilename());
		byte[] data = new byte[(int) Files.size(path)];
		RandomAccessFile aFile = new RandomAccessFile(filePath + uploadFile.getFilename(), "r");
    	FileChannel inChannel = aFile.getChannel();
    	ByteBuffer buffer = ByteBuffer.allocate((int) Files.size(path));
		inChannel.read(buffer);
		buffer.flip();
		buffer.get(data);
		processedRequest++;
		aFile.close();
		return sendFile(data, uploadFile);
		
	}
	
	private static Messages.ProtoMessage fileNotFound(Messages.UploadFile uploadFile) {
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
	
	private static Messages.ProtoMessage sendFile(byte[] data, Messages.UploadFile uploadFile) {
		return Messages.ProtoMessage.newBuilder()
				.setClient(Messages.Client.newBuilder()
						.setDownloadFile(Messages.DownloadFile.newBuilder()
								.setFileFound(true)
								.setStoreChunk(Messages.StoreChunk.newBuilder()
										.setFileName(uploadFile.getFilename())
										.setData(ByteString.copyFrom(data))
										.build())
								.build())
						.build())
				.build();
	}

	public static void clearStoragePath(File directory, boolean isDelete) {
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
