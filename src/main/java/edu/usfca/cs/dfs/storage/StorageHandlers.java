package edu.usfca.cs.dfs.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.messages.Messages.Controller;
import edu.usfca.cs.dfs.utils.Constants;

public class StorageHandlers {
	
	public static String STORAGE_PATH;
	private static long processedRequest = 0;
	
	public static void store(Messages.StoreChunk chunk) throws IOException {
		List<Messages.StorageNode> locations = chunk.getStorageLocationsList();
		Messages.StorageNode location = locations.get(0);
		Messages.StoreProof.StorageType storageType = Messages.StoreProof.StorageType.PRIMARY;
		// ToDo: Remove this before sumbitting
		String pathString = STORAGE_PATH + location.getHost() + location.getPort() + "/"; 
		// ToDo: Uncomment this before submitting
		// String pathString = STORAGE_PATH;
		if(locations.size() == 2 || locations.size() == 1) {
			pathString += Constants.REPLICA_PATH;
			storageType = Messages.StoreProof.StorageType.REPLICA;
		}
		locations = new LinkedList<>(locations);
		locations.remove(0);
		Path path = Paths.get(pathString+chunk.getFileName());
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
		BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.ISO_8859_1);
		String data = chunk.getData().toString(Charset.forName("ISO_8859_1"));
		writer.write(data, 0, chunk.getData().toByteArray().length);
		writer.close();
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
		ControllerClientProxy controllerProxy = new ControllerClientProxy();
		controllerProxy.sendStorageProof(chunk.getFileName(), storageType, location);
		processedRequest++;
		controllerProxy.disconnect();
	}
	
	public static void startHeartbeat(String selfHostName, int selfPort) throws InterruptedException {
		Thread heartbeatThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while(true) {
					ControllerClientProxy contorllerClientProxy = new ControllerClientProxy();
					try {
						contorllerClientProxy.sendHeartbeat(Files.getFileStore(Paths.get(STORAGE_PATH)).getUsableSpace()
								, selfPort, Messages.StorageNode.newBuilder()
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
		heartbeatThread.join();
	}
	
	public static Messages.ProtoMessage retrive(Messages.UploadFile uploadFile) throws IOException {
		String filePath = STORAGE_PATH + uploadFile.getStorageNode().getHost()
				+ uploadFile.getStorageNode().getPort() + "/";
		File file = new File(filePath + uploadFile.getFilename());
		if(!file.exists()) {
			file = new File(filePath + Constants.REPLICA_PATH + uploadFile.getFilename());
			if(!file.exists()) {
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
		}
		Path path = Paths.get(filePath + uploadFile.getFilename());
		try(BufferedReader in = Files.newBufferedReader(path, StandardCharsets.ISO_8859_1)) {
			int read;
			byte[] data = new byte[(int) Files.size(path)];
			int count = 0;
			while((read = in.read()) != -1) {
				data[count] = (byte) read;
				count++;
			}
			processedRequest++;
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
	}

	public static void clearStoragePath(File directory) {
		File[] allContents = directory.listFiles();
	    if (allContents != null) {
	        for (File file : allContents) {
	        	clearStoragePath(file);
	        }
	    }
	}

}
