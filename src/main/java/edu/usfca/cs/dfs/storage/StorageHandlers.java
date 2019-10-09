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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.github.luben.zstd.Zstd;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

public class StorageHandlers {
	
	public static String STORAGE_PATH;
	private static long processedRequest = 0;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
	
	public static synchronized Messages.ProtoMessage store(Messages.StoreChunk chunk) throws InterruptedException, ExecutionException, IOException {
		Messages.StorageType storageType = chunk.getStorageType();
		String pathString = STORAGE_PATH;
		Messages.StorageNode location;
		Messages.StorageNode primary = chunk.getPrimary();
		List<Messages.StorageNode> locations = chunk.getReplicasList();
		location = primary;
		if(storageType == Messages.StorageType.PRIMARY) {
			pathString += location.getHost() + location.getPort() + "/";
		}
		if(storageType == Messages.StorageType.REPLICA) {
			location = locations.get(0);
			pathString += location.getHost() + location.getPort() + "/";
			locations = new LinkedList<>(locations);
			locations.remove(location);
			pathString += Constants.REPLICA_PATH + primary.getHost() + primary.getPort() + "/";
		}
		boolean compressFlag = canCompress(chunk.getData().toByteArray());
		byte[] data = chunk.getData().toByteArray();
		if(compressFlag) {
			pathString += Constants.COMPRESSED_PATH + "/";
			data = compress(chunk.getData().toByteArray());
		}
		Path path = Paths.get(pathString+chunk.getFileName());
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
		Files.write(path, data, StandardOpenOption.CREATE);
		Messages.ProtoMessage message = getFeedback(storageType, location, chunk);
		if(!locations.isEmpty()) {
			location = locations.get(0);
			//System.out.println(locations.get(0));
			sendToReplicas(primary, location, locations, chunk);
		}
		processedRequest++;
		return message;
	}
	
	private static synchronized Messages.ProtoMessage getFeedback(Messages.StorageType storageType, 
			Messages.StorageNode location, Messages.StoreChunk chunk) throws InterruptedException, ExecutionException {
		if(storageType == Messages.StorageType.PRIMARY) {
			return Messages.ProtoMessage.newBuilder()
					.setClient(Messages.Client.newBuilder()
							.setStorageFeedback(getStorageFeedback(chunk, location, storageType).get())
							.build())
					.build();
		}
		return null;
	}
	
	private static synchronized Future<Messages.StorageFeedback> getStorageFeedback(Messages.StoreChunk chunk,
			Messages.StorageNode location, Messages.StorageType storageType) {
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
	
	public static synchronized Messages.StorageFeedback sendToReplicas(Messages.StorageNode primary, Messages.StorageNode location,
			List<Messages.StorageNode> locations, Messages.StoreChunk chunk) throws InterruptedException, ExecutionException {
		StorageClientProxy storageClientProxy = new StorageClientProxy(location.getHost(), location.getPort());
		storageClientProxy.upload(Messages.StoreChunk.newBuilder()
				.setChunkId(chunk.getChunkId())
				.setData(chunk.getData())
				.setFileName(chunk.getFileName())
				.setPrimary(primary)
				.addAllReplicas(locations)
				.setStorageType(Messages.StorageType.REPLICA)
				.build());
		Messages.StorageFeedback feedback = getStorageFeedback(chunk, location, Messages.StorageType.REPLICA).get();
		storageClientProxy.disconnect();
		return feedback;
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
			System.out.println(directory.getAbsolutePath() + " " + filename);
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
				System.out.println(directory.getAbsolutePath() + " " + filename);
				return newfile;
			}
		}
		return null;
	}
	
	public static synchronized Messages.ProtoMessage retrive(Messages.UploadFile uploadFile) throws IOException {
		String filePath = STORAGE_PATH + uploadFile.getStorageNode().getHost()
				+ uploadFile.getStorageNode().getPort() + "/";
		if(uploadFile.getStorageType() == Messages.StorageType.REPLICA) {
			filePath += Constants.REPLICA_PATH;
		}
		System.out.println(uploadFile.getStorageType());
		File directory = new File(filePath);
		directory = getFilePath(directory, uploadFile.getFilename());
		if(directory == null) {
			return fileNotFound(uploadFile);
		}
		File file = new File(directory, uploadFile.getFilename());
		byte[] data = new byte[(int) file.length()];
		RandomAccessFile aFile = new RandomAccessFile(file, "r");
    	FileChannel inChannel = aFile.getChannel();
    	ByteBuffer buffer = ByteBuffer.allocate((int) file.length());
		inChannel.read(buffer);
		buffer.flip();
		buffer.get(data);
		if(directory.getAbsolutePath().endsWith(Constants.COMPRESSED_PATH)) {
			data = decompress(data);
		}
		processedRequest++;
		aFile.close();
		return sendFile(data, uploadFile);
		
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
