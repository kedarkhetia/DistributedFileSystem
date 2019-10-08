package edu.usfca.cs.dfs.storage;

import java.io.File;
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
import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.clients.ControllerClientProxy;
import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
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
		boolean compressFlag = canCompress(chunk.getData().toByteArray());
		byte[] data = chunk.getData().toByteArray();
		if(compressFlag) {
			pathString += Constants.COMPRESSED_PATH;
			data = compress(chunk.getData().toByteArray());
			//data = chunk.getData().toByteArray();
		}
		locations = new LinkedList<>(locations);
		Path path = Paths.get(pathString+chunk.getFileName());
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
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
	
	private static byte[] compress(byte[] data) {
		return Zstd.compress(data, Constants.COMPRESS_LEVEL);
	}
	
	private static byte[] decompress(byte[] data) {
		long decompressedSize = Zstd.decompressedSize(data);
		return Zstd.decompress(data, (int) decompressedSize);
	}
	
	private static boolean canCompress(byte[] input) {
        if (input.length == 0) {
            return false;
        }
        /* Total up the occurrences of each byte */
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
        // Calculate compression
        return (1d - (entropy / 8)) > 0.6d;
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
		String filePath = STORAGE_PATH + uploadFile.getStorageNode().getHost()
				+ uploadFile.getStorageNode().getPort() + "/";
		File file = new File(filePath + uploadFile.getFilename());
		String finalPath = filePath + uploadFile.getFilename();
		if(!file.exists()) {
			File isReplica = new File(filePath + Constants.REPLICA_PATH + uploadFile.getFilename());
			File isCompressed = new File(filePath + Constants.COMPRESSED_PATH + uploadFile.getFilename());
			if(!isReplica.exists()) {
				File isCompressedReplica = new File(filePath + Constants.REPLICA_PATH +
						Constants.COMPRESSED_PATH + uploadFile.getFilename());
				if(isCompressedReplica.exists()) {
					file = isCompressedReplica;
					finalPath = filePath + Constants.REPLICA_PATH +
							Constants.COMPRESSED_PATH + uploadFile.getFilename();
				}
			} 
			else {
				file = isReplica;
				finalPath = filePath + Constants.REPLICA_PATH + uploadFile.getFilename();
			}
			if(isCompressed.exists()) {
				file = isCompressed;
				finalPath = filePath + Constants.COMPRESSED_PATH + uploadFile.getFilename();
			}
			else {
				fileNotFound(uploadFile);
			}
		}
		Path path = Paths.get(finalPath);
		byte[] data = new byte[(int) Files.size(path)];
		RandomAccessFile aFile = new RandomAccessFile(finalPath, "r");
    	FileChannel inChannel = aFile.getChannel();
    	ByteBuffer buffer = ByteBuffer.allocate((int) Files.size(path));
		inChannel.read(buffer);
		buffer.flip();
		buffer.get(data);
		if(finalPath.endsWith(Constants.COMPRESSED_PATH + uploadFile.getFilename())) {
			data = decompress(data);
		}
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
