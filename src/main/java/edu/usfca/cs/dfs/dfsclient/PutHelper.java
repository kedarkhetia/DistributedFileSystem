package edu.usfca.cs.dfs.dfsclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

public class PutHelper {
	
	private static int chunkSize = Constants.CHUNK_SIZE;
	private static ExecutorService threadPool = Executors.newFixedThreadPool(Constants.NUMBER_OF_THREADS);
	
	public static List<Messages.StoreChunk> getChunks(String filename) throws IOException {
        Path path = Paths.get(filename);
        List<Messages.StoreChunk> storeChunks = new LinkedList<>();
        try(BufferedReader in = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
            int readBytes;
            byte[] data;
            int count = (int) Files.size(path) / chunkSize;
            int bufCount = 0;
            int i = 0;
            for(i=0; i<count; i++) {
                data = new byte[chunkSize];
                bufCount = 0;
                while(bufCount < data.length && (readBytes = in.read()) != -1) {
                    data[bufCount] = (byte) readBytes;
                    bufCount++;
                }
                storeChunks.add(Messages.StoreChunk.newBuilder()
                        .setFileName(filename+"_chunk"+i)
                        .setChunkId(i)
                        .setData(ByteString.copyFrom(data))
                        .build());
            }
            int restBuffer = (int) Files.size(path) - (chunkSize * count);
            bufCount = 0;
            data = new byte[restBuffer];
            while(bufCount < restBuffer && (readBytes = in.read()) != -1) {
                data[bufCount] = (byte) readBytes;
                bufCount++;
            }
            storeChunks.add(Messages.StoreChunk.newBuilder()
                    .setFileName(filename+"_chunk"+i)
                    .setChunkId(i)
                    .setData(ByteString.copyFrom(Arrays.copyOf(data, bufCount)))
                    .build());
        }
        return storeChunks;
    }
	
	public static void storeInStorage(Messages.StoreChunk chunk, List<Messages.StorageNode> locations) {
		for(Messages.StorageNode location : locations) {
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					StorageClientProxy storageClient = 
							new StorageClientProxy(location.getHost(), location.getPort());
					storageClient.upload(chunk);
					storageClient.disconnect();
				}
			});
		}
	}
	
	public static Future<List<Messages.StorageNode>> getStorageNodes() {
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

}
