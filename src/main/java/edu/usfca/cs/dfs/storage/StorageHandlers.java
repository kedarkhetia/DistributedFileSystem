package edu.usfca.cs.dfs.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;

import edu.usfca.cs.dfs.clients.StorageClientProxy;
import edu.usfca.cs.dfs.messages.Messages;
import edu.usfca.cs.dfs.utils.Constants;

public class StorageHandlers {
	
	public static void store(Messages.StoreChunk chunk) throws IOException {
		List<Messages.StorageNode> locations = chunk.getStorageLocationsList();
		Messages.StorageNode location = locations.get(0);
		String pathString = "./bigdata/" + location.getHost() + location.getPort() + "/";
		if(locations.size() == 2 || locations.size() == 1) {
			pathString += "replica/";
		}
		locations = new LinkedList<>(locations);
		locations.remove(0);
		Path path = Paths.get(pathString+chunk.getFileName());
		if(!Files.exists(path)) {
			Files.createDirectories(path.getParent());
			Files.createFile(path);
		}
		BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
		String data = chunk.getData().toStringUtf8();
		writer.write(data, 0, data.length());
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
	}
	
	public static Messages.ProtoMessage getAvailableSpace() throws IOException {
		FileStore store = Files.getFileStore(Paths.get(Constants.STORAGE_PATH));
		return Messages.ProtoMessage
				.newBuilder().setController(Messages.Controller
						.newBuilder().setStorageSpace(Messages.StorageSpace
								.newBuilder().setAvailableSpace(store.getUsableSpace())
								.build())
						.build())
				.build();
	}

}
