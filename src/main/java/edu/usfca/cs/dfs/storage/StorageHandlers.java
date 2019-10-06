package edu.usfca.cs.dfs.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
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
import edu.usfca.cs.dfs.utils.Constants;

public class StorageHandlers {
	
	public static void store(Messages.StoreChunk chunk) throws IOException {
		List<Messages.StorageNode> locations = chunk.getStorageLocationsList();
		Messages.StorageNode location = locations.get(0);
		Messages.StoreProof.StorageType storageType = Messages.StoreProof.StorageType.PRIMARY;
		String pathString = Constants.STORAGE_PATH + location.getHost() + location.getPort() + "/";
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
		BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"));
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
		ControllerClientProxy controllerProxy = new ControllerClientProxy();
		controllerProxy.sendStorageProof(chunk.getFileName(), storageType, location);
		controllerProxy.disconnect();
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
	
	public static Messages.ProtoMessage upload(Messages.UploadFile uploadFile) throws IOException {
		String filePath = Constants.STORAGE_PATH + uploadFile.getStorageNode().getHost()
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
		try(BufferedReader in = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
			int read;
			byte[] data = new byte[(int) Files.size(path)];
			int count = 0;
			while((read = in.read()) != -1) {
				data[count] = (byte) read;
			}
			System.out.println(ByteString.copyFrom(data).toStringUtf8().hashCode());
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

}
