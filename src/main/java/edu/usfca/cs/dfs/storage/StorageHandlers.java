package edu.usfca.cs.dfs.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import edu.usfca.cs.dfs.messages.Messages;

public class StorageHandlers {
	
	public static void store(Messages.StoreChunk chunk) throws IOException {
		Path path = Paths.get("./bigdata/"+chunk.getFileName());
		if(!Files.exists(path,  LinkOption.NOFOLLOW_LINKS)) {
			Files.createFile(path);
		}
		BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
		String data = chunk.getData().toStringUtf8();
		writer.write(data, 0, data.length());
		writer.close();
	}

}
