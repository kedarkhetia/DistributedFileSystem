package edu.usfca.cs.dfs.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import edu.usfca.cs.dfs.utils.Config;


public class Storage {

	private static String CONFIG_KEY = "-config";
	private static String PORT_KEY = "-port";
    public static Config config;

    public static void main(String args[]) throws InterruptedException, IOException {
    	Gson gson = new Gson();
    	if(args.length != 4 || !args[0].equals(CONFIG_KEY) || !args[2].equals(PORT_KEY)) {
    		System.out.println("Failed to start server: Incomplete/Invalid arguments passed");
    		return;
    	}
    	config = gson.fromJson(readFile(Paths.get(args[1])), Config.class);
    	startStorageServer(Integer.parseInt(args[3]));
    }
    
    public static void startStorageServer(int storagePort) throws IOException, InterruptedException {
    	int port = 7765;
    	StorageServer s = new StorageServer(port, config.getChunkSize());
    	StorageHandlers.startHeartbeat(InetAddress.getLocalHost().getHostAddress(), port);
        s.start();
    }
    
    public static String readFile(Path path) throws IOException  {
		StringBuilder sb = new StringBuilder();
		try (BufferedReader in = Files.newBufferedReader(path)) {
			String line;
			while((line = in.readLine()) != null) {
				sb.append(line);
			}
			return sb.toString();
		}
	}
    
}
