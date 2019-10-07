package edu.usfca.cs.dfs.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.gson.Gson;

public class Controller {
	
	private static String CONFIG_KEY = "-config";

    public static void main(String args[]) throws IOException {
    	Gson gson = new Gson();
    	if(args.length != 2 || !args[0].equals(CONFIG_KEY)) {
    		System.out.println("Failed to start server: Incomplete/Invalid arguments passed");
    		return;
    	}
    	ControllerHandlers.CONFIG = gson.fromJson(readFile(Paths.get(args[1])), Config.class);
        ControllerServer s = new ControllerServer(ControllerHandlers.CONFIG.getPort());
        s.start();
        ControllerHandlers.monitorHeartBeats();
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
