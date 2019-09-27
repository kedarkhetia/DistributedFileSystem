package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.controller.net.Server;

import java.io.IOException;

public class Controller {

    public static void main(String args[]) throws IOException {
        Server s = new Server(7777);
        s.start();
    }
}
