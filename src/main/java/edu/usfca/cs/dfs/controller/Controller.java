package edu.usfca.cs.dfs.controller;

import java.io.IOException;

public class Controller {

    public static void main(String args[]) throws IOException {
        ControllerServer s = new ControllerServer(7777);
        s.start();
    }
}
