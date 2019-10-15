package edu.usfca.cs.dfs.utils;

public class Config {
	private int primaryK;
	private int primaryM;
	private int replicaK;
	private int replicaM;
	private int replicaCount;
	private int storagePort;
	private int chunkSize;
	private String controllerHost;
	private int controllerPort;
	private String storagePath;
	private String retrivePath;
	
	public String getStoragePath() {
		return storagePath;
	}
	public void setStoragePath(String storagePath) {
		this.storagePath = storagePath;
	}
	public String getRetrivePath() {
		return retrivePath;
	}
	public void setRetrivePath(String retrivePath) {
		this.retrivePath = retrivePath;
	}
	public String getControllerHost() {
		return controllerHost;
	}
	public void setControllerHost(String controllerHost) {
		this.controllerHost = controllerHost;
	}
	public int getControllerPort() {
		return controllerPort;
	}
	public void setControllerPort(int controllerPort) {
		this.controllerPort = controllerPort;
	}
	public int getChunkSize() {
		return chunkSize;
	}
	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}
	public int getStoragePort() {
		return storagePort;
	}
	public void setStoragePort(int port) {
		this.storagePort = port;
	}
	public int getReplicaCount() {
		return replicaCount;
	}
	public void setReplicaCount(int replicaCount) {
		this.replicaCount = replicaCount;
	}
	public int getPrimaryK() {
		return primaryK;
	}
	public void setPrimaryK(int primaryK) {
		this.primaryK = primaryK;
	}
	public int getPrimaryM() {
		return primaryM;
	}
	public void setPrimaryM(int primaryM) {
		this.primaryM = primaryM;
	}
	public int getReplicaK() {
		return replicaK;
	}
	public void setReplicaK(int replicaK) {
		this.replicaK = replicaK;
	}
	public int getReplicaM() {
		return replicaM;
	}
	public void setReplicaM(int replicaM) {
		this.replicaM = replicaM;
	}
}
