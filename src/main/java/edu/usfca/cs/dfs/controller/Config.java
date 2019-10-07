package edu.usfca.cs.dfs.controller;

public class Config {
	private int primaryK;
	private int primaryM;
	private int replicaK;
	private int replicaM;
	private int replicaCount;
	private int port;
	
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
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
