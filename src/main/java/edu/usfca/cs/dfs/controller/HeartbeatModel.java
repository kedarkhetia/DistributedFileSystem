package edu.usfca.cs.dfs.controller;

import java.util.Date;

import edu.usfca.cs.dfs.utils.BloomFilter;

public class HeartbeatModel {
	private BloomFilter primary;
	private BloomFilter replica;
	private long availableSpace;
	private long processedRequests;
	private long timestamp;
	
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public BloomFilter getPrimary() {
		return primary;
	}
	public void setPrimary(BloomFilter primary) {
		this.primary = primary;
	}
	public BloomFilter getReplica() {
		return replica;
	}
	public void setReplica(BloomFilter replica) {
		this.replica = replica;
	}
	public long getAvailableSpace() {
		return availableSpace;
	}
	public void setAvailableSpace(long availableSpace) {
		this.availableSpace = availableSpace;
	}
	public long getProcessedRequests() {
		return processedRequests;
	}
	public void setProcessedRequests(long processedRequests) {
		this.processedRequests = processedRequests;
	}
	
	
}
