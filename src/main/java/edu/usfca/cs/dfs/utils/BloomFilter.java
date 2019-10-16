package edu.usfca.cs.dfs.utils;

import com.sangupta.murmur.Murmur3;

/**
 * Bloom filter implementation.
 * @author kedarkhetia
 *
 */
public class BloomFilter {
    private byte[] filter;
    private int k, m;

    public BloomFilter(int k, int m) {
        this.k = k;
        this.m = m;
        filter = new byte[m];
    }

    public void put(byte[] data) {
        int[] tempHash = hash(data);
        for(int i=0; i<tempHash.length; i++) {
            filter[tempHash[i]] = 1;
        }
    }

    public boolean get(byte[] data) {
        int[] tempHash = hash(data);
        for(int i=0; i<tempHash.length; i++) {
            if(filter[tempHash[i]] == 0) {
                return false;
            }
        }
        return true;
    }

    public  float falsePositiveProb() {
        int n = getNumOfItems();
        double expExpression = ((double)(-k*n)/m);
        double inner = 1 - Math.exp(expExpression);
        double probability = Math.pow(inner, k);
        return (float) probability;
    }

    public int getNumOfItems() {
        int count = 0;
        for(int i=0; i<filter.length; i++) {
            if(filter[i] == 1) count++;
        }
        return count;
    }

    private int[] hash(byte[] data) {
        int[] tempHash = new int[k];
        tempHash[0] = Math.abs((int) Murmur3.hash_x64_128(data, data.length, 0)[0] % m);
        for(int i=1; i<k; i++) {
            tempHash[i] = Math.abs((int) Murmur3.hash_x64_128(data, data.length, tempHash[i-1])[0] % m);
        }
        return tempHash;
    }

    public String toString() {
        String bloomFilter = "";
        for(int i=0; i<filter.length; i++) {
            bloomFilter += filter[i] + " ";
        }
        return bloomFilter;
    }
}
