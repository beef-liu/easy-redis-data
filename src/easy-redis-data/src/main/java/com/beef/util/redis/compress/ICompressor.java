package com.beef.util.redis.compress;

public interface ICompressor {
	
	//public String compress(String str) throws CompressException;
	
	public byte[] compress(byte[] bytes) throws CompressException;
	
	//public String decompress(String str) throws CompressException;
	
	public byte[] decompress(byte[] bytes) throws CompressException;
}
