package com.beef.util.redis.compress;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

public class LZFCompressor implements ICompressor {

	@Override
	public byte[] compress(byte[] bytes) throws CompressException {
		return LZFEncoder.encode(bytes);
	}

	@Override
	public byte[] decompress(byte[] bytes) throws CompressException {
		try {
			return LZFDecoder.decode(bytes);
		} catch(Throwable t) {
			throw new CompressException(t);
		}
	}

}
