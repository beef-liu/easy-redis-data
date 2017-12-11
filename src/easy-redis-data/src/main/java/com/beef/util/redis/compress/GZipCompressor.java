package com.beef.util.redis.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZipCompressor implements ICompressor {

	@Override
	public byte[] compress(byte[] bytes) throws CompressException {
		try {
			ByteArrayOutputStream bytesZipped = new ByteArrayOutputStream();
			GZIPOutputStream gzipOut = new GZIPOutputStream(bytesZipped);

			gzipOut.write(bytes);
			gzipOut.close();
			
			return bytesZipped.toByteArray();
		} catch(Throwable t) {
			throw new CompressException(t);
		}
	}

	@Override
	public byte[] decompress(byte[] bytes) throws CompressException {
		try {
			//gzip decompress
			GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(bytes));
			ByteArrayOutputStream bytesOrigin = new ByteArrayOutputStream();
			
			byte[] tempBuf = new byte[1024];
			int readCnt;
			while(true) {
				readCnt = gzipIn.read(tempBuf, 0, tempBuf.length);
				
				if(readCnt < 0) {
					break;
				}
				
				if(readCnt > 0) {
					bytesOrigin.write(tempBuf, 0, readCnt);
				}
			}
			
			//GZip must close
			gzipIn.close();
			
			return bytesOrigin.toByteArray();
		} catch(Throwable t) {
			throw new CompressException(t);
		}
	}

}
