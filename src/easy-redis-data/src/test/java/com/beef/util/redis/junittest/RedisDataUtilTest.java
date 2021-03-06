package com.beef.util.redis.junittest;

import MetoXML.Util.Base64Encoder;
import com.beef.util.redis.RedisDataUtil;
import com.beef.util.redis.RedisDataUtil.CompressAlgorithm;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RedisDataUtilTest {
	private JedisPool _jedisPool;
	
	public RedisDataUtilTest() {
		String redisHost = "127.0.0.1";
		int redisPort = 6379;
		int jedisPoolMaxActive = 2;
		int jedisPoolMaxIdle = 1;
		long jedisPoolMaxWait = 100;
		long jedisPoolSoftMinEvictableIdleTimeMillis = 10000;
		
		JedisPoolConfig jedisConfig = new JedisPoolConfig();
		jedisConfig.setMaxIdle(jedisPoolMaxIdle);
		
		/* old jedis version
		jedisConfig.setMaxActive(_jedisPoolMaxActive);
		jedisConfig.setMaxWait(_jedisPoolMaxWait);
		*/
		jedisConfig.setMaxTotal(jedisPoolMaxActive);
		jedisConfig.setMaxWaitMillis(jedisPoolMaxWait);
		
		jedisConfig.setSoftMinEvictableIdleTimeMillis(jedisPoolSoftMinEvictableIdleTimeMillis);
		jedisConfig.setTestOnBorrow(false);
		
		_jedisPool = new JedisPool(jedisConfig, redisHost, redisPort);
	}

	public void testRedisWrite() {
		Jedis jedis = null;
		
		try {
			jedis = _jedisPool.getResource();
			
			TestData1 data = createTestData1();
			long intVal;
			
			intVal = RedisDataUtil.incr(jedis, "test.int.1");
			intVal = RedisDataUtil.incrBy(jedis, "test.int.1", 2);
			
			RedisDataUtil.set(jedis, "test.val.1", "test测试", true);
			
			RedisDataUtil.set(jedis, "test.data.1", data, TestData1.class, false);
			RedisDataUtil.set(jedis, "test.data.2", data, TestData1.class, true);
			
			RedisDataUtil.hset(jedis, "test.hset.1", "origin", data, TestData1.class, false);
			RedisDataUtil.hset(jedis, "test.hset.2", "gzipped", data, TestData1.class, true);

			RedisDataUtil.rpush(jedis, "test.list.1", data, TestData1.class, false);
			RedisDataUtil.rpush(jedis, "test.list.1", data, TestData1.class, true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			_jedisPool.returnResource(jedis);
		}
	}

	public void testRedisRead() {
		Jedis jedis = null;

		String val;
		TestData1 data;
		
		try {
			jedis = _jedisPool.getResource();
			
			val = RedisDataUtil.get(jedis, "test.int.1", false);
			
			val = RedisDataUtil.get(jedis, "test.val.1", true);
			
			data = (TestData1) RedisDataUtil.get(jedis, "test.data.1", TestData1.class, false);
			data = (TestData1) RedisDataUtil.get(jedis, "test.data.2", TestData1.class, true);
			
			data = (TestData1) RedisDataUtil.hget(jedis, "test.hset.1", "origin", TestData1.class, false);
			data = (TestData1) RedisDataUtil.hget(jedis, "test.hset.2", "gzipped", TestData1.class, true);

			data = (TestData1) RedisDataUtil.lpop(jedis, "test.list.1", TestData1.class, false);
			data = (TestData1) RedisDataUtil.lpop(jedis, "test.list.1", TestData1.class, true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			_jedisPool.returnResource(jedis);
		}
	}

	public void testGetNull() {
		Jedis jedis = null;

		String val;
		TestData1 data;
		
		try {
			jedis = _jedisPool.getResource();
			
			val = RedisDataUtil.get(jedis, "test.int.1aa", true);
			
			val = RedisDataUtil.get(jedis, "test.val.1aa", true);
			
			data = (TestData1) RedisDataUtil.get(jedis, "test.data.1aa", TestData1.class, false);
			data = (TestData1) RedisDataUtil.get(jedis, "test.data.2aa", TestData1.class, true);
			
			data = (TestData1) RedisDataUtil.hget(jedis, "test.hset.1aa", "origin", TestData1.class, false);
			data = (TestData1) RedisDataUtil.hget(jedis, "test.hset.2aa", "gzipped", TestData1.class, true);

			data = (TestData1) RedisDataUtil.lpop(jedis, "test.list.1aa", TestData1.class, false);
			data = (TestData1) RedisDataUtil.lpop(jedis, "test.list.1aa", TestData1.class, true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			_jedisPool.returnResource(jedis);
		}
	}
	
	public void testPerformance1() {
		long beginTime = System.currentTimeMillis();
		
		Jedis jedis = null;

		try {
			jedis = _jedisPool.getResource();
			
			String key = "test.data.";
			for(int i = 0; i < 10000; i++) {
				RedisDataUtil.set(jedis, key + i, createTestData1(), TestData1.class, false);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			_jedisPool.returnResource(jedis);
		}
		
		System.out.println("testPerformance1() time cost:" + (System.currentTimeMillis() - beginTime));
	}


	public void testPerformance2() {
		long beginTime = System.currentTimeMillis();

		Jedis jedis = null;

		String key = "test.data.";
		for(int i = 0; i < 100000; i++) {
			try {
				jedis = _jedisPool.getResource();
				
				RedisDataUtil.set(jedis, key + i, createTestData1(), TestData1.class, false);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				_jedisPool.returnResource(jedis);
			}
		}
	
		System.out.println("testPerformance1() time cost:" + (System.currentTimeMillis() - beginTime));
	}
	
	public void testEncodeCompressRate() {
		testEncodeRate(CompressAlgorithm.LZF);
		testEncodeRate(CompressAlgorithm.GZIP);
	}

	@Test
	public void testDetectCompress1() {
		
		String s = "WlYBF3L//x88TGlzdD4NCiAgPEVjaXRpY1JlZGlzVXNlckxvdHRlcgR5RGF0YWAfICEKYXdhcmRD\nb2RlPjCAAAAxYAYUMTQ3Y2I3ZjVlMTgwMDMxYWEzMTwv4AErQFzAPAdSZXN1bHQ+NaAfoA6gIQVl\neHBpcmUgcANlPjwv4AIMoB4GZ29vZHNTTkB/AHMgIOAEDqAiAGyAswhQYWNrYWdlSWTgCKcHNWJl\nZTAxM2JgtwQwMDY8L+AIMuAMShBOYW1lPuW3p+WFi+WKmyhFKeAHOGAg4Aw6H1BpY1VybD5odHRw\nOi8vbWFnaWNhY3Rpdml0eS5vc3MtH2NuLWhhbmd6aG91LmFsaXl1bmNzLmNvbS96eGJhbmtfEHR0\ncXliX2Nha2VfMDUucG5n4Ad6oGCgfAVyZWNvcmTgCvrhBqICMDwv4AAqoDoCdXBkIXsBVGkg6gsx\nNDA3OTA1NzU2NTAhzOACGcArAHMhYAZTdGF0dXM+QCDgAg5AIQE8L+IWTeI1bQZkYjhmZWY2IcEE\nMWU5MzOiTUIZYH0gXmJ+ol7AH6AOQJ8gIeICYOJSbQNjYWFmIxVgAAA04Qe5IaWgjOIFhWJM4gFt\nAEPibm0AM+IzbeEFogAy4h1tAjkwN0JM4gGHoR7iA15AIOJnbSCXAWJlIcMibQFiOeIWbeT/2+Rk\n2+EFogA44hxtAzYxMzgjkuJ9bQBhIl0iXyWmIMoCMTFiQA8DMjNlYyDKZMr"
				;
		CompressAlgorithm alg = RedisDataUtil.detectValueCompressAlgorithm(s);
		
		System.out.println("testDetectCompress1():" + alg);
	}

	@Test
	public void testCompress1() {
	    String s = "test1--------########";

	    try {
            String s2 = RedisDataUtil.encodeString(s, true);
            System.out.println("encoded:" + s2);

            String s3 = RedisDataUtil.decodeString(s2, true);
            if(!s.equals(s3)) {
                throw new RuntimeException("Failed to compress");
            }
        } catch (Throwable e) {
	        e.printStackTrace();
        }
    }

	@Test
	public void testDetectCompress2() {
		try {
			//String s = "abcdafafjdaljflajflajfja;lfjafjlajflajfafhafhkafhahfkahfkhafk2317832131hrkh41h87tysfda8yfashfh123h4h12h432h41487y9fhdifh";
			String s = readFileContent(new File("test2.xml"), Charset.forName("utf-8"));
			
			String encodedS;
			
			for(int i = 0; i < s.length(); i++) {
				encodedS = new String(RedisDataUtil.encodeStringBytes(s.substring(0, i + 1).getBytes("utf-8"), CompressAlgorithm.LZF), "utf-8");

				//System.out.println("[" + i + "] expect LZF, detected:" + RedisDataUtil.detectValueCompressAlgorithm(encodedS));
				if(RedisDataUtil.detectValueCompressAlgorithm(encodedS) != CompressAlgorithm.LZF) {
					System.out.println("[" + i + "] expect LZF, but failed");
				}
			}
			
			for(int i = 0; i < s.length(); i++) {
				encodedS = new String(RedisDataUtil.encodeStringBytes(s.substring(0, i + 1).getBytes("utf-8"), CompressAlgorithm.GZIP), "utf-8");
				//System.out.println("[" + i + "] expect GZIP, detected:" + RedisDataUtil.detectValueCompressAlgorithm(encodedS));
				if(RedisDataUtil.detectValueCompressAlgorithm(encodedS) != CompressAlgorithm.GZIP) {
					System.out.println("[" + i + "] expect GZIP, but failed");
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDetectCompress3() {
    	String s = "WlYBAXsCOxE8Q291cG9uSXRlbT4NCiAgPGNgDwRfaWQ+MGAACzIyNDAwNmRhOWQ8L+ABG4AqDXJlYXRlX3RpbWU+MTUyIAEGMzUzMzQzMyAp4AIaYCsHZXhwaXJlX2QgMAI+PC/gAw3gAx5gSgAw4AAfYA6gHwd0cmFfaW5mb2A94AAMYBwObGFzdF9udW1iZXI+MTwv4AMOYB8Nb3V0Ym91bmRfYmF0Y2jgANocMTYzMjAzMWMxOGYwMDU1ODUwMTgwMDRjOTVmPC/gCTNgSgJwd2QggEAFYA4fcXJfY29kZV91cmw+aHR0cDovL2QuZWJ1eS5pby9odXQOP0FpcHNQWm5hWWlYeTwv4AMuYD8CcmVmYEAOPjE3NjUzOTk2MzM3NDwv4AAWYCQBc2UgumEKIRngAQyAGwFuX0CACz4xMDA3NDI5MTAzOSEBwBWAIgZ0YXR1cz44QBeACmAWAXVzoV8gVOAAC2AZAnZvaeABb+ABDAQNCjwvQ2IcYiwBDQo="
				;
		CompressAlgorithm alg = RedisDataUtil.detectValueCompressAlgorithm(s);

		System.out.println("testDetectCompress1():" + alg);
        if(alg != CompressAlgorithm.LZF) {
            throw new RuntimeException("Failed to detect compress algorithm");
        }
	}

    @Test
    public void testDetectCompress4() {
        String s = "WlYBABAAFQV0ZXN0MS2gAAAjYAABIyM="
                ;
        CompressAlgorithm alg = RedisDataUtil.detectValueCompressAlgorithm(s);

        System.out.println("testDetectCompress1():" + alg);
        if(alg != CompressAlgorithm.LZF) {
            throw new RuntimeException("Failed to detect compress algorithm");
        }
    }

    public void testDecodeBase64ByHand() {
		String s = "AC0A";
		
		byte[] bytes = RedisDataUtil.decode3ByteBase64(
				s.charAt(0), s.charAt(1), s.charAt(2), s.charAt(3));
		
		int n = (bytes[0] << 8) & 0xff00 | bytes[1];
		
		System.out.println("n:" + n);
	}
	
	public void testEncode() {
		boolean isTestJedisSet = true;
		int loopCount = 1;
		String str = "a";
		testEncodeSpeed(str, CompressAlgorithm.LZF, isTestJedisSet, loopCount);
		testEncodeSpeed(str, CompressAlgorithm.GZIP, isTestJedisSet, loopCount);
	}
	
	public void testEncodeCompressAndJedisSetSpeed() {
		try {
			boolean isTestJedisSet = true;
			int loopCount = 2000;
			String str = readFileContent(new File("test2.xml"), Charset.forName("utf-8"));
			testEncodeSpeed(str, CompressAlgorithm.NotCompress, isTestJedisSet, loopCount);
			testEncodeSpeed(str, CompressAlgorithm.LZF, isTestJedisSet, loopCount);
			testEncodeSpeed(str, CompressAlgorithm.GZIP, isTestJedisSet, loopCount);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void testEncodeCompressSpeed() {
		try {
			boolean isTestJedisSet = false;
			int loopCount = 2000;
			String str = readFileContent(new File("test2.xml"), Charset.forName("utf-8"));
			testEncodeSpeed(str, CompressAlgorithm.NotCompress, isTestJedisSet, loopCount);
			testEncodeSpeed(str, CompressAlgorithm.LZF, isTestJedisSet, loopCount);
			testEncodeSpeed(str, CompressAlgorithm.GZIP, isTestJedisSet, loopCount);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void testEncodeRate(CompressAlgorithm algorithm) {
		//String str = "Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试";
		try {
			RedisDataUtil.setCompressAlgorithm(algorithm);
			
			String str = readFileContent(new File("test2.xml"), Charset.forName("utf-8"));
			byte[] bytes0 = str.getBytes("utf-8");
			int len = bytes0.length;
			
			byte[] bytes1 = RedisDataUtil.encodeStringBytes(bytes0, true);
			int lenGzipped = bytes1.length;
			
			String str3 = new String(RedisDataUtil.decodeStringBytes(bytes1, true), "utf-8");

			System.out.println("compress(" + algorithm + ") ratio:" + (((double)lenGzipped) / len) + " isEqual:" + str3.equals(str));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void testEncodeSpeed(String str, CompressAlgorithm algorithm, boolean isTestJedisSet, int loopCount) {
		//String str = "Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试";
		try {
			RedisDataUtil.setCompressAlgorithm(algorithm);

			
			long beginTime = System.currentTimeMillis();
			byte[] bytes1 = null;
			//int loopCount = 20000;
			String algo = "nocomp";
			if(algorithm == CompressAlgorithm.LZF) {
				algo = "lzf";
			} else if(algorithm == CompressAlgorithm.GZIP) {
				algo = "gzip";
			}
			for(int i = 0; i < loopCount; i++) {
				if(isTestJedisSet) {
					String key = "testEncodeSpeed." + algo + "." + i;
					Jedis jedis = null;
					try {
						jedis = _jedisPool.getResource();
						
						RedisDataUtil.set(jedis, key, str, true);
					} finally {
						_jedisPool.returnResource(jedis);
					}
				} else {
					byte[] bytes0 = str.getBytes("utf-8");
					bytes1 = RedisDataUtil.encodeStringBytes(bytes0, true);
				}
			}
			
			System.out.println("compress(" + algorithm + ") loopCount:" + loopCount + " time cost:" + (System.currentTimeMillis() - beginTime));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void testGZIP() {
		try {
			String str = "Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试Test测试";
			byte[] bytes = str.getBytes("utf-8");
			int bytesLen = bytes.length;
			
			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			GZIPOutputStream gzipOut = new GZIPOutputStream(bytesOut);
			
			gzipOut.write(bytes);
			gzipOut.close();
			
			byte[] bytesZipped = bytesOut.toByteArray();
			
			ByteArrayOutputStream bytesOutTmp = new ByteArrayOutputStream();
			Base64Encoder base64 = new Base64Encoder(new ByteArrayInputStream(bytesZipped), bytesOutTmp);
			base64.process();
			byte[] bytesZippedBase64 = bytesOutTmp.toByteArray();
			int len = bytesZippedBase64.length;
			
			GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(bytesZipped));
			
			bytesOut = new ByteArrayOutputStream();
			
			byte[] tempBuff = new byte[1024];
			int readCnt = gzipIn.read(tempBuff, 0, tempBuff.length);
			
			gzipIn.close();
			
			bytesOut.write(tempBuff, 0, readCnt);
			
			String str2 = new String(bytesOut.toByteArray(), "utf-8");
			System.out.println("str2:" + str2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static TestData1 createTestData1() {
		TestData1 data = new TestData1();
		
		data.setItem1("test1");
		data.setItem2("测试001");
		data.setItem3(1234);
		
		return data;
	}
	
	private boolean isDataSame(TestData1 dataA, TestData1 dataB) {
		if(!dataA.getItem1().equals(dataB.getItem1())) {
			return false;
		}
		if(!dataA.getItem2().equals(dataB.getItem2())) {
			return false;
		}
		
		if(dataA.getItem3() != dataB.getItem3()) {
			return false;
		}
		
		return true;
	}

	
	public static String readFileContent(File file, Charset charset) throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		
		readFile(file, bytesOut);
		
		return new String(bytesOut.toByteArray(), charset);
	}
	
	public static int readFile(File file, OutputStream output) throws IOException {
		FileInputStream fis = null;
		
		try {
			fis = new FileInputStream(file);
			
			return copy(fis, output);
		} finally {
			fis.close();
		}
		
	}
	
	public static int copy(InputStream input, OutputStream output) throws IOException {
		byte[] tempBuff = new byte[1024];
		int readCnt;
		int totalRead = 0;
		while(true) {
			readCnt = input.read(tempBuff, 0, tempBuff.length);
			
			if(readCnt < 0) {
				break;
			}
			
			totalRead += readCnt;

			if(readCnt > 0) {
				output.write(tempBuff, 0, readCnt);
				output.flush();
			}
		}
		
		return totalRead;
	}
	
}
