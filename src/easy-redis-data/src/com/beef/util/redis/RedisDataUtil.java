package com.beef.util.redis;

import java.beans.IntrospectionException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.w3c.tools.codec.Base64Decoder;
import org.w3c.tools.codec.Base64Encoder;
import org.w3c.tools.codec.Base64FormatException;

import com.beef.util.redis.compress.CompressException;
import com.beef.util.redis.compress.GZipCompressor;
import com.beef.util.redis.compress.ICompressor;
import com.beef.util.redis.compress.LZFCompressor;

import redis.clients.jedis.Jedis;
import MetoXML.XmlDeserializer;
import MetoXML.XmlSerializer;
import MetoXML.Base.XmlParseException;
import MetoXML.Util.ClassFinder;

public class RedisDataUtil {
	public static enum CompressAlgorithm {NotCompress, GZIP, LZF};
	
	//default utf-8
	protected static Charset _charset = Charset.forName("utf-8");

	protected static CompressAlgorithm _compressAlgorithm = CompressAlgorithm.LZF;
	protected static ICompressor _compressor = new LZFCompressor();
	
	public static void setCompressAlgorithm(CompressAlgorithm algorithm) {
		if(algorithm == CompressAlgorithm.LZF) {
			_compressor = new LZFCompressor();
		} else if (algorithm == CompressAlgorithm.GZIP) {
			_compressor = new GZipCompressor();
		}
		
		_compressAlgorithm = algorithm;
	}
	
	public static CompressAlgorithm detectValueCompressAlgorithm(String value) {
		//check wether length is compatible with base64
		int len = value.length();
		
		if(len < 8) {
			return CompressAlgorithm.NotCompress;
		}
		
		int countExcludeReturnLine = 0;
		for(int i = 0; i < value.length(); i++) {
			if(value.charAt(i) != '\r' && value.charAt(i) != '\n') {
				countExcludeReturnLine++;
			}
		}
		if((countExcludeReturnLine % 4) != 0) {
			return CompressAlgorithm.NotCompress;
		}
		
		if(value.startsWith("WlY")) {
			//check signature of lzf
			if(value.charAt(3) == 'A' || value.charAt(3) == 'B') {
				//check chunk len
				byte[] lenBytes = decode3ByteBase64(value.charAt(4), value.charAt(5), value.charAt(6), value.charAt(7));
				int chunkLen = ((byte)lenBytes[0] << 8) & 0xff00 | (lenBytes[1] & 0xff);
				
				int padLen = 0;
				if(value.charAt(value.length() - 1) == '=') {
					padLen ++;
				}
				if(value.charAt(value.length() - 2) == '=') {
					padLen ++;
				}
				
				int lenOfOriginLen = (value.charAt(3) == 'B' ? 2 : 0);
				
				int calcuChunkLen = ((countExcludeReturnLine - 8) / 4) * 3 + 1 - padLen - lenOfOriginLen;
				if(calcuChunkLen == chunkLen) {
					return CompressAlgorithm.LZF;
				}
			}
		} else if (value.length() >= 16) {
			//check signature of gzip
			byte[] bytes = decode3ByteBase64(
					value.charAt(0), value.charAt(1), value.charAt(2), value.charAt(3));
			if(bytes[0] == (byte) 0x1f && bytes[1] == (byte) 0x8b && bytes[2] == (byte) 0x08) {
				if(value.charAt(5) == 'A' && value.charAt(6) == 'A' && value.charAt(7) == 'A' 
						&& value.charAt(8) == 'A' && value.charAt(9) == 'A' ) {
					return CompressAlgorithm.GZIP;
				}
			}
		}
		
		
		return CompressAlgorithm.NotCompress;
	}
	
	public static byte[] decode3ByteBase64(char base64chr0, char base64chr1, char base64chr2, char base64chr3) {
		byte[] bytes = new byte[3];
		byte v;
		
		//char 0
		v = decodeBase64Char(base64chr0);
		bytes[0] = (byte) ((v << 2) & 0xfc);
		
		//char 1
		v = decodeBase64Char(base64chr1);
		bytes[0] |= (byte) ((v >> 4) & 0x03);
		bytes[1] = (byte) ((v << 4) & 0xf0);
		
		//char 2
		v = decodeBase64Char(base64chr2);
		bytes[1] |= (byte) ((v >> 2) & 0x0f);
		bytes[2] = (byte) ((v << 6) & 0xc0);
		
		//char 3
		v = decodeBase64Char(base64chr3);
		bytes[2] |= v;
		
		return bytes;
	}
	
	private static byte decodeBase64Char(char base64chr) {
		if (base64chr == '+') {
			return 62;
		} else if (base64chr == '/') {
			return 63;
		} else if (base64chr == '=') {
			return 0;
		}
		
		byte c = (byte) base64chr;
		if(c >= 0x30 && c <= 0x39) {
			return (byte) (c + 4);
		} else if (c >= 0x41 && c <= 0x5A) {
			return (byte) (c - 0x41);
		} else if (c >= 0x61 && c <= 0x7A) {
			return (byte) (c - 71);
		} else {
			return 0;
		}
	}
	
	public static void setCharset(Charset charset) {
		_charset = charset;
	}
	
	public static long del(
			Jedis jedis,
			String key 
			) {
		return jedis.del(key.getBytes(_charset));
	}

	public static long incr(
			Jedis jedis,
			String key 
			) {
		return jedis.incr(key.getBytes(_charset));
	}
	
	public static long incrBy(
			Jedis jedis,
			String key, long integer
			) {
		return jedis.incrBy(key.getBytes(_charset), integer);
	}

	public static String set(
			Jedis jedis,
			String key, String val, boolean isUseCompress
			) throws IOException, CompressException {
		return jedis.set(key.getBytes(_charset), encodeStringBytes(val.getBytes(_charset), isUseCompress));
	}
	
	public static String get(
			Jedis jedis,
			String key, boolean isUseCompress 
			) throws IOException, Base64FormatException, CompressException {
		byte[] val = jedis.get(key.getBytes(_charset));
		if(val == null) {
			return null;
		} else {
			return new String(decodeStringBytes(val, isUseCompress), _charset);
		}
	}
	
	public static String set(
			Jedis jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = serializeData(data, dataClass, isUseCompress);
		
		return jedis.set(key.getBytes(_charset), dataBytes);
	}
	
	public static Object get(
			Jedis jedis,
			String key, Class<?> dataClass, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return get(jedis, key, dataClass, isUseCompress, null);
	}
	
	public static Object get(
			Jedis jedis,
			String key, Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		byte[] dataBytes = jedis.get(key.getBytes(_charset));
		if(dataBytes == null) {
			return null;
		} else {
			return deserializeData(dataBytes, dataClass, isUseCompress, classFinder);
		}
	}
	
	public static long llen(
			Jedis jedis,
			String key
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException {
		return jedis.llen(key.getBytes(_charset));
	}
	
	public static String lindex(
			Jedis jedis,
			String key, long index, 
			boolean isUseCompress
			) throws IOException, Base64FormatException, CompressException {
		byte[] bytes = jedis.lindex(key.getBytes(_charset), index);
		if(bytes == null) {
			return null;
		} else {
			return new String(decodeStringBytes(bytes, isUseCompress), _charset);
		}
	}

	public static List<String> lrange(
			Jedis jedis,
			String key, long start, long end, 
			boolean isUseCompress
			) throws IOException, Base64FormatException, CompressException {
		if(!isUseCompress && _charset.name().equalsIgnoreCase("utf-8")) {
			return jedis.lrange(key, start, end);
		} else {
			List<byte[]> bytesList = jedis.lrange(key.getBytes(_charset), start, end);
			if(bytesList == null) {
				return null;
			} else {
				List<String> valueList = new ArrayList<String>();
				for(int i = 0; i < bytesList.size(); i++) {
					valueList.add(new String(decodeStringBytes(bytesList.get(i), isUseCompress), _charset));
				}
				
				return valueList;
			}
		}
	}
	
	public static String lpop(
			Jedis jedis,
			String key, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		byte[] bytes = jedis.lpop(key.getBytes(_charset));
		if(bytes == null) {
			return null;
		} else {
			return new String(decodeStringBytes(bytes, isUseCompress), _charset);
		}
	}

	public static String lset(Jedis jedis,
			String key, long index, 
			String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = encodeStringBytes(value.getBytes(_charset), isUseCompress);
		return jedis.lset(key.getBytes(_charset), index, dataBytes);
	}
	
	public static long rpush(Jedis jedis,
			String key, String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = encodeStringBytes(value.getBytes(_charset), isUseCompress);
		return jedis.rpush(key.getBytes(_charset), dataBytes);
	}
	
	public static long lpush(Jedis jedis,
			String key, String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = encodeStringBytes(value.getBytes(_charset), isUseCompress);
		return jedis.lpush(key.getBytes(_charset), dataBytes);
	}

	public static Object lindex(
			Jedis jedis,
			String key, long index, 
			Class<?> dataClass, 
			boolean isUseCompress
			) throws IOException, Base64FormatException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, CompressException {
		return lindex(jedis, key, index, dataClass, isUseCompress, null);
	}

	public static Object lindex(
			Jedis jedis,
			String key, long index, 
			Class<?> dataClass, 
			boolean isUseCompress,
			ClassFinder classFinder
			) throws IOException, Base64FormatException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, CompressException {
		byte[] dataBytes = jedis.lindex(key.getBytes(_charset), index);
		if(dataBytes == null) {
			return null;
		} else {
			return deserializeData(dataBytes, dataClass, isUseCompress, classFinder);
		}
	}

	public static List<Object> lrange(
			Jedis jedis,
			String key, long start, long end, 
			Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws IOException, Base64FormatException, CompressException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException {
		List<byte[]> bytesList = jedis.lrange(key.getBytes(_charset), start, end);
		if(bytesList == null) {
			return null;
		} else {
			List<Object> valueList = new ArrayList<Object>();
			for(int i = 0; i < bytesList.size(); i++) {
				valueList.add(
						deserializeData(bytesList.get(i), dataClass, isUseCompress, classFinder)
						);
			}
			
			return valueList;
		}
	}
	
	public static Object lpop(
			Jedis jedis,
			String key, Class<?> dataClass, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return lpop(jedis, key, dataClass, isUseCompress, null);
	}

	public static Object lpop(
			Jedis jedis,
			String key, 
			Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		byte[] dataBytes = jedis.lpop(key.getBytes(_charset));
		if(dataBytes == null) {
			return null;
		} else {
			return deserializeData(dataBytes, dataClass, isUseCompress, classFinder);
		}
	}
	
	public static String lset(
			Jedis jedis,
			String key, long index, 
			Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = serializeData(data, dataClass, isUseCompress);
		return jedis.lset(key.getBytes(_charset), index, dataBytes);
	}
	
	public static long rpush(
			Jedis jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = serializeData(data, dataClass, isUseCompress);
		return jedis.rpush(key.getBytes(_charset), dataBytes);
	}

	public static long lpush(
			Jedis jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = serializeData(data, dataClass, isUseCompress);
		return jedis.lpush(key.getBytes(_charset), dataBytes);
	}
	
	public static Object hdel(
			Jedis jedis,
			String key, String field
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException {
		return jedis.hdel(key.getBytes(_charset), field.getBytes(_charset));
	}

	public static String hget(
			Jedis jedis,
			String key, String field, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		byte[] dataBytes = jedis.hget(key.getBytes(_charset), field.getBytes(_charset));
		if(dataBytes == null) {
			return null;
		} else {
			return new String(decodeStringBytes(dataBytes, isUseCompress), _charset);
		}
	}

	public static long hset(
			Jedis jedis,
			String key, String field, String value, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = encodeStringBytes(value.getBytes(_charset), isUseCompress);
		return jedis.hset(key.getBytes(_charset), field.getBytes(_charset), dataBytes);
	}
	
	public static Object hget(
			Jedis jedis,
			String key, String field, Class<?> dataClass, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return hget(jedis, key, field, dataClass, isUseCompress, null);
	}
	
	public static Object hget(
			Jedis jedis,
			String key, String field, Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		byte[] dataBytes = jedis.hget(key.getBytes(_charset), field.getBytes(_charset));
		if(dataBytes == null) {
			return null;
		} else {
			return deserializeData(dataBytes, dataClass, isUseCompress, classFinder);
		}
	}

	public static long hset(
			Jedis jedis,
			String key, String field, Object data, Class<?> dataClass, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = serializeData(data, dataClass, isUseCompress);
		return jedis.hset(key.getBytes(_charset), field.getBytes(_charset), dataBytes);
	}
	
	public static byte[] serializeData(Object data, Class<?> dataClass, boolean isUseCompress) 
			throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		String dataXml = XmlSerializer.objectToString(data, dataClass);
		
		return encodeStringBytes(dataXml.getBytes(_charset), isUseCompress);
	}
	
	public static Object deserializeData(byte[] dataBytes, Class<?> dataClass, boolean isUseCompress) 
			throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return deserializeData(dataBytes, dataClass, isUseCompress, null);
	}
	
	public static Object deserializeData(byte[] dataBytes, Class<?> dataClass, boolean isUseCompress, ClassFinder classFinder) 
			throws IOException, Base64FormatException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, CompressException {
		byte[] bytes = decodeStringBytes(dataBytes, isUseCompress);
		
		String xmlStr = new String(bytes, _charset);
		
		return XmlDeserializer.stringToObject(xmlStr, dataClass, classFinder);
	}
	
	public static byte[] encodeStringBytes(byte[] stringBytes, boolean isUseCompress) throws IOException, CompressException {
		if(isUseCompress && _compressAlgorithm != CompressAlgorithm.NotCompress) {
			//compress
			byte[] bytesCompressed = _compressor.compress(stringBytes);
			
			//encode to base64
			ByteArrayOutputStream bytesEncoded = new ByteArrayOutputStream();
			Base64Encoder base64 = new Base64Encoder(new ByteArrayInputStream(bytesCompressed), bytesEncoded);
			base64.process();
			
			return bytesEncoded.toByteArray();
		} else {
			return stringBytes;
		}
	}

	public static byte[] decodeStringBytes(byte[] stringBytes, boolean isUseCompress) throws IOException, Base64FormatException, CompressException {
		if(isUseCompress && _compressAlgorithm != CompressAlgorithm.NotCompress) {
			//decode from base64
			ByteArrayOutputStream bytesDecoded = new ByteArrayOutputStream();
			Base64Decoder base64 = new Base64Decoder(new ByteArrayInputStream(stringBytes), bytesDecoded);
			base64.process();
			
			//decompress
			return _compressor.decompress(bytesDecoded.toByteArray());
		} else {
			return stringBytes;
		}
	}
	
}
