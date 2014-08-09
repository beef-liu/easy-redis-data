package com.beef.util.redis;

import java.beans.IntrospectionException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
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

	public static long rpush(Jedis jedis,
			String key, String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = encodeStringBytes(value.getBytes(_charset), isUseCompress);
		return jedis.rpush(key.getBytes(_charset), dataBytes);
	}
	
	public static Object lpop(
			Jedis jedis,
			String key, Class<?> dataClass, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return lpop(jedis, key, dataClass, isUseCompress, null);
	}

	public static Object lpop(
			Jedis jedis,
			String key, Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		byte[] dataBytes = jedis.lpop(key.getBytes(_charset));
		if(dataBytes == null) {
			return null;
		} else {
			return deserializeData(dataBytes, dataClass, isUseCompress, classFinder);
		}
	}
	
	public static long rpush(Jedis jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		byte[] dataBytes = serializeData(data, dataClass, isUseCompress);
		return jedis.rpush(key.getBytes(_charset), dataBytes);
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
