package com.beef.util.redis;

import java.beans.IntrospectionException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import MetoXML.Util.Base64Decoder;
import MetoXML.Util.Base64Encoder;
import MetoXML.Util.Base64FormatException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;
import MetoXML.XmlDeserializer;
import MetoXML.XmlSerializer;
import MetoXML.Base.XmlParseException;
import MetoXML.Cast.BaseTypesMapping;
import MetoXML.Util.ClassFinder;

import com.beef.util.redis.compress.CompressException;
import com.beef.util.redis.compress.GZipCompressor;
import com.beef.util.redis.compress.ICompressor;
import com.beef.util.redis.compress.LZFCompressor;

public class RedisDataUtil {
	public static enum CompressAlgorithm {NotCompress, GZIP, LZF};
	
	//default utf-8
	//protected static Charset _charset = Charset.forName("utf-8");
	protected static Charset _charset = Charset.forName(Protocol.CHARSET);

	protected static CompressAlgorithm _defaultCompressAlgorithm = CompressAlgorithm.LZF;
	protected static ICompressor _compressorOfLZF = new LZFCompressor();
	protected static ICompressor _compressorOfGZIP = new GZipCompressor();
	
	public static void setCompressAlgorithm(CompressAlgorithm algorithm) {
		if(algorithm == CompressAlgorithm.LZF) {
			_compressorOfLZF = new LZFCompressor();
		} else if (algorithm == CompressAlgorithm.GZIP) {
			_compressorOfGZIP = new GZipCompressor();
		}
		
		_defaultCompressAlgorithm = algorithm;
	}
	
	public static CompressAlgorithm detectValueCompressAlgorithm(String value) {
		//check wether length is compatible with base64
		int len = value.length();
		
		if(len < 8) {
			return CompressAlgorithm.NotCompress;
		}
		
		if(value.startsWith("WlY")) {
			//check signature of lzf
			if(value.charAt(3) == 'A' || value.charAt(3) == 'B') {
				//find out if multi chunk
				int nextChunkIndex = value.indexOf("WlY", 4);
				if(nextChunkIndex < 0) {
					nextChunkIndex = value.length();
				}
				int countExcludeReturnLine = 0;
				for(int i = 0; i < nextChunkIndex; i++) {
					if(value.charAt(i) != '\r' && value.charAt(i) != '\n') {
						countExcludeReturnLine++;
					}
				}
				if((countExcludeReturnLine % 4) != 0) {
					return CompressAlgorithm.NotCompress;
				}
				
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
			int countExcludeReturnLine = 0;
			for(int i = 0; i < value.length(); i++) {
				if(value.charAt(i) != '\r' && value.charAt(i) != '\n') {
					countExcludeReturnLine++;
				}
			}
			if((countExcludeReturnLine % 4) != 0) {
				return CompressAlgorithm.NotCompress;
			}

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
	
	/*
	public static void setCharset(Charset charset) {
		_charset = charset;
	}
	*/
	
	/**
	 * for compatible with older version (Jedis other than JedisCommands)
	 * @param jedis
	 * @param key
	 * @return
	 */
	public static long del(
			Jedis jedis,
			String key 
			) {
		return del((JedisCommands)jedis, key);
	}
	public static long del(
			JedisCommands jedis,
			String key 
			) {
		return jedis.del(key);
	}

	public static long incr(
			Jedis jedis,
			String key 
			) {
		return incr((JedisCommands)jedis, key);
	}
	public static long incr(
			JedisCommands jedis,
			String key 
			) {
		return jedis.incr(key);
	}
	
	public static long incrBy(
			Jedis jedis,
			String key, long integer
			) {
		return incrBy((JedisCommands)jedis, key, integer);
	}
	public static long incrBy(
			JedisCommands jedis,
			String key, long integer
			) {
		return jedis.incrBy(key, integer);
	}

	public static String set(
			Jedis jedis,
			String key, String val, boolean isUseCompress
			) throws IOException, CompressException {
		return set((JedisCommands)jedis, key, val, isUseCompress);
	}
	public static String set(
			JedisCommands jedis,
			String key, String val, boolean isUseCompress
			) throws IOException, CompressException {
		return jedis.set(key, encodeString(val, isUseCompress));
	}
	public static String setex(
			JedisCommands jedis,
			String key, int seconds, 
			String val, boolean isUseCompress
			) throws IOException, CompressException {
		return jedis.setex(key, seconds, encodeString(val, isUseCompress));
	}
	
	public static String get(
			Jedis jedis,
			String key, boolean isUseCompress 
			) throws IOException, Base64FormatException, CompressException {
		return get((JedisCommands)jedis, key, isUseCompress);
	}
	public static String get(
			JedisCommands jedis,
			String key, boolean isUseCompress 
			) throws IOException, Base64FormatException, CompressException {
		return decodeString(jedis.get(key), isUseCompress);
	}
	
	public static String set(
			Jedis jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return set((JedisCommands)jedis, key, data, dataClass, isUseCompress);
	}
	public static String set(
			JedisCommands jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.set(key, 
				serializeDataToString(data, dataClass, isUseCompress));
	}
	
	public static String setex(
			JedisCommands jedis,
			String key, int seconds, 
			Object data, Class<?> dataClass, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.setex(key, seconds, 
				serializeDataToString(data, dataClass, isUseCompress));
	}
	
	public static Object get(
			Jedis jedis,
			String key, Class<?> dataClass, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return get((JedisCommands)jedis, key, dataClass, isUseCompress);
	}
	public static Object get(
			JedisCommands jedis,
			String key, Class<?> dataClass, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return get(jedis, key, dataClass, isUseCompress, null);
	}
	
	public static Object get(
			Jedis jedis,
			String key, Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return get((JedisCommands)jedis, key, dataClass, isUseCompress, classFinder);
	}
	public static Object get(
			JedisCommands jedis,
			String key, Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return deserializeData(jedis.get(key), 
				dataClass, isUseCompress, classFinder);
	}
	
	public static long llen(
			Jedis jedis,
			String key
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException {
		return llen((JedisCommands)jedis, key);
	}
	public static long llen(
			JedisCommands jedis,
			String key
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException {
		return jedis.llen(key);
	}
	
	public static String lindex(
			Jedis jedis,
			String key, long index, 
			boolean isUseCompress
			) throws IOException, Base64FormatException, CompressException {
		return lindex((JedisCommands)jedis, key, index, isUseCompress);
	}
	public static String lindex(
			JedisCommands jedis,
			String key, long index, 
			boolean isUseCompress
			) throws IOException, Base64FormatException, CompressException {
		return decodeString(jedis.lindex(key, index), isUseCompress);
	}

	public static List<String> lrange(
			Jedis jedis,
			String key, long start, long end, 
			boolean isUseCompress
			) throws IOException, Base64FormatException, CompressException {
		return lrange((JedisCommands)jedis, key, start, end, isUseCompress);
	}
	public static List<String> lrange(
			JedisCommands jedis,
			String key, long start, long end, 
			boolean isUseCompress
			) throws IOException, Base64FormatException, CompressException {
		if(!isUseCompress 
				//&& _charset.name().equalsIgnoreCase("utf-8")
				) {
			return jedis.lrange(key, start, end);
		} else {
			List<String> strList = jedis.lrange(key, start, end);
			if(strList == null) {
				return null;
			} else {
				List<String> valueList = new ArrayList<String>();
				for(int i = 0; i < strList.size(); i++) {
					valueList.add(decodeString(strList.get(i), isUseCompress));
				}
				
				return valueList;
			}
		}
	}
	
	public static String lpop(
			Jedis jedis,
			String key, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return lpop((JedisCommands)jedis, key, isUseCompress);
	}
	public static String lpop(
			JedisCommands jedis,
			String key, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return decodeString(jedis.lpop(key), isUseCompress);
	}

	public static String lset(
			Jedis jedis,
			String key, long index, 
			String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return lset((JedisCommands)jedis, key, index, value, isUseCompress);
	}
	public static String lset(JedisCommands jedis,
			String key, long index, 
			String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.lset(key, index, encodeString(value, isUseCompress));
	}
	
	public static long rpush(Jedis jedis,
			String key, String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return rpush((JedisCommands)jedis, key, value, isUseCompress);
	}
	public static long rpush(JedisCommands jedis,
			String key, String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.rpush(key, encodeString(value, isUseCompress));
	}
	
	public static long lpush(Jedis jedis,
			String key, String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return lpush((JedisCommands)jedis, key, value, isUseCompress);
	}
	public static long lpush(JedisCommands jedis,
			String key, String value, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.lpush(key, encodeString(value, isUseCompress));
	}

	public static Object lindex(
			Jedis jedis,
			String key, long index, 
			Class<?> dataClass, 
			boolean isUseCompress
			) throws IOException, Base64FormatException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, CompressException {
		return lindex((JedisCommands)jedis, key, index, dataClass, isUseCompress);
	}
	public static Object lindex(
			JedisCommands jedis,
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
		return lindex((JedisCommands)jedis, key, index, dataClass, isUseCompress, classFinder);
	}
	public static Object lindex(
			JedisCommands jedis,
			String key, long index, 
			Class<?> dataClass, 
			boolean isUseCompress,
			ClassFinder classFinder
			) throws IOException, Base64FormatException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, CompressException {
		return deserializeData(jedis.lindex(key, index), 
				dataClass, isUseCompress, classFinder);
	}

	public static List<Object> lrange(
			Jedis jedis,
			String key, long start, long end, 
			Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws IOException, Base64FormatException, CompressException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException {
		return lrange((JedisCommands)jedis, key, start, end, dataClass, isUseCompress, classFinder);
	}
	public static List<Object> lrange(
			JedisCommands jedis,
			String key, long start, long end, 
			Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws IOException, Base64FormatException, CompressException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException {
		List<String> strList = jedis.lrange(key, start, end);
		if(strList == null) {
			return null;
		} else {
			List<Object> valueList = new ArrayList<Object>();
			for(int i = 0; i < strList.size(); i++) {
				valueList.add(
						deserializeData(strList.get(i), dataClass, isUseCompress, classFinder)
						);
			}
			
			return valueList;
		}
	}
	
	public static Object lpop(
			Jedis jedis,
			String key, Class<?> dataClass, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return lpop((JedisCommands)jedis, key, dataClass, isUseCompress);
	}
	public static Object lpop(
			JedisCommands jedis,
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
		return lpop((JedisCommands)jedis, key, dataClass, isUseCompress, classFinder);
	}
	public static Object lpop(
			JedisCommands jedis,
			String key, 
			Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return deserializeData(jedis.lpop(key), dataClass, isUseCompress, classFinder);
	}
	
	public static String lset(
			Jedis jedis,
			String key, long index, 
			Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return lset((JedisCommands)jedis, key, index, data, dataClass, isUseCompress);
	}
	public static String lset(
			JedisCommands jedis,
			String key, long index, 
			Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.lset(key, index, serializeDataToString(data, dataClass, isUseCompress));
	}
	
	public static long rpush(
			Jedis jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return rpush((JedisCommands)jedis, key, data, dataClass, isUseCompress);
	}
	public static long rpush(
			JedisCommands jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.rpush(key, serializeDataToString(data, dataClass, isUseCompress));
	}

	public static long lpush(
			Jedis jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return lpush((JedisCommands)jedis, key, data, dataClass, isUseCompress);
	}
	public static long lpush(
			JedisCommands jedis,
			String key, Object data, Class<?> dataClass, boolean isUseCompress
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.lpush(key, serializeDataToString(data, dataClass, isUseCompress));
	}
	
	public static Object hdel(
			Jedis jedis,
			String key, String field
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException {
		return hdel((JedisCommands)jedis, key, field);
	}
	public static Object hdel(
			JedisCommands jedis,
			String key, String field
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException {
		return jedis.hdel(key, field);
	}

	public static String hget(
			Jedis jedis,
			String key, String field, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return hget((JedisCommands)jedis, key, field, isUseCompress);
	}
	public static String hget(
			JedisCommands jedis,
			String key, String field, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return decodeString(jedis.hget(key, field), isUseCompress);
	}

	public static long hset(
			Jedis jedis,
			String key, String field, String value, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return hset((JedisCommands)jedis, key, field, value, isUseCompress);
	}
	public static long hset(
			JedisCommands jedis,
			String key, String field, String value, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.hset(key, field, encodeString(value, isUseCompress));
	}
	
	public static Object hget(
			Jedis jedis,
			String key, String field, Class<?> dataClass, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return hget((JedisCommands)jedis, key, field, dataClass, isUseCompress);
	}
	public static Object hget(
			JedisCommands jedis,
			String key, String field, Class<?> dataClass, boolean isUseCompress
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return hget(jedis, key, field, dataClass, isUseCompress, null);
	}
	
	public static Object hget(
			Jedis jedis,
			String key, String field, Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return hget((JedisCommands)jedis, key, field, dataClass, isUseCompress, classFinder);
	}
	public static Object hget(
			JedisCommands jedis,
			String key, String field, Class<?> dataClass, boolean isUseCompress, 
			ClassFinder classFinder
			) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return deserializeData(jedis.hget(key, field), dataClass, isUseCompress, classFinder);
	}

	public static long hset(
			Jedis jedis,
			String key, String field, Object data, Class<?> dataClass, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return hset((JedisCommands)jedis, key, field, data, dataClass, isUseCompress);
	}
	public static long hset(
			JedisCommands jedis,
			String key, String field, Object data, Class<?> dataClass, boolean isUseCompress 
			) throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		return jedis.hset(key, field, serializeDataToString(data, dataClass, isUseCompress));
	}
	
	public static String serializeDataToString(Object data, Class<?> dataClass, boolean isUseCompress) 
			throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		String dataXml = XmlSerializer.objectToString(data, dataClass);
		return encodeString(dataXml, isUseCompress);
	}
	
	public static byte[] serializeData(Object data, Class<?> dataClass, boolean isUseCompress) 
			throws IntrospectionException, IllegalAccessException, InvocationTargetException, IOException, CompressException {
		String dataXml = XmlSerializer.objectToString(data, dataClass);
		
		return encodeStringBytes(dataXml.getBytes(_charset), isUseCompress);
	}

	public static Object deserializeData(String str, Class<?> dataClass, boolean isUseCompress) 
			throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		if(str == null) {
			return null;
		}
		
		return deserializeData(str, dataClass, isUseCompress, null);
	}
	
	public static Object deserializeData(byte[] dataBytes, Class<?> dataClass, boolean isUseCompress) 
			throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, Base64FormatException, CompressException {
		return deserializeData(dataBytes, dataClass, isUseCompress, null);
	}

	public static Object deserializeData(String str, Class<?> dataClass, boolean isUseCompress, ClassFinder classFinder) 
			throws IOException, Base64FormatException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, CompressException {
		if(str == null) {
			return null;
		}
		
		String dataXml = decodeString(str, isUseCompress);
		return deserializeDataXml(dataXml, dataClass, classFinder);
	}
	
	public static Object deserializeData(byte[] dataBytes, Class<?> dataClass, boolean isUseCompress, ClassFinder classFinder) 
			throws IOException, Base64FormatException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException, CompressException {
		byte[] bytes = decodeStringBytes(dataBytes, isUseCompress);
		String dataXml = new String(bytes, _charset);
	
		return deserializeDataXml(dataXml, dataClass, classFinder);
	}
	
	private final static Object deserializeDataXml(String dataXml, Class<?> dataClass, ClassFinder classFinder) throws IOException, XmlParseException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException {
		Object data = XmlDeserializer.stringToObject(dataXml, dataClass, classFinder);
		checkClassFinder(data, classFinder);
		
		return data;
	}
	
	public static String encodeString(String string, boolean isUseCompress) throws IOException, CompressException {
		if(string == null || string.length() == 0) {
			return string;
		}
		
		CompressAlgorithm compressAlgorithm = (isUseCompress?_defaultCompressAlgorithm : CompressAlgorithm.NotCompress);
		if(compressAlgorithm == CompressAlgorithm.NotCompress) {
			return string;
		} else {
			return new String(encodeStringBytes(string.getBytes(_charset), compressAlgorithm), _charset);
		}
	}
	
	public static byte[] encodeStringBytes(byte[] stringBytes, boolean isUseCompress) throws IOException, CompressException {
		return encodeStringBytes(stringBytes, isUseCompress?_defaultCompressAlgorithm : CompressAlgorithm.NotCompress);
	}
	
	public static byte[] encodeStringBytes(byte[] stringBytes, CompressAlgorithm compressAlgorithm) throws IOException, CompressException {
		if(compressAlgorithm != CompressAlgorithm.NotCompress) {
			ICompressor compressor;
			if(compressAlgorithm == CompressAlgorithm.LZF) {
				compressor = _compressorOfLZF;
			} else {
				compressor = _compressorOfGZIP;
			}
			
			//compress
			byte[] bytesCompressed = compressor.compress(stringBytes);
			
			//encode to base64
			ByteArrayOutputStream bytesEncoded = new ByteArrayOutputStream();
			Base64Encoder base64 = new Base64Encoder(new ByteArrayInputStream(bytesCompressed), bytesEncoded);
			base64.process();
			
			return bytesEncoded.toByteArray();
		} else {
			return stringBytes;
		}
	}

	public static String decodeString(String string, boolean isUseCompress) throws IOException, Base64FormatException, CompressException {
		if(string == null || string.length() == 0) {
			return string;
		}
		
		CompressAlgorithm compressAlgorithm = (isUseCompress?_defaultCompressAlgorithm : CompressAlgorithm.NotCompress);
		if(compressAlgorithm == CompressAlgorithm.NotCompress) {
			return string;
		} else {
			return new String(decodeStringBytes(string.getBytes(_charset), compressAlgorithm), _charset);
		}
	}
	
	public static byte[] decodeStringBytes(byte[] stringBytes, boolean isUseCompress) throws IOException, Base64FormatException, CompressException {
		return decodeStringBytes(stringBytes, isUseCompress?_defaultCompressAlgorithm : CompressAlgorithm.NotCompress);
	}
	
	public static byte[] decodeStringBytes(byte[] stringBytes, CompressAlgorithm compressAlgorithm) throws IOException, Base64FormatException, CompressException {
		if(compressAlgorithm != CompressAlgorithm.NotCompress) {
			ICompressor compressor;
			if(compressAlgorithm == CompressAlgorithm.LZF) {
				compressor = _compressorOfLZF;
			} else {
				compressor = _compressorOfGZIP;
			}
			
			//decode from base64
			ByteArrayOutputStream bytesDecoded = new ByteArrayOutputStream();
			Base64Decoder base64 = new Base64Decoder(new ByteArrayInputStream(stringBytes), bytesDecoded);
			base64.process();
			
			//decompress
			return compressor.decompress(bytesDecoded.toByteArray());
		} else {
			return stringBytes;
		}
	}
	
	private static void checkClassFinder(Object data, ClassFinder classFinder) {
		if(classFinder == null 
				&& data != null
				&& List.class.isAssignableFrom(data.getClass())
				) {
			List<?> list = (List<?>) data;
			if(list.size() > 0) {
				if(!BaseTypesMapping.IsSupportedBaseType(list.get(0).getClass())) {
					throw new RuntimeException("ClassFinder must be assigned when element type of List is not primitive type. (Operation of finding properties through default ClassLoader is very slow when threads access simultaneously)");
				}
			}
		}
	}
}
