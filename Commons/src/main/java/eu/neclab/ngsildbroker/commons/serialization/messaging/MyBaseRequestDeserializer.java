package eu.neclab.ngsildbroker.commons.serialization.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.InflaterOutputStream;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.smallrye.mutiny.tuples.Tuple2;

public class MyBaseRequestDeserializer {

	private Field field;

	private static final Decoder base64Decoder = Base64.getDecoder();
	private static final TypeReference<Map<String, Object>> mapTypeRef = new TypeReference<Map<String, Object>>() {
	};

	public MyBaseRequestDeserializer() {
		try {
			field = String.class.getDeclaredField("value");
			field.setAccessible(true);
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

	}

	public BaseRequest deserialize(String body)
			throws IOException, JacksonException, IllegalArgumentException, IllegalAccessException {
		BaseRequest result = new BaseRequest();
		final byte[] chars = (byte[]) field.get(body);
		final int len = chars.length;

		for (int i = 2; i < chars.length; i++) {
			switch (chars[i]) {
			case AppConstants.BYTE_ZIPPED_SERIALIZATION_CHAR: {
				result.setZipped(chars[i + 3] == 't');
				i += 7;
				break;
			}
			case AppConstants.BYTE_TENANT_SERIALIZATION_CHAR: {
				int offset = i + 4;
				int endIndex = find(chars, '"', offset);
				result.setTenant(new String(chars, offset, endIndex - offset));
				i += endIndex + 3;
				break;
			}

			case AppConstants.BYTE_REQUESTTYPE_SERIALIZATION_CHAR: {
				int offset = i + 3;
				int endIndex = find(chars, ',', offset);
				result.setRequestType(Integer.parseInt(new String(chars, offset, endIndex - offset)));
				i += endIndex + 2;
				break;
			}
			case AppConstants.BYTE_SENDTIMESTAMP_SERIALIZATION_CHAR: {
				int offset = i + 3;
				int endIndex = find(chars, ',', offset);
				result.setSendTimestamp(Long.parseLong(new String(chars, offset, endIndex - offset)));
				i += endIndex + 2;
				break;
			}

			case AppConstants.BYTE_ATTRIBNAME_SERIALIZATION_CHAR: {
				int offset = i + 4;
				int endIndex = find(chars, '"', offset);
				result.setAttribName(new String(chars, offset, endIndex - offset));
				i += endIndex + 3;
				break;
			}
			case AppConstants.BYTE_DATASETID_SERIALIZATION_CHAR: {
				int offset = i + 4;
				int endIndex = find(chars, '"', offset);
				result.setDatasetId(new String(chars, offset, endIndex - offset));
				i += endIndex + 3;
				break;
			}
			case AppConstants.BYTE_DELETEALL_SERIALIZATION_CHAR: {
				result.setDeleteAll(chars[i + 3] == 't');
				i += 7;
				break;
			}
			case AppConstants.BYTE_DISTRIBUTED_SERIALIZATION_CHAR: {
				result.setDistributed(chars[i + 3] == 't');
				i += 7;
				break;
			}
			case AppConstants.BYTE_NOOVERWRITE_SERIALIZATION_CHAR: {
				result.setNoOverwrite(chars[i + 3] == 't');
				i += 7;
				break;
			}
			case AppConstants.BYTE_INSTANCEID_SERIALIZATION_CHAR: {
				int offset = i + 4;
				int endIndex = find(chars, '"', offset);
				result.setInstanceId(new String(chars, offset, endIndex - offset));
				i += endIndex + 3;
				break;
			}
			case AppConstants.BYTE_PAYLOAD_SERIALIZATION_CHAR: {
				int end = chars.length - 2;
				int start = i + 3;
				Set<String> ids = Sets.newHashSet();
				Map<String, List<Map<String, Object>>> payloadMap = Maps.newHashMap();
				Map<String, List<Map<String, Object>>> prevPayloadMap = Maps.newHashMap();
				for (int j = start; j < end; j++) {
					int offset = j + 1;
					int endIndex = find(chars, '"', j + 1);

					String id = new String(chars, offset, endIndex - offset);
					ids.add(id);

					j += endIndex + 1;
					Tuple2<Integer, Map<String, Object>> t = parsePayload(chars, j, end, result.isZipped());
					Map<String, Object> payload = t.getItem2();
					j = t.getItem1();
					t = parsePayload(chars, j, end, result.isZipped());
					Map<String, Object> prevPayload = t.getItem2();
					j = t.getItem1();
					if (payload != null) {
						MicroServiceUtils.putIntoIdMap(payloadMap, id, payload);
					}
					if (prevPayload != null) {
						MicroServiceUtils.putIntoIdMap(prevPayloadMap, id, prevPayload);
					}

				}
				if (!payloadMap.isEmpty()) {
					result.setPayload(payloadMap);
				}
				if (!prevPayloadMap.isEmpty()) {
					result.setPrevPayload(prevPayloadMap);
				}
				result.setIds(ids);
				break;
			}
			}
		}
		return result;
	}

	private Tuple2<Integer, Map<String, Object>> parsePayload(byte[] chars, int offset, int end, boolean zipped) {

		Map<String, Object> result;
		int resultOffset;
		if (zipped) {
			if (Arrays.equals(chars, offset, MicroServiceUtils.getZippedNullArrayLength(),
					MicroServiceUtils.getZippedNullArray(), 0, MicroServiceUtils.getZippedNullArrayLength())) {
				result = null;
				resultOffset = offset + MicroServiceUtils.getZippedNullArrayLength() + 1;
			} else {
				int b64Start = offset + 1;
				int b64End = find(chars, '"', b64Start);
				int length = b64End - b64Start;
				byte[] tmp = new byte[length];
				System.arraycopy(chars, b64Start, tmp, 0, length);
				try {
					result = parseMap(MicroServiceUtils.unzip(base64Decoder.decode(tmp)), 0, true, end).getItem2();
				} catch (IOException e) {
					result = null;
				}
				resultOffset = b64End + 1;
			}
		} else {
			if (Arrays.equals(chars, offset, MicroServiceUtils.NULL_ARRAY.length, MicroServiceUtils.NULL_ARRAY, 0,
					MicroServiceUtils.NULL_ARRAY.length)) {
				result = null;
				resultOffset = offset + MicroServiceUtils.NULL_ARRAY.length + 1;
			} else {
				Tuple2<Integer, Map<String, Object>> t;

				t = parseMap(chars, offset, true, end);

				resultOffset = t.getItem1() + 1;
				result = t.getItem2();
			}
		}

		return Tuple2.of(resultOffset, result);
	}

	private static Tuple2<Integer, Map<String, Object>> parseMap(byte[] jsonData, int offset, boolean breakAtRoot,
			int end) {
		Map<String, Object> resultMap = new HashMap<>();
		StringBuilder keyBuilder = new StringBuilder();
		StringBuilder valueBuilder = new StringBuilder();
		boolean inKey = false, inValue = false, inString = false;
		int braceCount = 0;
		char currentChar;
		int i;

		for (i = offset; i < end; i++) {
			currentChar = (char) jsonData[i];

			if (currentChar == '{') {
				braceCount++;
				if (braceCount == 1) {
					continue; // Skip the opening brace of the root object
				}
				Tuple2<Integer, Map<String, Object>> t = parseMap(jsonData, i, false, end);
				i = t.getItem1();
				resultMap.put(keyBuilder.toString(), t.getItem2());
			} else if (currentChar == '}') {
				braceCount--;
				if (braceCount == 0) {
					if (inValue) {
						resultMap.put(keyBuilder.toString(), parseValue(valueBuilder.toString()));
					}
					if (breakAtRoot) {
						break; // End of the root JSON object
					}
				}
			} else if (currentChar == '[') {
				Tuple2<Integer, List<Object>> t = parseArray(jsonData, i, end);
				i = t.getItem1();
				resultMap.put(keyBuilder.toString(), t.getItem2());
			} else if (currentChar == '"') {
				inString = !inString;
				if (inString) {
					if (!inKey && !inValue) {
						inKey = true;
						keyBuilder.setLength(0); // Reset key builder
					} else if (inKey) {
						inKey = false;
						inValue = true;
						valueBuilder.setLength(0); // Reset value builder
					} else if (inValue) {
						inValue = false;
						resultMap.put(keyBuilder.toString(), parseValue(valueBuilder.toString()));
					}
				}
			} else if (inString) {
				if (inKey) {
					keyBuilder.append(currentChar);
				} else if (inValue) {
					valueBuilder.append(currentChar);
				}
			} else if (currentChar == ':' && !inString) {
				inValue = true;
				valueBuilder.setLength(0); // Reset value builder
			} else if (currentChar == ',' && !inString) {
				if (inValue) {
					resultMap.put(keyBuilder.toString(), parseValue(valueBuilder.toString()));
					inValue = false;
				}
			} else if (!Character.isWhitespace(currentChar)) {
				if (inValue) {
					valueBuilder.append(currentChar);
				}
			}
		}

		return Tuple2.of(i, resultMap);
	}

	private static Tuple2<Integer, List<Object>> parseArray(byte[] jsonData, int offset, int end) {
		List<Object> resultList = new ArrayList<>();
		StringBuilder valueBuilder = new StringBuilder();
		boolean inString = false;
		int braceCount = 0;
		char currentChar;
		int i;

		for (i = offset; i < jsonData.length; i++) {
			currentChar = (char) jsonData[i];

			if (currentChar == '[') {
				braceCount++;
				if (braceCount == 1) {
					continue; // Skip the opening bracket of the array
				}
				Tuple2<Integer, List<Object>> t = parseArray(jsonData, i, end);
				i = t.getItem1();
				resultList.add(t.getItem2());
			} else if (currentChar == ']') {
				braceCount--;
				if (braceCount == 0) {
					if (valueBuilder.length() > 0) {
						resultList.add(parseValue(valueBuilder.toString()));
					}
					break; // End of the array
				}
			} else if (currentChar == '{') {
				Tuple2<Integer, Map<String, Object>> t = parseMap(jsonData, i, false, end);
				i = t.getItem1();
				resultList.add(t.getItem2());
			} else if (currentChar == '"') {
				inString = !inString;
				if (!inString) {
					resultList.add(valueBuilder.toString());
					valueBuilder.setLength(0); // Reset value builder
				}
			} else if (inString) {
				valueBuilder.append(currentChar);
			} else if (currentChar == ',' && !inString) {
				if (valueBuilder.length() > 0) {
					resultList.add(parseValue(valueBuilder.toString()));
					valueBuilder.setLength(0); // Reset value builder
				}
			} else if (!Character.isWhitespace(currentChar)) {
				valueBuilder.append(currentChar);
			}
		}

		return Tuple2.of(i, resultList);
	}

	private static Object parseValue(String value) {
		if (value.equals("null")) {
			return null;
		} else if (value.equals("true") || value.equals("false")) {
			return Boolean.parseBoolean(value);
		} else {
			try {
				if (value.contains(".")) {
					return Double.parseDouble(value);
				} else {
					return Integer.parseInt(value);
				}
			} catch (NumberFormatException e) {
				return value; // Return as string if not a number
			}
		}
	}

	private int find(byte[] chars, char c, int offset) {
		for (int i = offset; i < chars.length; i++) {
			if (chars[i] == c) {
				return i;
			}
		}
		return -1;
	}

}
