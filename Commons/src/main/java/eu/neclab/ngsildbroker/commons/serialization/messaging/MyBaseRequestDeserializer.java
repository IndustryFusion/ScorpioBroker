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
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;

public class MyBaseRequestDeserializer {

	private static final Decoder base64Decoder = Base64.getDecoder();
	private static final TypeReference<Map<String, Object>> mapTypeRef = new TypeReference<Map<String, Object>>() {
	};

	public BaseRequest deserialize(String body)
			throws IOException, JacksonException, IllegalArgumentException, IllegalAccessException {
		BaseRequest result = new BaseRequest();

		final int len = body.length();

		for (int i = 2; i < len; i++) {
			switch (body.charAt(i)) {
			case AppConstants.BYTE_ZIPPED_SERIALIZATION_CHAR: {
				if (body.charAt(i + 3) == 't') {
					result.setZipped(true);
					i += 8;
				} else {
					result.setZipped(false);
					i += 9;
				}
				break;
			}
			case AppConstants.BYTE_TENANT_SERIALIZATION_CHAR: {
				int offset = i + 4;
				int endIndex = body.indexOf('"', offset);
				result.setTenant(body.substring(offset, endIndex));
				i = endIndex + 2;
				break;
			}

			case AppConstants.BYTE_REQUESTTYPE_SERIALIZATION_CHAR: {
				int offset = i + 3;
				int endIndex = body.indexOf(',', offset);
				result.setRequestType(Integer.parseInt(body.substring(offset, endIndex)));
				i = endIndex + 1;
				break;
			}
			case AppConstants.BYTE_SENDTIMESTAMP_SERIALIZATION_CHAR: {
				int offset = i + 3;
				int endIndex = body.indexOf(',', offset);
				result.setSendTimestamp(Long.parseLong(body.substring(offset, endIndex)));
				i = endIndex + 1;
				break;
			}

			case AppConstants.BYTE_ATTRIBNAME_SERIALIZATION_CHAR: {
				int offset = i + 4;
				int endIndex = body.indexOf('"', offset);
				result.setAttribName(body.substring(offset, endIndex));
				i = endIndex + 2;
				break;
			}
			case AppConstants.BYTE_DATASETID_SERIALIZATION_CHAR: {
				int offset = i + 4;
				int endIndex = body.indexOf('"', offset);
				result.setDatasetId(body.substring(offset, endIndex));
				i = endIndex + 2;
				break;
			}
			case AppConstants.BYTE_DELETEALL_SERIALIZATION_CHAR: {
				if (body.charAt(i + 3) == 't') {
					result.setDeleteAll(true);
					i += 8;
				} else {
					result.setDeleteAll(false);
					i += 9;
				}

				break;
			}
			case AppConstants.BYTE_DISTRIBUTED_SERIALIZATION_CHAR: {
				if (body.charAt(i + 3) == 't') {
					result.setDistributed(true);
					i += 8;
				} else {
					result.setDistributed(false);
					i += 9;
				}
				break;
			}
			case AppConstants.BYTE_NOOVERWRITE_SERIALIZATION_CHAR: {
				if (body.charAt(i + 3) == 't') {
					result.setNoOverwrite(true);
					i += 8;
				} else {
					result.setNoOverwrite(false);
					i += 9;
				}
				break;
			}
			case AppConstants.BYTE_INSTANCEID_SERIALIZATION_CHAR: {
				int offset = i + 4;
				int endIndex = body.indexOf('"', offset);
				result.setInstanceId(body.substring(offset, endIndex));
				i = endIndex + 2;
				break;
			}
			case AppConstants.BYTE_PAYLOAD_SERIALIZATION_CHAR: {
				int end = body.length() - 2;
				int start = i + 3;
				Set<String> ids = Sets.newHashSet();
				Map<String, List<Map<String, Object>>> payloadMap = Maps.newHashMap();
				Map<String, List<Map<String, Object>>> prevPayloadMap = Maps.newHashMap();
				int j;
				for (j = start; j < end; j++) {
					int offset = j + 2;
					int endIndex = body.indexOf('"', offset);

					String id = body.substring(offset, endIndex);
					ids.add(id);

					j = endIndex + 2;

					Tuple2<Integer, Map<String, Object>> t = parsePayload(body, j, end, result.isZipped());
					Map<String, Object> payload = t.getItem2();
					j = t.getItem1();

					t = parsePayload(body, j, end, result.isZipped());
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
				i = j;
				break;
			}
			}
		}
		return result;
	}

	private Tuple2<Integer, Map<String, Object>> parsePayload(String body, int offset, int end, boolean zipped) {
		Map<String, Object> result;
		int resultOffset;
		if (zipped) {
			if (checkForZippedNullEntry(body, offset)) {
				result = null;
				resultOffset = offset + MicroServiceUtils.getZippedNullArrayLength() + 1;
			} else {
				int b64Start = offset + 1;
				int b64End = body.indexOf('"', b64Start);
				String tmp = body.substring(b64Start, b64End);

				try {
					result = (Map<String, Object>) parseValue(new String(MicroServiceUtils.unzip(base64Decoder.decode(tmp))), new int[] { 0 });
				} catch (IOException e) {
					result = null;
				}
				resultOffset = b64End + 1;
			}
		} else {
			if (checkForNullEntry(body, offset)) {
				result = null;
				resultOffset = offset + MicroServiceUtils.NULL_ARRAY.length + 1;
			} else {
				int[] tmpOffset = new int[] { offset };
				result = (Map<String, Object>) parseValue(body,tmpOffset );
				resultOffset = tmpOffset[0] + 1;
			}
		}

		return Tuple2.of(resultOffset, result);
	}

	private boolean checkForZippedNullEntry(String body, int offset) {
		for (int i = 0; i < MicroServiceUtils.getZippedNullArrayLength(); i++) {
			if (body.charAt(i + offset) == MicroServiceUtils.getZippedNullArray()[i]) {
				return false;
			}
		}
		return true;
	}

	private boolean checkForNullEntry(String body, int offset) {
		for (int i = 0; i < MicroServiceUtils.NULL_ARRAY.length; i++) {
			if (body.charAt(i + offset) != MicroServiceUtils.NULL_ARRAY[i]) {
				return false;
			}
		}
		return true;
	}

	private static Object parseValue(String json, int[] index) {
		char currentChar = json.charAt(index[0]);
		if (currentChar == '{') {
			return parseObject(json, index);
		} else if (currentChar == '[') {
			return parseArray(json, index);
		} else if (currentChar == '"') {
			return parseString(json, index);
		} else if (Character.isDigit(currentChar) || currentChar == '-') {
			return parseNumber(json, index);
		} else if (json.startsWith("true", index[0])) {
			index[0] += 4;
			return true;
		} else if (json.startsWith("false", index[0])) {
			index[0] += 5;
			return false;
		} else if (json.startsWith("null", index[0])) {
			index[0] += 4;
			return null;
		}
		throw new IllegalArgumentException("Unexpected character: " + currentChar);
	}

	private static Map<String, Object> parseObject(String json, int[] index) {
		Map<String, Object> map = new HashMap<>();
		index[0]++; // Skip '{'
		while (json.charAt(index[0]) != '}') {
			skipWhitespace(json, index);
			String key = parseString(json, index);
			skipWhitespace(json, index);
			index[0]++; // Skip ':'
			skipWhitespace(json, index);
			Object value = parseValue(json, index);
			map.put(key, value);
			skipWhitespace(json, index);
			if (json.charAt(index[0]) == ',') {
				index[0]++; // Skip ','
			}
			skipWhitespace(json, index);
		}
		index[0]++; // Skip '}'
		return map;
	}

	private static List<Object> parseArray(String json, int[] index) {
		List<Object> list = new ArrayList<>();
		index[0]++; // Skip '['
		while (json.charAt(index[0]) != ']') {
			skipWhitespace(json, index);
			list.add(parseValue(json, index));
			skipWhitespace(json, index);
			if (json.charAt(index[0]) == ',') {
				index[0]++; // Skip ','
			}
			skipWhitespace(json, index);
		}
		index[0]++; // Skip ']'
		return list;
	}

	private static String parseString(String json, int[] index) {
		StringBuilder sb = new StringBuilder();
		index[0]++; // Skip '"'
		while (json.charAt(index[0]) != '"') {
			char currentChar = json.charAt(index[0]);
			if (currentChar == '\\') {
				index[0]++;
				currentChar = json.charAt(index[0]);
				if (currentChar == 'u') {
					sb.append((char) Integer.parseInt(json.substring(index[0] + 1, index[0] + 5), 16));
					index[0] += 4;
				} else {
					sb.append(currentChar);
				}
			} else {
				sb.append(currentChar);
			}
			index[0]++;
		}
		index[0]++; // Skip closing '"'
		return sb.toString();
	}

	private static Number parseNumber(String json, int[] index) {
		int start = index[0];
		while (index[0] < json.length() && (Character.isDigit(json.charAt(index[0])) || json.charAt(index[0]) == '.'
				|| json.charAt(index[0]) == '-' || json.charAt(index[0]) == '+')) {
			index[0]++;
		}
		String numberStr = json.substring(start, index[0]);
		if (numberStr.contains(".") || numberStr.contains("e") || numberStr.contains("E")) {
			return Double.parseDouble(numberStr);
		} else {
			return Long.parseLong(numberStr);
		}
	}

	private static void skipWhitespace(String json, int[] index) {
		while (index[0] < json.length() && Character.isWhitespace(json.charAt(index[0]))) {
			index[0]++;
		}
	}

}
