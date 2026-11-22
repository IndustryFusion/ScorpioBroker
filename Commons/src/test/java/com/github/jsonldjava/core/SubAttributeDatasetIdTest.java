/*
 * Copyright (c) 2026 IndustryFusion Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jsonldjava.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

/**
 * A sub-attribute is an attribute of its own (CIM 009 4.5.2, 4.5.3), so its instances are validated against
 * their own datasetIds (4.5.5), not counted for the Attribute instance that holds them.
 */
public class SubAttributeDatasetIdTest {

	private static final Map<String, Object> CONTEXT = Map.of("id", "@id", "type", "@type", "Property",
			"https://uri.etsi.org/ngsi-ld/Property", "value", "https://uri.etsi.org/ngsi-ld/hasValue", "datasetId",
			Map.of("@id", "https://uri.etsi.org/ngsi-ld/datasetId", "@type", "@id"));

	private static final ObjectMapper MAPPER = new ObjectMapper();

	@SuppressWarnings("unchecked")
	private static void expand(String attributes) throws Exception {
		Map<String, Object> entity = MAPPER.readValue(
				"{\"id\":\"urn:test:sub:1\",\"type\":\"http://ex.org/T\"," + attributes + "}", Map.class);
		JsonLdOptions opts = new JsonLdOptions();
		Context ctx = new Context(opts).parse(CONTEXT, false, null, null).await().indefinitely();
		new JsonLdApi(opts).expandEntitySync(ctx, entity, AppConstants.ENTITY_CREATE_PAYLOAD, false, null, null);
	}

	private static void refused(String attributes) {
		ResponseException e = assertThrows(ResponseException.class, () -> expand(attributes));
		assertTrue(String.valueOf(e.getDetail()).contains("Duplicated datasetId"), String.valueOf(e.getDetail()));
	}

	/** an instance with a datasetId whose sub-attribute is written as an array of one default instance */
	@Test
	public void subAttributeArrayUnderNamedInstance() {
		assertDoesNotThrow(() -> expand("\"http://ex.org/a\":[{\"type\":\"Property\",\"value\":\"A\","
				+ "\"datasetId\":\"urn:d\",\"http://ex.org/p1\":[{\"type\":\"Property\",\"value\":1}]}]"));
	}

	/** a sub-attribute instance with a datasetId of its own does not count for the instance that holds it */
	@Test
	public void namedSubAttributeUnderNamedInstance() {
		assertDoesNotThrow(() -> expand("\"http://ex.org/a\":[{\"type\":\"Property\",\"value\":\"A\","
				+ "\"datasetId\":\"urn:d\",\"http://ex.org/p1\":{\"type\":\"Property\",\"value\":1,"
				+ "\"datasetId\":\"urn:x\"}}]"));
	}

	@Test
	public void subAttributeInstancesWithDistinctDatasetIds() {
		assertDoesNotThrow(() -> expand("\"http://ex.org/a\":[{\"type\":\"Property\",\"value\":\"A\","
				+ "\"datasetId\":\"urn:d\",\"http://ex.org/p1\":[{\"type\":\"Property\",\"value\":1},"
				+ "{\"type\":\"Property\",\"value\":2,\"datasetId\":\"urn:x\"},"
				+ "{\"type\":\"Property\",\"value\":3,\"datasetId\":\"urn:y\"}]}]"));
	}

	/** two named instances of an attribute, each with a default sub-attribute instance */
	@Test
	public void namedInstancesWithSubAttributes() {
		assertDoesNotThrow(() -> expand("\"http://ex.org/a\":[{\"type\":\"Property\",\"value\":\"A\","
				+ "\"datasetId\":\"urn:d1\",\"http://ex.org/p1\":{\"type\":\"Property\",\"value\":1}},"
				+ "{\"type\":\"Property\",\"value\":\"B\",\"datasetId\":\"urn:d2\","
				+ "\"http://ex.org/p1\":[{\"type\":\"Property\",\"value\":2}]}]"));
	}

	@Test
	public void twoDefaultInstancesOfASubAttribute() {
		refused("\"http://ex.org/a\":[{\"type\":\"Property\",\"value\":\"A\",\"datasetId\":\"urn:d\","
				+ "\"http://ex.org/p1\":[{\"type\":\"Property\",\"value\":1},{\"type\":\"Property\",\"value\":2}]}]");
	}

	@Test
	public void twoInstancesOfASubAttributeWithOneDatasetId() {
		refused("\"http://ex.org/a\":{\"type\":\"Property\",\"value\":\"A\",\"http://ex.org/p1\":["
				+ "{\"type\":\"Property\",\"value\":1,\"datasetId\":\"urn:x\"},"
				+ "{\"type\":\"Property\",\"value\":2,\"datasetId\":\"urn:x\"}]}");
	}

	@Test
	public void twoDefaultInstancesOfAnAttribute() {
		refused("\"http://ex.org/a\":[{\"type\":\"Property\",\"value\":1},{\"type\":\"Property\",\"value\":2}]");
	}

	@Test
	public void twoInstancesOfAnAttributeWithOneDatasetId() {
		refused("\"http://ex.org/a\":[{\"type\":\"Property\",\"value\":1,\"datasetId\":\"urn:d\"},"
				+ "{\"type\":\"Property\",\"value\":2,\"datasetId\":\"urn:d\"}]");
	}
}
