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
package eu.neclab.ngsildbroker.allinone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;

/**
 * The merge of value objects as a whole (V20261120.1), on the store the all-in-one runner migrated at its start.
 */
@QuarkusTest
@TestProfile(PlatformProfile.class)
public class MergeValueObjectsTest {

	private static final String DOUBLE = "http://www.w3.org/2001/XMLSchema#double";

	@Inject
	AgroalDataSource ds;

	private String query(String sql, String... args) throws SQLException {
		try (Connection c = ds.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
			for (int i = 0; i < args.length; i++) {
				s.setString(i + 1, args[i]);
			}
			try (ResultSet r = s.executeQuery()) {
				return r.next() ? r.getString(1) : null;
			}
		}
	}

	private Object mergeHasValue(String newValue, String oldValue) throws SQLException {
		String r = query("SELECT merge_has_value(?::jsonb, ?::jsonb)", newValue, oldValue);
		return r == null ? null : new JsonArray(r);
	}

	/** Merge of typed and language-tagged values (A1, IndustryFusion/ScorpioBroker#8, CIM 009 3.1, 5.5.12) */
	@Test
	public void typedValueReplacesTheOldValueObject() throws SQLException {
		assertEquals("true", query("SELECT success::text FROM flyway_schema_history WHERE version = '20261120.1'"));
		String typed = "[{\"@value\": 80, \"@type\": \"" + DOUBLE + "\"}]";
		assertEquals(new JsonArray(typed), mergeHasValue(typed, "[{\"@value\": 1, \"@type\": \"" + DOUBLE + "\"}]"));
		assertEquals(new JsonArray(typed), mergeHasValue(typed, "[{\"@value\": 1}]"));
		assertEquals(new JsonArray(typed), mergeHasValue(typed, null));
	}

	@Test
	public void languageTaggedStringReplacesTheOldOne() throws SQLException {
		String de = "[{\"@value\": \"Hallo\", \"@language\": \"de\"}]";
		assertEquals(new JsonArray(de), mergeHasValue(de, "[{\"@value\": \"Hi\", \"@language\": \"en\"}]"));
		assertEquals(new JsonArray(de), mergeHasValue(de, "[{\"@value\": \"plain\"}]"));
	}

	@Test
	public void nodeReferenceReplacesTheOldValue() throws SQLException {
		String ref = "[{\"@id\": \"urn:test:x\"}]";
		assertEquals(new JsonArray(ref), mergeHasValue(ref, "[{\"@value\": 1}]"));
		assertEquals(new JsonArray(ref), mergeHasValue(ref, "[{\"@id\": \"urn:test:y\"}]"));
	}

	/** A plain value merged onto a declared one keeps the stored @type (Declared types in the broker, D10) */
	@Test
	public void plainValueKeepsTheDeclaredType() throws SQLException {
		assertEquals(new JsonArray("[{\"@value\": 42.5, \"@type\": \"" + DOUBLE + "\"}]"),
				mergeHasValue("[{\"@value\": 42.5}]", "[{\"@value\": 0, \"@type\": \"" + DOUBLE + "\"}]"));
	}

	/** NGSI-LD Null deletes the value (CIM 009 5.5.12) */
	@Test
	public void ngsiLdNullDeletes() throws SQLException {
		assertNull(mergeHasValue("[{\"@value\": \"urn:ngsi-ld:null\"}]", "[{\"@value\": 1}]"));
	}

	@Test
	public void typedValueInAList() throws SQLException {
		String list = "{\"@list\": [{\"@value\": 2, \"@type\": \"" + DOUBLE + "\"}, {\"@value\": \"b\", \"@language\": \"en\"}]}";
		String r = query("SELECT merge_has_value_list(?::jsonb, ?::jsonb)", list,
				"{\"@list\": [{\"@value\": 1}, {\"@value\": \"a\"}]}");
		assertEquals(new JsonObject(list), new JsonObject(r));
		assertTrue(r.contains(DOUBLE));
	}
}
