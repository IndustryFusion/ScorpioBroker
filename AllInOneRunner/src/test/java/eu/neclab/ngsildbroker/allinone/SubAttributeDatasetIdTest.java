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

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;

/**
 * A sub-attribute is an attribute of its own and may have several instances, so its instances are validated
 * against their own datasetIds, not counted for the Attribute instance that holds them (CIM 009 4.5.2, 4.5.3,
 * 4.5.5; contracts/context-broker: Value forms the broker stores, accepted deviation A5).
 */
@QuarkusTest
@TestProfile(PlatformProfile.class)
public class SubAttributeDatasetIdTest {

	private static RequestSpecification admin() {
		return given().contentType("application/json").accept("application/json").header("Authorization",
				"Bearer " + PlatformProfile.clientRoles("Factory-Admin"));
	}

	private static String newId() {
		return "urn:test:subattribute:" + UUID.randomUUID();
	}

	private static String entity(String id, String a) {
		return "{\"id\":\"" + id + "\",\"type\":\"SubAttributeTest\",\"a\":" + a + "}";
	}

	/** an instance of a with a datasetId whose sub-attribute p1 is an array of one default instance */
	private static final String NAMED_WITH_SUB_ARRAY = "[{\"type\":\"Property\",\"value\":\"A\","
			+ "\"datasetId\":\"urn:d\",\"p1\":[{\"type\":\"Property\",\"value\":1}]}]";

	@Test
	public void createWithSubAttributeArray() {
		String id = newId();
		admin().body(entity(id, NAMED_WITH_SUB_ARRAY)).post("/ngsi-ld/v1/entities").then().statusCode(201);
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.datasetId", equalTo("urn:d"))
				.body("a.p1.value", equalTo(1));
	}

	@Test
	public void upsertAndMergeWithSubAttributeArray() {
		String id = newId();
		admin().body("[" + entity(id, NAMED_WITH_SUB_ARRAY) + "]").post("/ngsi-ld/v1/entityOperations/upsert").then()
				.statusCode(201);
		admin().body("{\"a\":[{\"type\":\"Property\",\"value\":\"B\",\"datasetId\":\"urn:d\","
				+ "\"p1\":[{\"type\":\"Property\",\"value\":2}]}]}").patch("/ngsi-ld/v1/entities/" + id).then()
				.statusCode(204);
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.value", equalTo("B"))
				.body("a.p1.value", equalTo(2));
		admin().body("[" + entity(id, "[{\"type\":\"Property\",\"value\":\"C\",\"datasetId\":\"urn:d\","
				+ "\"p1\":[{\"type\":\"Property\",\"value\":3}]}]") + "]")
				.post("/ngsi-ld/v1/entityOperations/merge").then().statusCode(204);
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.value", equalTo("C"))
				.body("a.p1.value", equalTo(3));
	}

	@Test
	public void subAttributeInstancesWithDistinctDatasetIds() {
		String id = newId();
		admin().body(entity(id, "[{\"type\":\"Property\",\"value\":\"A\",\"datasetId\":\"urn:d\",\"p1\":["
				+ "{\"type\":\"Property\",\"value\":1},{\"type\":\"Property\",\"value\":2,\"datasetId\":\"urn:x\"},"
				+ "{\"type\":\"Property\",\"value\":3,\"datasetId\":\"urn:y\"}]}]")).post("/ngsi-ld/v1/entities")
				.then().statusCode(201);
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.p1", hasSize(3))
				.body("a.p1.find { it.datasetId == null }.value", equalTo(1))
				.body("a.p1.find { it.datasetId == 'urn:x' }.value", equalTo(2))
				.body("a.p1.find { it.datasetId == 'urn:y' }.value", equalTo(3));
	}

	@Test
	public void twoDefaultInstancesOfASubAttribute() {
		String id = newId();
		admin().body(entity(id, "[{\"type\":\"Property\",\"value\":\"A\",\"datasetId\":\"urn:d\",\"p1\":["
				+ "{\"type\":\"Property\",\"value\":1},{\"type\":\"Property\",\"value\":2}]}]"))
				.post("/ngsi-ld/v1/entities").then().statusCode(400)
				.body("detail", containsString("Duplicated datasetId"));
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(404);
	}

	@Test
	public void twoDefaultInstancesOfAnAttribute() {
		String id = newId();
		admin().body(entity(id, "[{\"type\":\"Property\",\"value\":1},{\"type\":\"Property\",\"value\":2}]"))
				.post("/ngsi-ld/v1/entities").then().statusCode(400)
				.body("detail", containsString("Duplicated datasetId"));
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(404);
	}
}
