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
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;

/**
 * "datasetId": "@none" names the default Attribute instance (CIM 009 4.5.5; contracts/context-broker: Default
 * attribute instance, accepted deviation A2).
 */
@QuarkusTest
@TestProfile(PlatformProfile.class)
public class DefaultDatasetIdTest {

	private static RequestSpecification admin() {
		return given().contentType("application/json").accept("application/json").header("Authorization",
				"Bearer " + PlatformProfile.clientRoles("Factory-Admin"));
	}

	private static String create() {
		String id = "urn:test:none:" + UUID.randomUUID();
		admin().body("{\"id\":\"" + id + "\",\"type\":\"NoneTest\",\"a\":{\"type\":\"Property\",\"value\":1}}")
				.post("/ngsi-ld/v1/entities").then().statusCode(201);
		return id;
	}

	private static String instance(Object value) {
		return "{\"type\":\"Property\",\"value\":" + value + ",\"datasetId\":\"@none\"}";
	}

	@Test
	public void batchMerge() {
		String id = create();
		admin().body("[{\"id\":\"" + id + "\",\"a\":[" + instance(5) + "]}]").post("/ngsi-ld/v1/entityOperations/merge")
				.then().statusCode(204);
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.value", equalTo(5))
				.body("a.datasetId", nullValue());
	}

	@Test
	public void mergeEntity() {
		String id = create();
		admin().body("{\"a\":" + instance(6) + "}").patch("/ngsi-ld/v1/entities/" + id).then().statusCode(204);
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.value", equalTo(6))
				.body("a.datasetId", nullValue());
	}

	@Test
	public void appendAndCreate() {
		String id = create();
		admin().body("{\"a\":" + instance(7) + "}").post("/ngsi-ld/v1/entities/" + id + "/attrs").then()
				.statusCode(204);
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.value", equalTo(7));
		String id2 = "urn:test:none:" + UUID.randomUUID();
		admin().body("{\"id\":\"" + id2 + "\",\"type\":\"NoneTest\",\"a\":" + instance(8) + "}")
				.post("/ngsi-ld/v1/entities").then().statusCode(201);
		admin().get("/ngsi-ld/v1/entities/" + id2).then().statusCode(200).body("a.value", equalTo(8))
				.body("a.datasetId", nullValue());
	}

	/** "@none" and a missing datasetId are the same instance, so both in one request are two default instances */
	@Test
	public void twoDefaultInstances() {
		String id = create();
		admin().body("[{\"id\":\"" + id + "\",\"a\":[{\"type\":\"Property\",\"value\":2}," + instance(3) + "]}]")
				.post("/ngsi-ld/v1/entityOperations/merge").then().statusCode(207)
				.body("errors[0].entityId", equalTo(id)).body("errors[0].error.type", endsWith("/BadRequestData"));
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.value", equalTo(1));
	}

	/** Other datasetIds stay as they are */
	@Test
	public void namedDatasetIdKept() {
		String id = create();
		admin().body("[{\"id\":\"" + id
				+ "\",\"a\":[{\"type\":\"Property\",\"value\":9,\"datasetId\":\"urn:test:dataset:1\"}]}]")
				.post("/ngsi-ld/v1/entityOperations/merge").then().statusCode(204);
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200)
				.body("a.find { it.datasetId == 'urn:test:dataset:1' }.value", equalTo(9))
				.body("a.find { it.datasetId == null }.value", equalTo(1));
	}
}
