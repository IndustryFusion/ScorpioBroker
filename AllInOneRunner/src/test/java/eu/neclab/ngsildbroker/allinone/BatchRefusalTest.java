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
import static org.hamcrest.Matchers.hasSize;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;

/**
 * A batch whose only entity is refused answers 207 with a BatchOperationResult (CIM 009 6.31.3.1, 6.16.3.1, 5.2.16,
 * 5.2.17; a Property value of JSON null is refused, 4.5.0; contracts/context-broker: Refusals the platform relies
 * on / Null value in a batch of one, accepted deviation A3).
 */
@QuarkusTest
@TestProfile(PlatformProfile.class)
public class BatchRefusalTest {

	private static RequestSpecification admin() {
		return given().contentType("application/json").accept("application/json").header("Authorization",
				"Bearer " + PlatformProfile.clientRoles("Factory-Admin"));
	}

	private static String create() {
		String id = "urn:test:refusal:" + UUID.randomUUID();
		admin().body("{\"id\":\"" + id + "\",\"type\":\"RefusalTest\",\"a\":{\"type\":\"Property\",\"value\":1}}")
				.post("/ngsi-ld/v1/entities").then().statusCode(201);
		return id;
	}

	private static String nullValue(String id) {
		return "[{\"id\":\"" + id + "\",\"a\":{\"type\":\"Property\",\"value\":null}}]";
	}

	private static void refusedAlone(String operation) {
		String id = create();
		admin().body(nullValue(id)).post("/ngsi-ld/v1/entityOperations/" + operation).then().statusCode(207)
				.body("success", hasSize(0)).body("errors", hasSize(1)).body("errors[0].entityId", equalTo(id))
				.body("errors[0].error.type", endsWith("/errors/BadRequestData"));
		admin().get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.value", equalTo(1));
	}

	@Test
	public void batchMergeOfOne() {
		refusedAlone("merge");
	}

	@Test
	public void batchUpdateOfOne() {
		refusedAlone("update");
	}

	/** As before: in a batch of two, the other entity is merged */
	@Test
	public void batchMergeOfTwo() {
		String refused = create();
		String merged = create();
		admin().body("[{\"id\":\"" + refused + "\",\"a\":{\"type\":\"Property\",\"value\":null}},{\"id\":\"" + merged
				+ "\",\"a\":{\"type\":\"Property\",\"value\":2}}]").post("/ngsi-ld/v1/entityOperations/merge").then()
				.statusCode(207).body("success", hasSize(1)).body("success[0]", equalTo(merged))
				.body("errors[0].entityId", equalTo(refused));
		admin().get("/ngsi-ld/v1/entities/" + merged).then().statusCode(200).body("a.value", equalTo(2));
	}

	/** A batch merge of an entity that doesn't exist still answers with its refusal */
	@Test
	public void missingEntity() {
		String id = "urn:test:refusal:missing:" + UUID.randomUUID();
		admin().body("[{\"id\":\"" + id + "\",\"a\":{\"type\":\"Property\",\"value\":2}}]")
				.post("/ngsi-ld/v1/entityOperations/merge").then().statusCode(207).body("success", hasSize(0))
				.body("errors[0].entityId", equalTo(id)).body("errors[0].error.type", endsWith("/ResourceNotFound"));
	}
}
