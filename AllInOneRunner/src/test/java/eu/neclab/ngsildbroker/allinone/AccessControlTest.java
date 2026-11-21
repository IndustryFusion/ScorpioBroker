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
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;

/**
 * Access control of the all-in-one runner with tokens signed by a configured key (contracts/context-broker:
 * Requests need a token with a platform role, Factory-Admin may do what the platform does, Factory-Reader only
 * reads, Subscriptions and notifications, The broker runs with the platform's configuration / Without an identity
 * provider).
 */
@QuarkusTest
@TestProfile(PlatformProfile.class)
public class AccessControlTest {

	private static final String JSON = "application/json";

	private static RequestSpecification as(String token) {
		RequestSpecification r = given().contentType(JSON).accept(JSON);
		return token == null ? r : r.header("Authorization", "Bearer " + token);
	}

	private static String entity(String id) {
		return "{\"id\":\"" + id + "\",\"type\":\"AccessTest\",\"a\":{\"type\":\"Property\",\"value\":1}}";
	}

	@Test
	public void noToken() {
		as(null).get("/ngsi-ld/v1/types").then().statusCode(401);
		as(null).body(entity("urn:test:ac:none")).post("/ngsi-ld/v1/entities").then().statusCode(401);
	}

	@Test
	public void tokenNotSignedByTheKey() {
		String t = PlatformProfile.clientRoles("Factory-Admin");
		String forged = t.substring(0, t.lastIndexOf('.') + 1) + "AAAA";
		as(forged).get("/ngsi-ld/v1/types").then().statusCode(401);
	}

	@Test
	public void noPlatformRole() {
		as(PlatformProfile.token(Map.of())).get("/ngsi-ld/v1/types").then().statusCode(403);
		as(PlatformProfile.clientRoles("offline_access")).get("/ngsi-ld/v1/types").then().statusCode(403);
	}

	@Test
	public void readerOnlyReads() {
		String reader = PlatformProfile.clientRoles("Factory-Reader");
		as(reader).get("/ngsi-ld/v1/types").then().statusCode(200);
		String id = "urn:test:ac:reader:" + UUID.randomUUID();
		as(reader).body(entity(id)).post("/ngsi-ld/v1/entities").then().statusCode(403);
		as(reader).body("[" + entity(id) + "]").post("/ngsi-ld/v1/entityOperations/merge").then().statusCode(403);
		as(reader).delete("/ngsi-ld/v1/entities/" + id).then().statusCode(403);
	}

	private static void adminLifecycle(String admin) {
		String id = "urn:test:ac:admin:" + UUID.randomUUID();
		as(admin).body(entity(id)).post("/ngsi-ld/v1/entities").then().statusCode(201);
		as(admin).body("[{\"id\":\"" + id + "\",\"a\":{\"type\":\"Property\",\"value\":2}}]")
				.post("/ngsi-ld/v1/entityOperations/merge").then().statusCode(204);
		as(admin).get("/ngsi-ld/v1/entities/" + id).then().statusCode(200).body("a.value", equalTo(2));
		as(admin).delete("/ngsi-ld/v1/entities/" + id).then().statusCode(204);
	}

	@Test
	public void adminAsClientRole() {
		adminLifecycle(PlatformProfile.clientRoles("Factory-Admin"));
	}

	@Test
	public void adminAsRealmRole() {
		adminLifecycle(PlatformProfile.realmRoles("Factory-Admin"));
	}

	/** T7: a subscription is readable by its id (CIM 009 6.11.3.1) */
	@Test
	public void subscriptionReadByItsId() {
		String admin = PlatformProfile.clientRoles("Factory-Admin");
		String id = "urn:test:ac:sub:" + UUID.randomUUID();
		String sub = "{\"id\":\"" + id + "\",\"type\":\"Subscription\",\"entities\":[{\"type\":\"AccessTest\"}],"
				+ "\"notification\":{\"endpoint\":{\"uri\":\"http://localhost:1/notify\",\"accept\":\"application/json\"}}}";
		as(admin).body(sub).post("/ngsi-ld/v1/subscriptions").then().statusCode(201);
		as(admin).get("/ngsi-ld/v1/subscriptions/" + id).then().statusCode(200).body("id", equalTo(id));
		as(PlatformProfile.clientRoles("Factory-Reader")).get("/ngsi-ld/v1/subscriptions/" + id).then()
				.statusCode(200);
		as(admin).delete("/ngsi-ld/v1/subscriptions/" + id).then().statusCode(204);
	}
}
