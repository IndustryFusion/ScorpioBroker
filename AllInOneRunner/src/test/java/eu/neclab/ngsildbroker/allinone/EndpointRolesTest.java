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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;

/**
 * The roles of every endpoint of the all-in-one runner (contracts/context-broker: Requests need a token with a
 * platform role, Factory-Admin may do what the platform does, Factory-Reader only reads, Subscriptions and
 * notifications). A new endpoint fails this test until it is given its roles here and in its controller.
 */
public class EndpointRolesTest {

	private static final Set<String> ADMIN = Set.of("Factory-Admin");
	private static final Set<String> CREATE = Set.of("Factory-Admin", "Factory-Writer");
	private static final Set<String> MODIFY = Set.of("Factory-Admin", "Factory-Editor", "Factory-Writer");
	private static final Set<String> READ = Set.of("Factory-Admin", "Factory-Reader", "Factory-Editor");
	private static final Set<String> REG_READ = Set.of("Factory-Admin", "Factory-Reader");
	private static final Set<String> SUB_WRITE = Set.of("Factory-Admin", "Subscriber");
	private static final Set<String> SUB_READ = Set.of("Factory-Admin", "Factory-Reader");
	private static final Set<String> SUB_READ_ONE = Set.of("Factory-Admin", "Factory-Reader", "Subscriber");
	/** Endpoints without a role check: internal calls of the broker and federation, as in the fork on 5.0.5 */
	private static final Set<String> OPEN = Set.of();

	private static final Map<String, Map<String, Set<String>>> EXPECTED = new TreeMap<>();
	static {
		put("eu.neclab.ngsildbroker.entityhandler.controller.EntityBatchController", Map.of("createMultiple", CREATE,
				"upsertMultiple", MODIFY, "appendMultiple", MODIFY, "deleteMultiple", ADMIN, "mergeMultiple", MODIFY));
		put("eu.neclab.ngsildbroker.entityhandler.controller.EntityController",
				Map.of("createEntity", CREATE, "updateEntity", MODIFY, "appendEntity", CREATE,
						"partialUpdateAttribute", MODIFY, "deleteAttribute", ADMIN, "deleteEntity", ADMIN,
						"mergePatchPure", MODIFY, "mergePatch", MODIFY, "replaceEntity", MODIFY, "replaceAttribute",
						MODIFY));
		put("eu.neclab.ngsildbroker.entityhandler.controller.BulkController", Map.of("bulkInsert", CREATE));
		put("eu.neclab.ngsildbroker.queryhandler.controller.EntityOperationsQueryController",
				Map.of("postQuery", READ));
		put("eu.neclab.ngsildbroker.queryhandler.controller.QueryController",
				Map.of("getEntity", READ, "query", READ, "getAllTypes", READ, "getType", READ, "getAllAttributes",
						READ, "getAttribute", READ, "queryEntityMap", OPEN, "getEntityMap", OPEN, "deleteEntityMap",
						OPEN, "updateEntityMap", OPEN));
		put("eu.neclab.ngsildbroker.registryhandler.controller.RegistryController", Map.of("queryCSource", REG_READ,
				"registerCSource", ADMIN, "getCSourceById", REG_READ, "updateCSource", ADMIN, "deleteCSource", ADMIN));
		put("eu.neclab.ngsildbroker.registry.subscriptionmanager.controller.RegistrySubscriptionController",
				Map.of("subscribe", SUB_WRITE, "getAllSubscriptions", SUB_READ, "getSubscriptionById", SUB_READ_ONE,
						"deleteSubscription", SUB_WRITE, "updateSubscription", SUB_WRITE));
		put("eu.neclab.ngsildbroker.subscriptionmanager.controller.SubscriptionController",
				Map.of("subscribe", SUB_WRITE, "getAllSubscriptions", SUB_READ, "getSubscriptionById", SUB_READ_ONE,
						"deleteSubscription", SUB_WRITE, "updateSubscription", SUB_WRITE));
		put("eu.neclab.ngsildbroker.subscriptionmanager.controller.NotificationController", Map.of("notify", OPEN));
		put("eu.neclab.ngsildbroker.historyentitymanager.controller.HistoryController",
				Map.of("createTemporalEntity", CREATE, "deleteTemporalEntityById", ADMIN, "addAttrib2TemopralEntity",
						CREATE, "deleteAttrib2TemporalEntity", ADMIN, "modifyAttribInstanceTemporalEntity", MODIFY,
						"deleteAtrribInstanceTemporalEntity", ADMIN));
		put("eu.neclab.ngsildbroker.historyquerymanager.controller.HistoryController",
				Map.of("queryTemporalEntities", READ, "retrieveTemporalEntity", READ));
		put("eu.neclab.ngsildbroker.historyquerymanager.controller.HistoryOperationsController",
				Map.of("postQuery", READ));
		put("eu.neclab.ngsildbroker.infomanager.controller.InfoController", Map.of("getSourceIdentity", READ));
		put("eu.neclab.ngsildbroker.atcontextserver.controller.ContextController",
				Map.of("getContextById", OPEN, "getContexts", OPEN, "createContext", OPEN, "deleteContextById", OPEN,
						"loadCache", OPEN, "createImplicitly", OPEN));
	}

	private static void put(String cls, Map<String, Set<String>> methods) {
		EXPECTED.put(cls, methods);
	}

	private static boolean isEndpoint(Method m) {
		return m.isAnnotationPresent(GET.class) || m.isAnnotationPresent(POST.class)
				|| m.isAnnotationPresent(PUT.class) || m.isAnnotationPresent(PATCH.class)
				|| m.isAnnotationPresent(DELETE.class);
	}

	@Test
	public void everyEndpointHasItsRoles() throws Exception {
		List<String> mismatches = new ArrayList<>();
		for (Map.Entry<String, Map<String, Set<String>>> cls : EXPECTED.entrySet()) {
			Class<?> c = Class.forName(cls.getKey());
			Set<String> seen = new TreeSet<>();
			for (Method m : c.getDeclaredMethods()) {
				if (!isEndpoint(m)) {
					continue;
				}
				seen.add(m.getName());
				Set<String> expected = cls.getValue().get(m.getName());
				RolesAllowed ra = m.getAnnotation(RolesAllowed.class);
				Set<String> actual = ra == null ? Set.of() : Set.of(ra.value());
				if (expected == null) {
					mismatches.add(c.getSimpleName() + "." + m.getName() + " is a new endpoint with roles " + actual);
				} else if (!expected.equals(actual)) {
					mismatches.add(c.getSimpleName() + "." + m.getName() + " has " + actual + ", expected " + expected);
				}
			}
			for (String name : cls.getValue().keySet()) {
				if (!seen.contains(name)) {
					mismatches.add(c.getSimpleName() + "." + name + " is no endpoint any more");
				}
			}
		}
		assertEquals(List.of(), mismatches);
	}

	/** Each role is a string of its own; "Factory-Admin, Subscriber" would be one role nobody has */
	@Test
	public void rolesAreSingleNames() throws Exception {
		for (String cls : EXPECTED.keySet()) {
			for (Method m : Class.forName(cls).getDeclaredMethods()) {
				RolesAllowed ra = m.getAnnotation(RolesAllowed.class);
				if (ra != null) {
					for (String role : ra.value()) {
						assertTrue(role.matches("[A-Za-z-]+"), m + " has the role \"" + role + "\"");
					}
				}
			}
		}
	}

	/** T7: a subscription is readable by its id with Factory-Admin (CIM 009 6.11.3.1) */
	@Test
	public void subscriptionReadableByIdAsAdmin() throws Exception {
		Method m = Arrays.stream(
				Class.forName("eu.neclab.ngsildbroker.subscriptionmanager.controller.SubscriptionController")
						.getDeclaredMethods())
				.filter(x -> x.getName().equals("getSubscriptionById")).findFirst().orElseThrow();
		RolesAllowed ra = m.getAnnotation(RolesAllowed.class);
		assertNotNull(ra);
		assertTrue(Set.of(ra.value()).contains("Factory-Admin"));
		assertTrue(Set.of(ra.value()).contains("Factory-Reader"));
	}

	/** Factory-Reader is never allowed a write */
	@Test
	public void readerOnlyReads() throws Exception {
		for (String cls : EXPECTED.keySet()) {
			for (Method m : Class.forName(cls).getDeclaredMethods()) {
				RolesAllowed ra = m.getAnnotation(RolesAllowed.class);
				if (ra != null && Set.of(ra.value()).contains("Factory-Reader")) {
					boolean read = m.isAnnotationPresent(GET.class)
							|| (m.isAnnotationPresent(POST.class) && m.getName().equals("postQuery"));
					assertTrue(read, m + " lets Factory-Reader write");
				}
			}
		}
	}
}
