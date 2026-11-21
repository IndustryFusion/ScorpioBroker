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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

/**
 * "datasetId": "@none" names the default Attribute instance where entities are written (CIM 009 4.5.5), and stays
 * a filter in subscriptions and queries.
 */
public class DefaultDatasetIdTest {

	@Test
	public void noneIsTheDefaultDatasetId() {
		assertTrue(NGSIObject.isDefaultDatasetId(Map.of("@id", "@none")));
		assertTrue(NGSIObject.isDefaultDatasetId(List.of(Map.of("@id", "@none"))));
	}

	@Test
	public void otherDatasetIdsAreNot() {
		assertFalse(NGSIObject.isDefaultDatasetId(Map.of("@id", "urn:test:dataset:1")));
		assertFalse(NGSIObject.isDefaultDatasetId(List.of(Map.of("@id", "@none"), Map.of("@id", "urn:x"))));
		assertFalse(NGSIObject.isDefaultDatasetId(Map.of("@value", "@none")));
		assertFalse(NGSIObject.isDefaultDatasetId(Map.of("@id", "@none", "@type", "x")));
		assertFalse(NGSIObject.isDefaultDatasetId("@none"));
		assertFalse(NGSIObject.isDefaultDatasetId(null));
	}

	@Test
	public void onlyWhereEntitiesAreWritten() {
		for (int t : new int[] { AppConstants.ENTITY_CREATE_PAYLOAD, AppConstants.ENTITY_UPDATE_PAYLOAD,
				AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, AppConstants.MERGE_PATCH_PAYLOAD,
				AppConstants.REPLACE_ENTITY_PAYLOAD, AppConstants.TEMP_ENTITY_CREATE_PAYLOAD,
				AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, AppConstants.CREATE_REQUEST, AppConstants.APPEND_REQUEST,
				AppConstants.MERGE_PATCH_REQUEST }) {
			assertTrue(JsonLdApi.isEntityWritePayload(t), "payload " + t);
		}
		for (int t : new int[] { AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD,
				AppConstants.QUERY_PAYLOAD, AppConstants.ENTITY_RETRIEVED_PAYLOAD,
				AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, AppConstants.CSOURCE_REG_CREATE_PAYLOAD,
				AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, AppConstants.NOTIFICAITION_RECEIVED }) {
			assertFalse(JsonLdApi.isEntityWritePayload(t), "payload " + t);
		}
	}
}
