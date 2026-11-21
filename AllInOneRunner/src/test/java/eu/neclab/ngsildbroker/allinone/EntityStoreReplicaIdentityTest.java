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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;

/**
 * REPLICA IDENTITY FULL on the entity table (V20261121.1), on the store the all-in-one runner migrated at its start.
 */
@QuarkusTest
@TestProfile(PlatformProfile.class)
public class EntityStoreReplicaIdentityTest {

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

	/**
	 * Entity changes are published through the entity store: updates and deletes carry the complete old row
	 */
	@Test
	public void entityTableHasReplicaIdentityFull() throws SQLException {
		assertEquals("f", query("SELECT relreplident::text FROM pg_class WHERE oid = 'public.entity'::regclass"));
		assertEquals("true", query("SELECT success::text FROM flyway_schema_history WHERE version = '20261121.1'"));
	}
}
