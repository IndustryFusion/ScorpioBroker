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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Every module that migrates the store carries the same migrations as the all-in-one runner, so a store migrated
 * by any of them gets the fork's migrations, and none of them sees a migration it doesn't know.
 */
public class MigrationCopiesTest {

	private static final Path OWN = Path.of("src", "main", "resources", "db", "migration");

	private static List<String> names(Path dir) throws IOException {
		try (Stream<Path> s = Files.list(dir)) {
			return s.map(p -> p.getFileName().toString()).sorted().toList();
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "AtContextServer", "EntityManager", "HistoryEntityManager", "HistoryQueryManager",
			"InfoManager", "QueryManager", "RegistryManager", "RegistrySubscriptionManager", "SubscriptionManager" })
	public void sameMigrations(String module) throws IOException {
		Path other = Path.of("..", module).resolve(OWN);
		List<String> own = names(OWN);
		assertEquals(own, names(other));
		for (String name : own) {
			assertArrayEquals(Files.readAllBytes(OWN.resolve(name)), Files.readAllBytes(other.resolve(name)), name);
		}
	}
}
