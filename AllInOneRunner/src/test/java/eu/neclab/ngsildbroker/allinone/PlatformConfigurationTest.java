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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;

/**
 * The all-in-one runner takes its settings from the variables the platform's chart sets
 * (contracts/context-broker: The broker runs with the platform's configuration), and still from upstream's.
 */
public class PlatformConfigurationTest {

	/** The variables of helm/charts/scorpio/templates/scorpio-aaio.yaml with the chart's values */
	private static Map<String, String> chart() {
		Map<String, String> env = new HashMap<>();
		env.put("CLIENT_ID", "scorpio");
		env.put("REALM", "iff");
		env.put("POSTGRES_PORT", "5432");
		env.put("KEYCLOAK_SERVER_URL", "http://keycloak.local/auth/");
		env.put("POSTGRES_SERVICE", "acid-test");
		env.put("POSTGRES_PASSWORD", "secret");
		env.put("POSTGRES_DBNAME", "ngb");
		env.put("SSL_REQUIRED", "none");
		env.put("KAFKA_PORT", "9092");
		env.put("KAFKA_BROKERS", "my-cluster-kafka-brokers");
		env.put("KAFKA_BOOTSTRAP", "my-cluster-kafka-brokers");
		env.put("ENTITY_TOPIC", "scorpio.entity");
		env.put("REGISTRY_TOPIC", "scorpio.registry");
		env.put("TEMPORAL_TOPIC", "scorpio.temporal");
		env.put("SUB_ALIVE_TOPIC", "scorpio.sub.alive");
		env.put("SUB_SYNC_TOPIC", "scorpio.sub.sync");
		return env;
	}

	private static SmallRyeConfig config(Map<String, String> env) throws IOException {
		ClassLoader cl = PlatformConfigurationTest.class.getClassLoader();
		return new SmallRyeConfigBuilder().addDefaultInterceptors()
				.withSources(new PropertiesConfigSource(cl.getResource("application.properties"), 250))
				.withSources(new PropertiesConfigSource(cl.getResource("application-kafka.properties"), 260))
				.withSources(new EnvConfigSource(env, 300)).build();
	}

	@Test
	public void chartVariables() throws IOException {
		SmallRyeConfig c = config(chart());
		assertEquals("jdbc:postgresql://acid-test:5432/ngb?ApplicationName=aio-runner&sslmode=require",
				c.getValue("quarkus.datasource.jdbc.url", String.class));
		assertEquals("postgresql://acid-test:5432/ngb?sslmode=require",
				c.getValue("quarkus.datasource.reactive.url", String.class));
		assertEquals("true", c.getValue("quarkus.datasource.reactive.trust-all", String.class));
		assertEquals("ngb", c.getValue("quarkus.datasource.username", String.class));
		assertEquals("secret", c.getValue("quarkus.datasource.password", String.class));
		assertEquals("my-cluster-kafka-brokers:9092", c.getValue("kafka.bootstrap.servers", String.class));
		assertEquals("scorpio.entity", c.getValue("scorpio.topics.entity", String.class));
		assertEquals("scorpio.registry", c.getValue("scorpio.topics.registry", String.class));
		assertEquals("scorpio.temporal", c.getValue("scorpio.topics.temporal", String.class));
		assertEquals("scorpio.sub.alive", c.getValue("scorpio.topics.subalive", String.class));
		assertEquals("scorpio.sub.sync", c.getValue("scorpio.topics.subsync", String.class));
		assertEquals("HIST_SUB_SYNC", c.getValue("scorpio.topics.histsync", String.class));
		assertEquals("scorpio.entity", c.getValue("mp.messaging.outgoing.entity.topic", String.class));
		assertEquals("http://keycloak.local/auth/realms/iff", c.getValue("quarkus.oidc.auth-server-url", String.class));
		assertEquals("scorpio", c.getValue("quarkus.oidc.client-id", String.class));
		assertEquals("none", c.getValue("quarkus.oidc.tls.verification", String.class));
	}

	/** The light stack of the OPC UA chain test sets the user, and a TLS mode can be chosen */
	@Test
	public void userAndSslMode() throws IOException {
		Map<String, String> env = chart();
		env.put("POSTGRES_USER", "dbowner");
		env.put("POSTGRES_SSLMODE", "verify-full");
		SmallRyeConfig c = config(env);
		assertEquals("dbowner", c.getValue("quarkus.datasource.username", String.class));
		assertEquals("postgresql://acid-test:5432/ngb?sslmode=verify-full",
				c.getValue("quarkus.datasource.reactive.url", String.class));
		env.put("POSTGRES_USERNAME", "owner2");
		assertEquals("owner2", config(env).getValue("quarkus.datasource.username", String.class));
	}

	/** Upstream's variable names keep working where the platform's are not set */
	@Test
	public void upstreamVariables() throws IOException {
		Map<String, String> env = new HashMap<>();
		env.put("DBHOST", "pg");
		env.put("DBPORT", "5433");
		env.put("DBUSER", "u");
		env.put("DBPASS", "p");
		env.put("DBNAME", "db");
		env.put("BUSHOST", "kafka");
		env.put("BUSPORT", "9093");
		SmallRyeConfig c = config(env);
		assertEquals("jdbc:postgresql://pg:5433/db?ApplicationName=aio-runner&sslmode=require",
				c.getValue("quarkus.datasource.jdbc.url", String.class));
		assertEquals("u", c.getValue("quarkus.datasource.username", String.class));
		assertEquals("p", c.getValue("quarkus.datasource.password", String.class));
		assertEquals("kafka:9093", c.getValue("kafka.bootstrap.servers", String.class));
		assertEquals("ENTITY", c.getValue("scorpio.topics.entity", String.class));
	}
}
