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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;
import io.vertx.core.json.JsonObject;

/**
 * The all-in-one runner as the platform's light stack starts it: the kafka profile, and instead of an identity
 * provider a public key and any issuer (QUARKUS_OIDC_PUBLIC_KEY, QUARKUS_OIDC_TOKEN_ISSUER=any), with REALM and
 * CLIENT_ID set and KEYCLOAK_SERVER_URL not. The database and Kafka are the ones the variables name (dbport,
 * busport, POSTGRES_SERVICE ...), as for the other modules' tests. The key pair is kept in target/ so that every
 * call of the profile and the tests use the same one.
 */
public class PlatformProfile implements QuarkusTestProfile {

	private static final Path DIR = Path.of("target", "token-test");
	private static final String PORT = System.getProperty("aio.test.port", "19190");

	@Override
	public String getConfigProfile() {
		return "kafka";
	}

	@Override
	public Map<String, String> getConfigOverrides() {
		return Map.of("quarkus.oidc.public-key", Base64.getEncoder().encodeToString(publicKey()),
				"quarkus.oidc.token.issuer", "any", "REALM", "iff", "CLIENT_ID", "scorpio",
				// the runner calls its own at-context server, so it listens where it expects itself
				"quarkus.http.test-port", PORT, "scorpio.atcontexturl", "http://localhost:" + PORT,
				"scorpio.querymanager.url", "http://localhost:" + PORT);
	}

	private static synchronized byte[] publicKey() {
		try {
			Path pub = DIR.resolve("public.der");
			if (!Files.exists(pub)) {
				KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
				g.initialize(2048);
				KeyPair kp = g.generateKeyPair();
				Files.createDirectories(DIR);
				Files.write(DIR.resolve("private.der"), kp.getPrivate().getEncoded());
				Files.write(pub, kp.getPublic().getEncoded());
			}
			return Files.readAllBytes(pub);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (GeneralSecurityException e) {
			throw new IllegalStateException(e);
		}
	}

	private static String b64(byte[] b) {
		return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
	}

	/** A token signed with the profile's key, with the given claims, as Keycloak's tokens carry roles */
	public static String token(Map<String, Object> claims) {
		try {
			publicKey();
			PrivateKey key = KeyFactory.getInstance("RSA")
					.generatePrivate(new PKCS8EncodedKeySpec(Files.readAllBytes(DIR.resolve("private.der"))));
			long now = Instant.now().getEpochSecond();
			JsonObject body = new JsonObject().put("sub", "unit-test").put("iat", now).put("exp", now + 600)
					.put("azp", "scorpio").put("aud", List.of("account"));
			claims.forEach(body::put);
			String unsigned = b64(new JsonObject().put("alg", "RS256").put("typ", "JWT").encode()
					.getBytes(StandardCharsets.UTF_8)) + "." + b64(body.encode().getBytes(StandardCharsets.UTF_8));
			Signature s = Signature.getInstance("SHA256withRSA");
			s.initSign(key);
			s.update(unsigned.getBytes(StandardCharsets.US_ASCII));
			return unsigned + "." + b64(s.sign());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (GeneralSecurityException e) {
			throw new IllegalStateException(e);
		}
	}

	/** A token with the roles as roles of the client scorpio */
	public static String clientRoles(String... roles) {
		return token(Map.of("resource_access", Map.of("scorpio", Map.of("roles", List.of(roles)))));
	}

	/** A token with the roles as realm roles */
	public static String realmRoles(String... roles) {
		return token(Map.of("realm_access", Map.of("roles", List.of(roles))));
	}
}
