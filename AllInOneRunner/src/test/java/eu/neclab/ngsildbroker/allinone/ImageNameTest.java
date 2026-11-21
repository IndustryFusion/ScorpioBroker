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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * The platform's build (DigitalTwin test/build-local-platform.sh) runs
 * {@code mvn clean package -DskipTests -Ddocker -Ddocker-tag=<tag> -Dkafka -Pkafka -Dquarkus.profile=kafka} and
 * deploys ibn40/scorpio-all-in-one-runner:<tag>, configured by its chart's variables only.
 */
public class ImageNameTest {

	private static String docker(String pom, String property) {
		String profile = pom.substring(pom.indexOf("<id>docker</id>"));
		profile = profile.substring(0, profile.indexOf("</profile>"));
		Matcher m = Pattern.compile("<" + Pattern.quote(property) + ">([^<]*)</").matcher(profile);
		return m.find() ? m.group(1) : null;
	}

	@Test
	public void imageName() throws IOException {
		String pom = Files.readString(Path.of("pom.xml"));
		assertEquals("ibn40", docker(pom, "quarkus.container-image.registry"));
		assertEquals("scorpio-${project.artifactId}", docker(pom, "quarkus.container-image.name"));
		assertEquals("${docker-tag}", docker(pom, "quarkus.container-image.tag"));
		assertEquals("", docker(pom, "quarkus.container-image.group"));
		assertEquals("all-in-one-runner", Pattern.compile("</parent>\\s*<artifactId>([^<]*)<").matcher(pom).results()
				.findFirst().orElseThrow().group(1));
	}

	/** Upstream's defaults for dbhost and bushost would hide a missing platform variable */
	@Test
	public void noUpstreamHostDefaults() throws IOException {
		String dockerfile = Files.readString(Path.of("src", "main", "docker", "Dockerfile.jvm"));
		assertFalse(Pattern.compile("(?m)^\\s*ENV\\s+(DBHOST|BUSHOST)\\b").matcher(dockerfile).find(), dockerfile);
	}
}
