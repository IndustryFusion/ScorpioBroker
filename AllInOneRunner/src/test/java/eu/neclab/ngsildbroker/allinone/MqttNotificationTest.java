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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.tools.SubscriptionTools;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.mqtt.MqttClient;

/**
 * A subscription notifies over MQTT whether or not its endpoint has a receiverInfo: receiverInfo and notifierInfo
 * are optional (CIM 009 5.2.15), without a receiverInfo the MQTT message has no metadata (7.2). The broker opened its
 * MQTT client to a host with the endpoint's receiverInfo and failed with a NullPointerException when there was none,
 * so after a restart notifications were lost until a subscription with a receiverInfo had connected to the same
 * host (contracts/context-broker: Subscriptions and notifications).
 *
 * The notification path of SubscriptionService.sendNotification is run here against an MQTT server in the test:
 * getMqttClient, then the payload and the QoS the service publishes with.
 */
public class MqttNotificationTest {

	private static final Duration WAIT = Duration.ofSeconds(10);

	private Vertx vertx;
	private MqttServer server;
	private final LinkedBlockingQueue<MqttPublishMessage> received = new LinkedBlockingQueue<>();

	@BeforeEach
	public void startServer() throws Exception {
		vertx = Vertx.vertx();
		server = MqttServer.create(vertx.getDelegate(), new MqttServerOptions().setHost("127.0.0.1").setPort(0));
		server.endpointHandler(endpoint -> {
			endpoint.publishHandler(message -> {
				received.add(message);
				if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
					endpoint.publishAcknowledge(message.messageId());
				}
			});
			endpoint.accept(false);
		});
		server.listen().toCompletionStage().toCompletableFuture().get(WAIT.toSeconds(), TimeUnit.SECONDS);
	}

	@AfterEach
	public void stop() {
		server.close();
		vertx.closeAndAwait();
	}

	private SubscriptionService service() throws Exception {
		SubscriptionService service = new SubscriptionService();
		Field field = SubscriptionService.class.getDeclaredField("vertx");
		field.setAccessible(true);
		field.set(service, vertx);
		return service;
	}

	private NotificationParam notificationParam(ArrayListMultimap<String, String> receiverInfo) {
		EndPoint endPoint = new EndPoint();
		endPoint.setUri(URI.create("mqtt://127.0.0.1:" + server.actualPort() + "/iff/notify"));
		endPoint.setAccept("application/json");
		if (receiverInfo != null) {
			endPoint.setReceiverInfo(receiverInfo);
		}
		NotificationParam notificationParam = new NotificationParam();
		notificationParam.setEndPoint(endPoint);
		return notificationParam;
	}

	@SuppressWarnings("unchecked")
	private static MqttClient getMqttClient(SubscriptionService service, NotificationParam notificationParam)
			throws Exception {
		Method method = SubscriptionService.class.getDeclaredMethod("getMqttClient", NotificationParam.class);
		method.setAccessible(true);
		return ((Uni<MqttClient>) method.invoke(service, notificationParam)).await().atMost(WAIT);
	}

	/** Publishes a notification as sendNotification does and returns the message the server received */
	private JsonObject notify(SubscriptionService service, NotificationParam notificationParam) throws Exception {
		MqttClient client = getMqttClient(service, notificationParam);
		assertTrue(client.isConnected());
		Map<String, Object> notification = Map.of("id", "urn:ngsi-ld:Notification:1", "type", "Notification",
				"data", List.of(Map.of("id", "urn:test:1", "type", "Test")));
		client.publish(notificationParam.getEndPoint().getUri().getPath().substring(1),
				Buffer.buffer(SubscriptionTools.getMqttPayload(notificationParam, notification)),
				MqttQoS.valueOf(SubscriptionTools.getMqttQos(notificationParam)), false, false).await()
				.atMost(WAIT);
		MqttPublishMessage message = received.poll(WAIT.toSeconds(), TimeUnit.SECONDS);
		assertNotNull(message, "the MQTT server received no notification");
		assertEquals("iff/notify", message.topicName());
		JsonObject payload = message.payload().toJsonObject();
		assertEquals("urn:ngsi-ld:Notification:1", payload.getJsonObject("body").getString("id"));
		return payload;
	}

	/** The first subscription to a host has no receiverInfo, as after a restart of the broker */
	@Test
	public void notifiesWithoutReceiverInfo() throws Exception {
		JsonObject payload = notify(service(), notificationParam(null));
		assertFalse(payload.containsKey("metadata"), payload.encode());
	}

	/** An empty receiverInfo means the same: no metadata */
	@Test
	public void notifiesWithEmptyReceiverInfo() throws Exception {
		JsonObject payload = notify(service(), notificationParam(ArrayListMultimap.create()));
		assertFalse(payload.containsKey("metadata"), payload.encode());
	}

	/** As before: the receiverInfo goes into the metadata of the MQTT message */
	@Test
	public void notifiesWithReceiverInfo() throws Exception {
		ArrayListMultimap<String, String> receiverInfo = ArrayListMultimap.create();
		receiverInfo.put("Fiware-Service", "iff");
		JsonObject payload = notify(service(), notificationParam(receiverInfo));
		assertEquals("iff", payload.getJsonArray("metadata").getJsonObject(0).getString("Fiware-Service"));
	}

	/** Both on one service, the one without receiverInfo first: the client it connects serves the other */
	@Test
	public void withoutThenWithReceiverInfo() throws Exception {
		SubscriptionService service = service();
		notify(service, notificationParam(null));
		ArrayListMultimap<String, String> receiverInfo = ArrayListMultimap.create();
		receiverInfo.put("Fiware-Service", "iff");
		notify(service, notificationParam(receiverInfo));
	}

	/**
	 * An endpoint without notifierInfo gets the default QoS 0; a notifierInfo of null or without the QoS means 1, as
	 * sendNotification had it
	 */
	@Test
	public void qosWithoutNotifierInfo() {
		NotificationParam notificationParam = notificationParam(null);
		assertEquals(0, SubscriptionTools.getMqttQos(notificationParam));
		notificationParam.getEndPoint().setNotifierInfo(null);
		assertEquals(1, SubscriptionTools.getMqttQos(notificationParam));
		notificationParam.getEndPoint().setNotifierInfo(Map.of());
		assertEquals(1, SubscriptionTools.getMqttQos(notificationParam));
		notificationParam.getEndPoint().setNotifierInfo(Map.of(NGSIConstants.MQTT_QOS, "1"));
		assertEquals(1, SubscriptionTools.getMqttQos(notificationParam));
	}
}
