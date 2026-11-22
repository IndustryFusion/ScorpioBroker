# Patches of the IndustryFusion fork

This branch is upstream Scorpio `quarkus-release/6.0.2` plus the patches below. The platform,
[IndustryFusion/DigitalTwin](https://github.com/IndustryFusion/DigitalTwin), states what it expects from
its context broker in the specification `contracts/context-broker`. NGSI-LD, ETSI GS CIM 009 V1.9.1, is
the reference: every patch follows it, names the requirement of `contracts/context-broker` it serves and
has a unit test here. A patch that serves no requirement is dropped on the next upstream update.

## Carried

| Patch | Requirement of `contracts/context-broker` | Unit test |
|---|---|---|
| Access control: `quarkus-oidc` and `quarkus-keycloak-authorization`, `@RolesAllowed` on every NGSI-LD endpoint with the roles of the fork on 5.0.5; the identity provider from `KEYCLOAK_SERVER_URL`, `REALM`, `CLIENT_ID`, or a public key and issuer instead (`QUARKUS_OIDC_PUBLIC_KEY`, `QUARKUS_OIDC_TOKEN_ISSUER`); roles as client roles of `CLIENT_ID` and as realm roles. From `67cdf1c`, `a754fd8`. Fixed on the port: `GET /subscriptions/{id}` named one role `"Factory-Admin, Subscriber, Factory-Reader"` (T7, CIM 009 6.11.3.1). Beyond 5.0.5: the entity maps (`/ngsi-ld/v1/entityMap`, query, retrieve, delete, update) have the read roles, as a query; `POST /ngsi-ld/v1/jsonldContexts/` and `DELETE /ngsi-ld/v1/jsonldContexts/{id}` need `Factory-Admin` | *Requests need a token with a platform role*, *Factory-Admin may do what the platform does*, *Factory-Reader only reads*, *Subscriptions and notifications* (T7) | `EndpointRolesTest`, `AccessControlTest` |
| Configuration through the chart's variables: `POSTGRES_SERVICE`, `POSTGRES_PORT`, `POSTGRES_USERNAME`/`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DBNAME`, `KAFKA_BOOTSTRAP`, `KAFKA_PORT`, the topic variables; `sslmode=require` on the JDBC and reactive URL, `quarkus.datasource.reactive.trust-all`. From `67cdf1c` | *The broker runs with the platform's configuration* | `PlatformConfigurationTest` |
| `ALTER TABLE public.entity REPLICA IDENTITY FULL` as the migration `V20261121.1` in every module that migrates the store. From `67cdf1c`, which edited it into `V20190604.1` | *Entity changes are published through the entity store* | `EntityStoreReplicaIdentityTest`, `MigrationCopiesTest` |
| Merge of a typed value, a language-tagged string or a node reference as a whole (`merge_has_value`, `merge_has_value_list`), the migration `V20261120.1` of `e1cea4b` unchanged (IndustryFusion/ScorpioBroker#8); `quarkus.flyway.out-of-order` for stores that already hold it | *Merge of typed and language-tagged values* (accepted deviation A1, CIM 009 3.1, 5.5.12) | `MergeValueObjectsTest` |
| Image `ibn40/scorpio-all-in-one-runner:<docker-tag>`; the Dockerfile sets no `DBHOST`, `BUSHOST`. From `45e40a4` | *The broker runs with the platform's configuration* (the image the chart starts) | `ImageNameTest` |
| History recording off by default: `scorpio.history.autorecording=${SCORPIO_HISTORY_AUTORECORDING:false}` (upstream: `true`). With the history managers in the build every entity change would be written to the temporal tables, growing without bound, while the platform doesn't use the temporal API | *What the platform doesn't rely on* (the temporal API) | `PlatformConfigurationTest` |
| `"datasetId": "@none"` names the default Attribute instance wherever an entity is written (CIM 009 4.5.5); it answered `404` | *Default attribute instance* (accepted deviation A2) | `DefaultDatasetIdTest` (Commons and AllInOneRunner) |
| A batch merge or batch update whose only entity is refused answers `207` with a BatchOperationResult (CIM 009 6.31.3.1, 6.16.3.1, 5.2.17); it answered `404`. A batch whose entities are all refused keeps `"success": []`, cardinality 1 in a BatchOperationResult (CIM 009 5.2.16); it was left out | *Refusals the platform relies on*, scenario *Null value in a batch of one* (accepted deviation A3) | `BatchRefusalTest` |
| A subscription notifies over MQTT without `receiverInfo` (and without `notifierInfo`), both optional (CIM 009 5.2.15): no extra key, certificate or trust-all for the client, no `metadata` in the message (7.2). Opening the MQTT client to a host failed with a `NullPointerException` on the missing `receiverInfo`, so after a restart notifications were lost until a subscription with a `receiverInfo` had connected to the same host | *Subscriptions and notifications* (notify over MQTT) | `MqttNotificationTest` |
| An Attribute instance's datasetIds are its own (CIM 009 4.5.5): a sub-attribute is an attribute of its own (4.5.2, 4.5.3) whose instances are validated against their own datasetIds. Expansion added a sub-attribute's datasetIds (or its default instance) to the instance that holds it, so an instance with a `datasetId` and a sub-attribute written as an array, or a sub-attribute instance with a `datasetId` of its own, was refused with `400` "Duplicated datasetId or multiple entries with no datasetId found". Two default instances of one attribute, or of one sub-attribute, are still refused | *Value forms the broker stores* (sub-attributes; accepted deviation A5) | `SubAttributeDatasetIdTest` (Commons and AllInOneRunner) |

Endpoints without a role check:

- the at-context server's reads (`GET /ngsi-ld/v1/jsonldContexts/`, `GET /ngsi-ld/v1/jsonldContexts/{id}`) and
  `GET /ngsi-ld/v1/jsonldContexts/createcache/{url}`, `POST /ngsi-ld/v1/jsonldContexts/createimplicitly/`: the
  broker calls them itself without a token. The ingress should not expose `createcache` and `createimplicitly`;
  the platform relies on neither (*What the platform doesn't rely on*).
- `/remotenotify`: a forged call needs the random callback id of a subscription the broker made at a federated
  context source, and registering a context source needs `Factory-Admin`.

## Dropped

| Commit of the fork on 5.0.5 | Change | Why |
|---|---|---|
| `135b5ee` | one notification for a whole batch merge, instead of one per entity | serves no requirement: *Subscriptions and notifications* holds for both, and the standard leaves the number of notifications open |
| `67cdf1c`, `a754fd8` | history managers out of the build (the at-context server back in) | serves no requirement: the platform doesn't rely on the temporal API (*What the platform doesn't rely on*); upstream's default stays, with access control on the temporal endpoints |

## Build

6.0.2 needs JDK 21 and Maven 3.9:

    mvn clean package -DskipTests -Ddocker -Ddocker-tag=<tag> -Dkafka -Pkafka -Dquarkus.profile=kafka

Without a local JDK 21, compile in a container and build the image on the host:

    docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -v "$PWD:/src" -v "$HOME/.m2:/m2" -w /src \
      maven:3.9-eclipse-temurin-21 mvn -B -Dmaven.repo.local=/m2/repository clean package -DskipTests \
      -Dkafka -Pkafka -Dquarkus.profile=kafka -pl AllInOneRunner -am
    docker build -f AllInOneRunner/src/main/docker/Dockerfile.jvm -t ibn40/scorpio-all-in-one-runner:<tag> AllInOneRunner

## Tests

The tests of the patches are in `AllInOneRunner/src/test` and `Commons/src/test`. The ones that start the
all-in-one runner need Postgres with PostGIS and TLS, and Kafka, named by the variables the runner reads
(`dbport`, `busport`, `POSTGRES_SERVICE` ...):

    mvn test -Dkafka -Pkafka -pl Commons,AllInOneRunner -am -Dsurefire.failIfNoSpecifiedTests=false \
      -Dtest='DefaultDatasetIdTest,EndpointRolesTest,PlatformConfigurationTest,MigrationCopiesTest,AccessControlTest,EntityStoreReplicaIdentityTest,MergeValueObjectsTest,ImageNameTest,BatchRefusalTest,SubAttributeDatasetIdTest,MqttNotificationTest'

`MqttNotificationTest` needs neither: it runs the MQTT notification path against an MQTT server of its own.
The runner listens on port 19190 in these tests (`-Daio.test.port` changes it), as it calls its own
at-context server.
