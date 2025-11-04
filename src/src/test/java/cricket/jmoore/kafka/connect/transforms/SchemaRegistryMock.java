/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import org.apache.avro.Schema;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class SchemaRegistryMock implements BeforeEachCallback, AfterEachCallback {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryMock.class);

    public enum Role {
        SOURCE,
        TARGET;
    }

    private static final String SCHEMA_REGISTRATION_PATTERN = "/subjects/[^/]+/versions";
    private static final String SCHEMA_BY_ID_PATTERN = "/schemas/ids/\\d+";
    private static final String CONFIG_PATTERN = "/config";
    private static final int IDENTITY_MAP_CAPACITY = 1000;

    private final String basicAuthTag;
    private final String basicAuthCredentials;

    private Function<MappingBuilder, StubMapping> stubFor;

    private final ListVersionsHandler listVersionsHandler;
    private final GetVersionHandler getVersionHandler;
    private final AutoRegistrationHandler autoRegistrationHandler;
    private final GetSchemasHandler getSchemasHandler;
    private final GetConfigHandler getConfigHandler;

    private final WireMockServer mockSchemaRegistry;
    private final SchemaRegistryClient schemaRegistryClient;

    public SchemaRegistryMock(Role role) {
        if (role == null) {
            throw new IllegalArgumentException("Role must be either SOURCE or TARGET");
        }

        this.basicAuthTag = (role == Role.SOURCE) ? Constants.USE_BASIC_AUTH_SOURCE_TAG : Constants.USE_BASIC_AUTH_TARGET_TAG;
        this.basicAuthCredentials = (role == Role.SOURCE)? Constants.HTTP_AUTH_SOURCE_CREDENTIALS_FIXTURE : Constants.HTTP_AUTH_TARGET_CREDENTIALS_FIXTURE;

        this.listVersionsHandler = new ListVersionsHandler();
        this.getVersionHandler = new GetVersionHandler();
        this.autoRegistrationHandler = new AutoRegistrationHandler();
        this.getConfigHandler = new GetConfigHandler();
        this.getSchemasHandler = new GetSchemasHandler();

        this.mockSchemaRegistry = new WireMockServer(
                WireMockConfiguration.wireMockConfig().dynamicPort().extensions(
                        this.autoRegistrationHandler,
                        this.listVersionsHandler,
                        this.getVersionHandler,
                        this.getSchemasHandler,
                        this.getConfigHandler));

        this.schemaRegistryClient = new MockSchemaRegistryClient();
    }

    @Override
    public void afterEach(final ExtensionContext context) {
        this.mockSchemaRegistry.stop();
    }

    @Override
    public void beforeEach(final ExtensionContext context) {

        if (context.getTags().contains(this.basicAuthTag)) {
            final String[] userPass = this.basicAuthCredentials.split(":");
            this.stubFor = (MappingBuilder mappingBuilder) -> this.mockSchemaRegistry.stubFor(
                    mappingBuilder.withBasicAuth(userPass[0], userPass[1]));
        } else {
            this.stubFor = this.mockSchemaRegistry::stubFor;
        }

        // GET /subjects/{subject}/versions
        this.stubFor.apply(WireMock.get(urlPathMatching(SCHEMA_REGISTRATION_PATTERN))
                .willReturn(WireMock.aResponse()
                    .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .withTransformers(this.listVersionsHandler.getName())));

        // POST /subjects/{subject}/versions
        this.stubFor.apply(WireMock.post(urlPathMatching(SCHEMA_REGISTRATION_PATTERN))
                .willReturn(WireMock.aResponse()
                        .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .withTransformers(this.autoRegistrationHandler.getName())));

        // Get /subjects/{subject}/versions/(latest|{id})
        this.stubFor.apply(WireMock.get(urlPathMatching(SCHEMA_REGISTRATION_PATTERN + "/(?:latest|\\d+)"))
                .willReturn(WireMock.aResponse()
                        .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .withTransformers(this.getVersionHandler.getName())));

        // GET /config
        this.stubFor.apply(WireMock.get(urlPathMatching(CONFIG_PATTERN))
                .willReturn(WireMock.aResponse()
                        .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .withTransformers(this.getConfigHandler.getName())));

        // GET /schemas/ids/{id}
        this.stubFor.apply(WireMock.get(urlPathMatching(SCHEMA_BY_ID_PATTERN))
                .willReturn(WireMock.aResponse()
                        .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .withTransformers(this.getSchemasHandler.getName())));

        this.mockSchemaRegistry.start();
    }

    public int registerSchema(final String topic, boolean isKey, final ParsedSchema schema) {
        return this.registerSchema(topic, isKey, schema, new TopicNameStrategy());
    }

    public int registerSchema(final String topic, boolean isKey, final ParsedSchema schema, SubjectNameStrategy strategy) {
        String subject = strategy.subjectName(topic, isKey, schema);
        return this.register(subject, schema);
    }

    private int register(final String subject, final ParsedSchema schema) {
        try {
            return this.schemaRegistryClient.register(subject, schema);
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client: {}", e);
        }
    }

    private List<Integer> listVersions(String subject) {
        try {
            return this.schemaRegistryClient.getAllVersions(subject);
        } catch (RestClientException e) {
            return Collections.emptyList();
        } catch (IOException e) {
            throw new IllegalStateException("Internal error in mock schema registry client: ", e);
        }
    }

    private SchemaMetadata getSubjectVersion(String subject, Object version) {
        try {
            if (version instanceof String && version.equals("latest")) {
                return this.schemaRegistryClient.getLatestSchemaMetadata(subject);
            } else if (version instanceof Number) {
                return this.schemaRegistryClient.getSchemaMetadata(subject, ((Number) version).intValue());
            } else {
                throw new IllegalArgumentException("Only 'latest' or integer versions are allowed");
            }
        } catch (IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private ParsedSchema getSchemaById(int schemaId) {
        try {
            return this.schemaRegistryClient.getSchemaById(schemaId);
        } catch (IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private String getCompatibility(String subject) {
        if (subject == null) {
            log.debug("Requesting registry base compatibility");
        } else {
            log.debug("Requesting compatibility for subject {}", subject);
        }
        try {
            return this.schemaRegistryClient.getCompatibility(subject);
        } catch (IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    public SchemaRegistryClient getSchemaRegistryClient() {
        return new CachedSchemaRegistryClient(this.getUrl(), IDENTITY_MAP_CAPACITY);
    }

    public String getUrl() {
        return "http://localhost:" + this.mockSchemaRegistry.port();
    }

    private abstract static class SubjectsVersionHandler implements ResponseDefinitionTransformerV2 {
        // Expected url pattern /subjects/.*-value/versions
        protected final Splitter urlSplitter = Splitter.on('/').omitEmptyStrings();

        protected String getSubject(Request request) {
            return Iterables.get(this.urlSplitter.split(request.getUrl()), 1);
        }

        @Override
        public boolean applyGlobally() {
            return false;
        }
    }

    private class AutoRegistrationHandler extends SubjectsVersionHandler {

        @Override
        public ResponseDefinition transform(ServeEvent serveEvent) {
            try {
                final int id = SchemaRegistryMock.this.register(getSubject(serveEvent.getRequest()),
                        new AvroSchema(new Schema.Parser()
                                .parse(RegisterSchemaRequest.fromJson(serveEvent.getRequest().getBodyAsString()).getSchema())));
                final RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
                registerSchemaResponse.setId(id);
                return ResponseDefinitionBuilder.jsonResponse(registerSchemaResponse);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Cannot parse schema registration request", e);
            }
        }

        @Override
        public String getName() {
            return AutoRegistrationHandler.class.getSimpleName();
        }
    }

    private class ListVersionsHandler extends SubjectsVersionHandler {

        @Override
        public ResponseDefinition transform(ServeEvent serveEvent) {
            final List<Integer> versions = SchemaRegistryMock.this.listVersions(getSubject(serveEvent.getRequest()));
            return ResponseDefinitionBuilder.jsonResponse(versions);
        }

        @Override
        public String getName() {
            return ListVersionsHandler.class.getSimpleName();
        }
    }

    private class GetVersionHandler extends SubjectsVersionHandler {

        @Override
        public ResponseDefinition transform(ServeEvent serveEvent) {
            String versionStr = Iterables.get(this.urlSplitter.split(serveEvent.getRequest().getUrl()), 3).replaceAll("[^0-9]", "");
            SchemaMetadata metadata;
            if (versionStr.equals("latest")) {
                metadata = SchemaRegistryMock.this.getSubjectVersion(getSubject(serveEvent.getRequest()), versionStr);
            } else {
                int version = Integer.parseInt(versionStr);
                metadata = SchemaRegistryMock.this.getSubjectVersion(getSubject(serveEvent.getRequest()), version);
            }
            return ResponseDefinitionBuilder.jsonResponse(metadata);
        }

        @Override
        public String getName() {
            return GetVersionHandler.class.getSimpleName();
        }
    }

    public class GetSchemasHandler extends SubjectsVersionHandler{

        @Override
        public ResponseDefinition transform(ServeEvent serveEvent) {
            try {
                int schemaId = Integer.parseInt(Iterables.get(this.urlSplitter.split(serveEvent.getRequest().getUrl()), 2).replaceAll("[^0-9]", ""));
                AvroSchema schema = (AvroSchema) SchemaRegistryMock.this.getSchemaById(schemaId);
                return ResponseDefinitionBuilder.jsonResponse(schema.canonicalString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Cannot parse schema request: ", e);
            }
        }

        @Override
        public String getName() {
            return GetSchemasHandler.class.getSimpleName();
        }
    }

    private class GetConfigHandler extends SubjectsVersionHandler {

        @Override
        public ResponseDefinition transform(ServeEvent serveEvent) {
            Config config = new Config(SchemaRegistryMock.this.getCompatibility(getSubject(serveEvent.getRequest())));
            return ResponseDefinitionBuilder.jsonResponse(config);
        }
        @Override
        protected String getSubject(Request request) {
            List<String> parts = StreamSupport
                    .stream(this.urlSplitter.split(request.getUrl()).spliterator(), false)
                    .collect(Collectors.toList());

            // Return null when path is only /config
            return parts.size() < 2 ? null : parts.get(1);
        }

        @Override
        public String getName() {
            return GetConfigHandler.class.getSimpleName();
        }
    }
}
