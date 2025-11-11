/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.NonRecordContainer;

public class TransformTest {

    private enum ExplicitAuthType {
        USER_INFO,
        URL,
        NULL;
    }

    private static final Logger log = LoggerFactory.getLogger(TransformTest.class);

    public static final String TOPIC = TransformTest.class.getSimpleName();
    public static final String SUBJECT_KEY = TOPIC + "-key";
    public static final String SUBJECT_VALUE = TOPIC + "-value";

    private static final byte MAGIC_BYTE = (byte) 0x0;
    public static final int ID_SIZE = Integer.SIZE / Byte.SIZE;
    private static final int AVRO_CONTENT_OFFSET = 1 + ID_SIZE;
    public static final org.apache.avro.Schema INT_SCHEMA = org.apache.avro.Schema.create(INT);
    public static final org.apache.avro.Schema STRING_SCHEMA = org.apache.avro.Schema.create(STRING);
    public static final org.apache.avro.Schema BOOLEAN_SCHEMA = org.apache.avro.Schema.create(BOOLEAN);
    public static final org.apache.avro.Schema NAME_SCHEMA = SchemaBuilder.record("FullName")
            .namespace("cricket.jmoore.kafka.connect.transforms").fields()
            .requiredString("first")
            .requiredString("last")
            .endRecord();
    public static final org.apache.avro.Schema NAME_SCHEMA_ALIASED = SchemaBuilder.record("FullName")
            .namespace("cricket.jmoore.kafka.connect.transforms").fields()
            .requiredString("first")
            .name("surname").aliases("last").type().stringType().noDefault()
            .endRecord();

    @RegisterExtension
    final SchemaRegistryMock sourceSchemaRegistry =
        new SchemaRegistryMock(SchemaRegistryMock.Role.SOURCE);

    @RegisterExtension
    final SchemaRegistryMock targetSchemaRegistry =
        new SchemaRegistryMock(SchemaRegistryMock.Role.TARGET);

    private SchemaRegistryTransfer<SourceRecord> smt;
    private Map<String, Object> smtConfiguration;

    private Map<String, Object> getRequiredTransformConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConfigName.SOURCE_SCHEMA_REGISTRY_URL, sourceSchemaRegistry.getUrl());
        configs.put(ConfigName.TARGET_SCHEMA_REGISTRY_URL, targetSchemaRegistry.getUrl());
        return configs;
    }

    private void configure(boolean copyKeys) {
        smtConfiguration.put(ConfigName.TRANSFER_KEYS, copyKeys);
        smt.configure(smtConfiguration);
    }

    private void configure(boolean copyKeys, boolean copyHeaders) {
        smtConfiguration.put(ConfigName.TRANSFER_KEYS, copyKeys);
        smtConfiguration.put(ConfigName.INCLUDE_HEADERS, copyHeaders);
        smt.configure(smtConfiguration);
    }

    private void configure(final String sourceUserInfo, final String targetUserInfo, ExplicitAuthType credentialSource) {
        if (credentialSource == ExplicitAuthType.USER_INFO) {
            if (sourceUserInfo != null) {
                smtConfiguration.put(ConfigName.SOURCE_BASIC_AUTH_CREDENTIALS_SOURCE, Constants.USER_INFO_SOURCE);
                smtConfiguration.put(ConfigName.SOURCE_USER_INFO, sourceUserInfo);
            }

            if (targetUserInfo != null) {
                smtConfiguration.put(ConfigName.TARGET_BASIC_AUTH_CREDENTIALS_SOURCE, Constants.USER_INFO_SOURCE);
                smtConfiguration.put(ConfigName.TARGET_USER_INFO, targetUserInfo);
            }
        } else {
            if (sourceUserInfo != null) {
                String url = sourceSchemaRegistry.getUrl();
                url = url.replace("://", "://" + sourceUserInfo + "@" );
                smtConfiguration.put(ConfigName.SOURCE_SCHEMA_REGISTRY_URL, url);

                if (credentialSource == ExplicitAuthType.URL) {
                    smtConfiguration.put(ConfigName.SOURCE_BASIC_AUTH_CREDENTIALS_SOURCE, Constants.URL_SOURCE);
                } else if (credentialSource == ExplicitAuthType.NULL) {
                    // For an explicit null case, set both the URL and UserInfo to confirm that neither is found
                    smtConfiguration.put(ConfigName.SOURCE_BASIC_AUTH_CREDENTIALS_SOURCE, null);
                    smtConfiguration.put(ConfigName.SOURCE_USER_INFO, sourceUserInfo);
                } else {
                    // For null ExplicitAuthType, insert no key and rely on implicit default.
                }
            }

            if (targetUserInfo != null) {
                String url = targetSchemaRegistry.getUrl();
                url = url.replace("://", "://" + targetUserInfo + "@" );
                smtConfiguration.put(ConfigName.TARGET_SCHEMA_REGISTRY_URL, url);

                if (credentialSource == ExplicitAuthType.URL) {
                    smtConfiguration.put(ConfigName.TARGET_BASIC_AUTH_CREDENTIALS_SOURCE, Constants.URL_SOURCE);
                } else if (credentialSource == ExplicitAuthType.NULL) {
                    // For an explicit null case, set both the URL and UserInfo to confirm that neither is found
                    smtConfiguration.put(ConfigName.TARGET_BASIC_AUTH_CREDENTIALS_SOURCE, null);
                    smtConfiguration.put(ConfigName.TARGET_USER_INFO, targetUserInfo);
                } else {
                    // For null ExplicitAuthType, insert no key and rely on implicit default.
                }
            }
        }

        smt.configure(smtConfiguration);
    }

    private byte[] encodeAvroObject(org.apache.avro.Schema schema, int schemaId, Object datum) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            out.write(MAGIC_BYTE);
            out.write(ByteBuffer.allocate(ID_SIZE).putInt(schemaId).array());
            new GenericDatumWriter<>(schema).write(
                    datum instanceof NonRecordContainer ? ((NonRecordContainer) datum).getValue() : datum,
                    EncoderFactory.get().directBinaryEncoder(out, null)
            );
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Used to run a message through the SMT when testing authentication modes, which only need to
    // know if there was a communication error, but rely on other tests to verify schema transfers
    // are making the correct API calls.
    private void passSimpleMessage() throws IOException {
        final int sourceKeyId = sourceSchemaRegistry.registerSchema(TOPIC, true, new AvroSchema(STRING_SCHEMA));
        final int sourceValId = sourceSchemaRegistry.registerSchema(TOPIC, false, new AvroSchema(STRING_SCHEMA));

        final byte[] keyOut = encodeAvroObject(STRING_SCHEMA, sourceKeyId, "Hello, world");
        final byte[] valOut = encodeAvroObject(STRING_SCHEMA, sourceValId, "Hello, world");

        final SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                Schema.OPTIONAL_BYTES_SCHEMA,
                keyOut,
                Schema.OPTIONAL_BYTES_SCHEMA,
                valOut);

        smt.apply(testRecord);
    }

    @BeforeEach
    public void setup() {
        smt = new SchemaRegistryTransfer<>();
        smtConfiguration = getRequiredTransformConfigs();
    }

    @Test
    public void applyKeySchemaNotBytes() {
        configure(true);

        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                null,
                null,
                null,
                null);

        // The key schema is not a byte[]
        assertThrows(ConnectException.class, () -> smt.apply(testRecord));
    }

    @Test
    public void applyValueSchemaNotBytes() {
        configure(false);

        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                null,
                null,
                null,
                null);

        // The value schema is not a byte[]
        assertThrows(ConnectException.class, () -> smt.apply(testRecord));
    }

    @Test
    public void applySchemalessKeyBytesTooShort() {
        configure(true);

        // Allocate enough space for the magic-byte
        byte[] b = ByteBuffer.allocate(1).array();
        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                null,
                ByteBuffer.allocate(1).array(),
                null,
                null);

        // The key payload is not long enough for schema registry wire-format
        assertThrows(SerializationException.class, () -> smt.apply(testRecord));
    }

    @Test
    public void applySchemalessValueBytesTooShort() {
        configure(false);

        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                null,
                null,
                null,
                ByteBuffer.allocate(1).array());

        // The value payload is not long enough for schema registry wire-format
        assertThrows(SerializationException.class, () -> smt.apply(testRecord));
    }

    @Test
    public void testKeySchemaLookupFailure() {
        configure(true);

        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                Schema.OPTIONAL_BYTES_SCHEMA,
                ByteBuffer.allocate(6).array(),
                null,
                null);

        // Will attempt to lookup schema ID 0, which is invalid
        assertThrows(ConnectException.class, () -> smt.apply(testRecord));
    }

    @Test
    public void testValueSchemaLookupFailure() {
        configure(false);

        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                null,
                null,
                Schema.OPTIONAL_BYTES_SCHEMA,
                ByteBuffer.allocate(6).array());

        // Will attempt to lookup schema ID 0, which is invalid
        assertThrows(ConnectException.class, () -> smt.apply(testRecord));
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    @Tag(Constants.USE_BASIC_AUTH_TARGET_TAG)
    public void testBothBasicHttpAuthUserInfo() throws IOException {
        configure(
                Constants.HTTP_AUTH_SOURCE_CREDENTIALS_FIXTURE,
                Constants.HTTP_AUTH_TARGET_CREDENTIALS_FIXTURE,
                ExplicitAuthType.USER_INFO);
        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthUserInfo() throws IOException {
        configure(Constants.HTTP_AUTH_SOURCE_CREDENTIALS_FIXTURE, null, ExplicitAuthType.USER_INFO);
        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_TARGET_TAG)
    public void testDestinationBasicHttpAuthUserInfo() throws IOException {
        configure(null, Constants.HTTP_AUTH_TARGET_CREDENTIALS_FIXTURE, ExplicitAuthType.USER_INFO);
        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthUrl() throws IOException {
        configure(Constants.HTTP_AUTH_SOURCE_CREDENTIALS_FIXTURE, null, ExplicitAuthType.URL);
        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_TARGET_TAG)
    public void testDestinationBasicHttpAuthUrl() throws IOException {
        configure(null, Constants.HTTP_AUTH_TARGET_CREDENTIALS_FIXTURE, ExplicitAuthType.URL);
        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthNull() throws IOException {
        configure(Constants.HTTP_AUTH_SOURCE_CREDENTIALS_FIXTURE, null, ExplicitAuthType.NULL);
        assertThrows(ConnectException.class, this::passSimpleMessage);
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_TARGET_TAG)
    public void testDestinationBasicHttpAuthNull() throws IOException {
        configure(null, Constants.HTTP_AUTH_TARGET_CREDENTIALS_FIXTURE, ExplicitAuthType.NULL);
        assertThrows(ConnectException.class, this::passSimpleMessage);
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthImplicitDefault() throws IOException {
        configure(Constants.HTTP_AUTH_SOURCE_CREDENTIALS_FIXTURE, null, null);
        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_TARGET_TAG)
    public void testDestinationBasicHttpAuthImplicitDefault() throws IOException {
        configure(null, Constants.HTTP_AUTH_TARGET_CREDENTIALS_FIXTURE, null);
        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthWrong() {
        configure(Constants.HTTP_AUTH_TARGET_CREDENTIALS_FIXTURE, null, null);
        assertThrows(ConnectException.class, this::passSimpleMessage);
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_TARGET_TAG)
    public void testDestinationBasicHttpAuthWrong() {
        configure(null, Constants.HTTP_AUTH_SOURCE_CREDENTIALS_FIXTURE, null);
        assertThrows(ConnectException.class, this::passSimpleMessage);
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthOmit() throws IOException {
        configure(null, null, null);
        assertThrows(ConnectException.class, this::passSimpleMessage);
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_TARGET_TAG)
    public void testDestinationBasicHttpAuthOmit() throws IOException {
        configure(null, null, null);
        assertThrows(ConnectException.class, this::passSimpleMessage);
    }

    @Test
    public void testKeySchemaTransfer() throws RestClientException, IOException {

        configure(true);

        SchemaRegistryClient sourceClient = this.sourceSchemaRegistry.getSchemaRegistryClient();
        SchemaRegistryClient targetClient = this.targetSchemaRegistry.getSchemaRegistryClient();

        // Create new schema for source registry
        int sourceSchemaId = this.sourceSchemaRegistry.registerSchema(TOPIC, true, new AvroSchema(STRING_SCHEMA));
        assertEquals(1, sourceSchemaId, "Source registry starts at ID=1 when empty");

        // Verify only one schema for this subject exists in the source registry
        List<Integer> sourceSchemaVersions = sourceClient.getAllVersions(SUBJECT_KEY);
        assertEquals(1, sourceSchemaVersions.size(), "the source registry subject contains the pre-registered schema");

        byte[] sourceSchemaKeyBytes = encodeAvroObject(STRING_SCHEMA, sourceSchemaId, "hello, world");

        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                Schema.BYTES_SCHEMA,
                sourceSchemaKeyBytes,
                null,
                null);

        // Verify that target has no versions for this subject, while a pre-existing schema using a bogus subject value should not
        // affect the returned value
        this.targetSchemaRegistry.registerSchema(UUID.randomUUID().toString(), true, new AvroSchema(INT_SCHEMA));
        List<Integer> targetSchemaVersions = targetClient.getAllVersions(SUBJECT_KEY);
        assertTrue(targetSchemaVersions.isEmpty(), "the target registry contains no key schemas for the subject");

        // Verify that the transform will fail on the byte[]-less record value, which in this test-case
        // is empty due to the fact that it is a key-schema only
        ConnectException connectException = assertThrows(ConnectException.class, () -> smt.apply(testRecord));
        assertTrue(connectException.getMessage().contains("Transform failed"));

        // Verify that the target registry now has a population of one for this subject
        targetSchemaVersions = targetClient.getAllVersions(SUBJECT_KEY);
        assertEquals(sourceSchemaVersions.size(), targetSchemaVersions.size(),
                "source and destination registries have the same number of schemas for the same subject");

        // Verify that the IDs for the source- and target registry differ
        SchemaMetadata targetSchemaMetadata = targetClient.getSchemaMetadata(SUBJECT_KEY, targetSchemaVersions.get(0));
        assertTrue(sourceSchemaId < targetSchemaMetadata.getId(),
                "destination ID should be different and higher since that registry already had schemas");

        // Verify schemas are identical between source and target
        org.apache.avro.Schema sourceSchema = new org.apache.avro.Schema.Parser().parse(sourceClient.getSchemaById(sourceSchemaId).canonicalString());
        org.apache.avro.Schema targetSchema = new org.apache.avro.Schema.Parser().parse(targetSchemaMetadata.getSchema());
        assertEquals(sourceSchema, targetSchema, "source and target schemas are a match");
    }

    @Test
    public void testValueSchemaTransfer() throws RestClientException, IOException {

        configure(true);

        SchemaRegistryClient sourceClient = this.sourceSchemaRegistry.getSchemaRegistryClient();
        SchemaRegistryClient targetClient = this.targetSchemaRegistry.getSchemaRegistryClient();

        // Create new schema for source registry
        int sourceSchemaId = sourceSchemaRegistry.registerSchema(TOPIC, false, new AvroSchema(STRING_SCHEMA));
        assertEquals(1, sourceSchemaId, "Source registry starts at ID=1 when empty");

        // Verify only one schema for this subject exists in the source registry
        List<Integer> sourceSchemaVersions = sourceClient.getAllVersions(SUBJECT_VALUE);
        assertEquals(1, sourceSchemaVersions.size(), "the source registry subject contains the pre-registered schema");

        // Verify that target has no versions for this subject, while a pre-existing schema using a bogus subject value should not
        // affect the returned value
        this.targetSchemaRegistry.registerSchema(UUID.randomUUID().toString(), false, new AvroSchema(INT_SCHEMA));
        List<Integer> targetSchemaVersions = targetClient.getAllVersions(SUBJECT_VALUE);
        assertTrue(targetSchemaVersions.isEmpty(), "the target registry contains no value schemas for the subject");

        byte[] sourceSchemaValueBytes = encodeAvroObject(STRING_SCHEMA, sourceSchemaId, "hello, world");

        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                Schema.OPTIONAL_BYTES_SCHEMA,
                null,
                Schema.BYTES_SCHEMA,
                sourceSchemaValueBytes);

        SourceRecord appliedRecord = assertDoesNotThrow(() -> smt.apply(testRecord));

        // Verify basic integrity
        assertNotNull(appliedRecord);
        assertEquals(testRecord.keySchema(), appliedRecord.keySchema());
        assertEquals(testRecord.key(), appliedRecord.key());
        assertEquals(testRecord.valueSchema(), appliedRecord.valueSchema());

        // Verify that the target registry now has a population of one for this subject
        targetSchemaVersions = targetClient.getAllVersions(SUBJECT_VALUE);
        assertEquals(sourceSchemaVersions.size(), targetSchemaVersions.size(),
                "source and target registries have the same amount of schemas for the same subject");

        // Verify that the IDs for the source- and target registry differ
        SchemaMetadata targetSchemaMetadata = targetClient.getSchemaMetadata(SUBJECT_VALUE, targetSchemaVersions.get(0));
        int targetSchemaId = targetSchemaMetadata.getId();
        assertTrue(sourceSchemaId < targetSchemaMetadata.getId(),
                "destination ID should be different and higher since that registry already had schemas");

        // Verify schemas are identical between source and target
        org.apache.avro.Schema sourceSchema = new org.apache.avro.Schema.Parser().parse(sourceClient.getSchemaById(sourceSchemaId).canonicalString());
        org.apache.avro.Schema targetSchema = new org.apache.avro.Schema.Parser().parse(targetSchemaMetadata.getSchema());
        assertEquals(STRING_SCHEMA, sourceSchema, "source registry returned same schema");
        assertEquals(STRING_SCHEMA, targetSchema, "target registry returned same schema");
        assertEquals(sourceSchema, targetSchema, "source and target schemas are a match");


        // Verify the byte value of the test record transformed and that the Avro content is identical
        byte[] appliedSchemaValue = (byte[]) appliedRecord.value();
        ByteBuffer appliedValueBuffer = ByteBuffer.wrap(appliedSchemaValue);
        assertEquals(sourceSchemaValueBytes.length, appliedSchemaValue.length, "transformed record byte-value size unchanged");
        assertEquals(MAGIC_BYTE, appliedValueBuffer.get(), "transformed record value starts with magic byte");

        int transformedRecordSchemaId = appliedValueBuffer.getInt();
        assertNotEquals(sourceSchemaId, transformedRecordSchemaId, "schema ID of the transformed record changed");
        assertEquals(targetSchemaId, transformedRecordSchemaId, "schema ID of the transformed record matches target schema ID");

        assertArrayEquals(Arrays.copyOfRange(sourceSchemaValueBytes, AVRO_CONTENT_OFFSET, sourceSchemaValueBytes.length),
                Arrays.copyOfRange(appliedValueBuffer.array(), AVRO_CONTENT_OFFSET, appliedSchemaValue.length),
                "the avro data is not modified");
    }

    @Test
    public void testKeyValueSchemaTransfer() throws RestClientException, IOException {

        configure(true);

        SchemaRegistryClient sourceClient = this.sourceSchemaRegistry.getSchemaRegistryClient();
        SchemaRegistryClient targetClient = this.targetSchemaRegistry.getSchemaRegistryClient();

        org.apache.avro.Schema sourceTestKeySchema = INT_SCHEMA;
        org.apache.avro.Schema sourceTestValueSchema = STRING_SCHEMA;

        // Create new key and value schema for source registry
        int sourceKeyId = this.sourceSchemaRegistry.registerSchema(TOPIC, true, new AvroSchema(sourceTestKeySchema));
        assertEquals(1, sourceKeyId, "Source registry starts at ID=1 when empty");
        int sourceValueId = this.sourceSchemaRegistry.registerSchema(TOPIC, false, new AvroSchema(sourceTestValueSchema));
        assertEquals(2, sourceValueId, "Schema IDs are monotonically increasing");

        // Verify that target has no versions for this subject, while a pre-existing schema using a bogus subject value should not
        // affect the returned value
        targetSchemaRegistry.registerSchema(UUID.randomUUID().toString(), false, new AvroSchema(BOOLEAN_SCHEMA));
        List<Integer> targetKeyVersions = targetClient.getAllVersions(SUBJECT_KEY);
        assertTrue(targetKeyVersions.isEmpty(), "target registry contains no key schemas for the subject");
        List<Integer> targetValueVersions = targetClient.getAllVersions(SUBJECT_VALUE);
        assertTrue(targetValueVersions.isEmpty(), "target registry contains no value schemas for the subject");

        List<Integer> sourceKeyVersions = sourceClient.getAllVersions(SUBJECT_KEY);
        assertEquals(1, sourceKeyVersions.size(), "the source registry subject contains the pre-registered key schema");
        List<Integer> sourceValueVersions = sourceClient.getAllVersions(SUBJECT_VALUE);
        assertEquals(1, sourceValueVersions.size(), "the source registry subject contains the pre-registered value schema");

        byte[] sourceSchemaKeyBytes = encodeAvroObject(sourceTestKeySchema, sourceKeyId, AVRO_CONTENT_OFFSET);
        byte[] sourceSchemaValueBytes = encodeAvroObject(sourceTestValueSchema, sourceValueId, "hello, world");

        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                Schema.OPTIONAL_BYTES_SCHEMA,
                sourceSchemaKeyBytes,
                Schema.BYTES_SCHEMA,
                sourceSchemaValueBytes);

        SourceRecord appliedRecord = assertDoesNotThrow(() -> smt.apply(testRecord));

        assertNotNull(appliedRecord);
        assertEquals(testRecord.keySchema(), appliedRecord.keySchema(), "key schema unchanged");
        assertEquals(testRecord.valueSchema(), appliedRecord.valueSchema(), "value schema unchanged");

        // Verify that the target registry now has a population of one for this subject for both key and value
        targetKeyVersions = targetClient.getAllVersions(SUBJECT_KEY);
        assertEquals(1, targetKeyVersions.size(),
                "source and target registries have the same number of schemas for the key subject");
        targetValueVersions = targetClient.getAllVersions(SUBJECT_VALUE);
        assertEquals(1, targetValueVersions.size(),
                "source and target registries have the same number of schemas for the value subject");

        // Verify that the IDs for the source- and target registry differ for both key and value
        SchemaMetadata targetKeyMetadata = targetClient.getSchemaMetadata(SUBJECT_KEY, targetKeyVersions.get(0));
        int targetKeyId = targetKeyMetadata.getId();
        assertTrue(sourceKeyId < targetKeyId,
                "target ID should be different and higher since that registry already had schemas");
        SchemaMetadata targetValueMetadata = targetClient.getSchemaMetadata(SUBJECT_VALUE, targetValueVersions.get(0));
        int targetValueId = targetValueMetadata.getId();
        assertTrue(sourceValueId < targetValueId,
                "target ID should be different and higher since that registry already had schemas");

        // Verify key schemas are identical between source and target
        org.apache.avro.Schema sourceKeySchema = new org.apache.avro.Schema.Parser().parse(sourceClient.getSchemaById(sourceKeyId).canonicalString());
        org.apache.avro.Schema targetKeySchema = new org.apache.avro.Schema.Parser().parse(targetKeyMetadata.getSchema());
        assertEquals(targetKeySchema, sourceKeySchema, "Source registry returned same key schema");
        assertEquals(sourceKeySchema, targetKeySchema, "Target registry returned same key schema");
        assertEquals(sourceKeySchema, targetKeySchema, "Source and target key schemas match");

        // Verify value schemas are identical between source and target
        org.apache.avro.Schema sourceValueSchema = new org.apache.avro.Schema.Parser().parse(sourceClient.getSchemaById(sourceValueId).canonicalString());
        org.apache.avro.Schema targetValueSchema = new org.apache.avro.Schema.Parser().parse(targetValueMetadata.getSchema());
        assertEquals(targetValueSchema, sourceValueSchema, "Source registry returned same value schema");
        assertEquals(sourceValueSchema, targetValueSchema, "Target registry returned same value schema");
        assertEquals(sourceValueSchema, targetValueSchema, "Source and target key schemas match");

        // Verify the byte key of the test record was transformed and that the Avro content is identical
        byte[] appliedKey = (byte[]) appliedRecord.key();
        ByteBuffer appliedKeyBuffer = ByteBuffer.wrap(appliedKey);
        assertEquals(sourceSchemaKeyBytes.length, appliedKey.length, "key byte[] sizes unchanged");
        assertEquals(MAGIC_BYTE, appliedKeyBuffer.get(), "Record key starts with magic byte");

        int transformedRecordKeySchemaId = appliedKeyBuffer.getInt();
        assertNotEquals(sourceKeyId, transformedRecordKeySchemaId, "Schema key ID of transformed record changed");
        assertEquals(targetKeyId, transformedRecordKeySchemaId, "Schema ID of transformed record matches target ID");
        assertArrayEquals(Arrays.copyOfRange(sourceSchemaKeyBytes, AVRO_CONTENT_OFFSET, sourceSchemaKeyBytes.length),
                Arrays.copyOfRange(appliedKeyBuffer.array(), AVRO_CONTENT_OFFSET, appliedKey.length),
                "Avro data not modified");

        // Verify the byte value of the test record was transformed and that the Avro content is identical
        byte[] appliedValue = (byte[]) appliedRecord.value();
        ByteBuffer appliedValueBuffer = ByteBuffer.wrap(appliedValue);
        assertEquals(sourceSchemaValueBytes.length, appliedValue.length, "Value byte[] sizes unchanged");
        assertEquals(MAGIC_BYTE, appliedValueBuffer.get(), "Record value starts with magic byte");

        int transformedRecordValueSchemaId = appliedValueBuffer.getInt();
        assertNotEquals(sourceValueId, transformedRecordValueSchemaId, "schema value ID of transformed changed");
        assertEquals(targetValueId, transformedRecordValueSchemaId, "Schema ID of transformed record matches target ID");
        assertArrayEquals(
                Arrays.copyOfRange(sourceSchemaValueBytes, AVRO_CONTENT_OFFSET, sourceSchemaValueBytes.length),
                Arrays.copyOfRange(appliedValueBuffer.array(), AVRO_CONTENT_OFFSET, appliedValue.length),
                "Avro data not modified");
    }

    @Test
    public void testTombstoneRecord() {
        configure(false);

        SourceRecord testRecord = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                Schema.OPTIONAL_BYTES_SCHEMA,
                null,
                Schema.BYTES_SCHEMA,
                null);

        SourceRecord appliedRecord = assertDoesNotThrow(() -> smt.apply(testRecord));

        assertNotNull(appliedRecord);
        assertEquals(testRecord.valueSchema(), appliedRecord.valueSchema(), "Value schema unchanged");
        assertNull(appliedRecord.value());
    }

    @Test
    public void testEvolvingValueSchemaTransfer() throws RestClientException, IOException {

        configure(true);

        SchemaRegistryClient sourceClient = this.sourceSchemaRegistry.getSchemaRegistryClient();
        SchemaRegistryClient targetClient = this.targetSchemaRegistry.getSchemaRegistryClient();

        // Create new schemas for source registry
        int sourceSchemaId = this.sourceSchemaRegistry.registerSchema(TOPIC, false, new AvroSchema(NAME_SCHEMA));
        assertEquals(1, sourceSchemaId, "Source registry starts at ID=1 when empty");
        int nextSourceSchemaId = this.sourceSchemaRegistry.registerSchema(TOPIC, false, new AvroSchema(NAME_SCHEMA_ALIASED));
        assertEquals(2, nextSourceSchemaId, "Next source schema ID=2");

        List<Integer> sourceSchemaVersions = sourceClient.getAllVersions(SUBJECT_VALUE);
        assertEquals(2, sourceSchemaVersions.size(), "The source registry subject contains the pre-registered schema");

        // Verify that target has no versions for this subject, while a pre-existing schema using a bogus subject value should not
        // affect the returned value
        this.targetSchemaRegistry.registerSchema(UUID.randomUUID().toString(), true, new AvroSchema(INT_SCHEMA));
        List<Integer> targetSchemaVersions = targetClient.getAllVersions(SUBJECT_VALUE);
        assertTrue(targetSchemaVersions.isEmpty(), "The target registry contains no schemas for the subject");

        SourceRecord testRecord1 = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                Schema.OPTIONAL_BYTES_SCHEMA,
                null,
                Schema.BYTES_SCHEMA,
                encodeAvroObject(NAME_SCHEMA, sourceSchemaId, new GenericRecordBuilder(NAME_SCHEMA)
                        .set("first", "fname")
                        .set("last", "lname")
                        .build()));

        SourceRecord testRecord2 = new SourceRecord(
                Collections.singletonMap("partition", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                Schema.OPTIONAL_BYTES_SCHEMA,
                null,
                Schema.BYTES_SCHEMA,
                encodeAvroObject(NAME_SCHEMA_ALIASED, nextSourceSchemaId, new GenericRecordBuilder(NAME_SCHEMA_ALIASED)
                        .set("first", "fname")
                        .set("surname", "lname")
                        .build()));

        assertDoesNotThrow(() -> smt.apply(testRecord1));

        // Verify that the value schema was copied for the first record and that target registry
        // has a valid schema version for the first record
        targetSchemaVersions = targetClient.getAllVersions(SUBJECT_VALUE);
        assertEquals(1, targetSchemaVersions.size(),
                "The target registry has been updated with first schema");

        assertDoesNotThrow(() -> smt.apply(testRecord2));

        // Verify that the value schema was copied for the second record and that target registry
        // has a valid schema version for the second record
        targetSchemaVersions = targetClient.getAllVersions(SUBJECT_VALUE);
        assertEquals(sourceSchemaVersions.size(), targetSchemaVersions.size(),
                "The target registry has been updated with the second schema");
    }

    @Test
    @Disabled("TODO: Find scenario where a backwards compatible change cannot be undone")
    public void testIncompatibleEvolvingValueSchemaTransfer() throws RestClientException, IOException {

        configure(true);

        SchemaRegistryClient sourceClient = this.sourceSchemaRegistry.getSchemaRegistryClient();
        SchemaRegistryClient targetClient = this.targetSchemaRegistry.getSchemaRegistryClient();

        // Create new schemas for source registry
        // TODO: Figure out what these should be, where if order is flipped, destination will not accept
        org.apache.avro.Schema sourceSchema = null;
        org.apache.avro.Schema nextSourceSchema = null;
        int sourceSchemaId = this.sourceSchemaRegistry.registerSchema(TOPIC, false, new AvroSchema(sourceSchema));
        int nextSourceSchemaId = this.sourceSchemaRegistry.registerSchema(TOPIC, false, new AvroSchema(nextSourceSchema));
        assertEquals(1, sourceSchemaId, "Source registry starts at ID=1 when empty");
        assertEquals(2, nextSourceSchemaId, "The next schema ID=2");

        // Verify that target has no versions for this subject, while a pre-existing schema using a bogus subject value should not
        // affect the returned value
        this.targetSchemaRegistry.registerSchema(UUID.randomUUID().toString(), false, new AvroSchema(INT_SCHEMA));
        List<Integer> targetSchemaVersions = targetClient.getAllVersions(SUBJECT_VALUE);
        assertTrue(targetSchemaVersions.isEmpty(), "The target registry contains no schemas for the subject");

        // TODO: Depending on schemas above, then build Avro records for them
        // Ensure second ID is encoded first
        byte[] sourceSchemaValueBytes = encodeAvroObject(nextSourceSchema, nextSourceSchemaId, null);

        SourceRecord testRecord1 = new SourceRecord(
                Collections.singletonMap("partitions", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                null,
                null,
                Schema.OPTIONAL_BYTES_SCHEMA,
                sourceSchemaValueBytes);

        byte[] nextSourceSchemaValueBytes = encodeAvroObject(sourceSchema, sourceSchemaId, null);

        SourceRecord testRecord2 = new SourceRecord(
                Collections.singletonMap("partitions", 0),
                Collections.singletonMap("offset", 0),
                TOPIC,
                null,
                null,
                Schema.OPTIONAL_BYTES_SCHEMA,
                nextSourceSchemaValueBytes);

        assertDoesNotThrow(() -> smt.apply(testRecord1));

        // Verify that the target registry now has a population of one for this subject
        targetSchemaVersions = targetClient.getAllVersions(SUBJECT_VALUE);
        assertEquals(1, targetSchemaVersions.size(),
                "The destination registry has been updated with first schema");

        assertThrows(ConnectException.class, () -> smt.apply(testRecord2));
    }
}
