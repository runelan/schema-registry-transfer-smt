/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

@SuppressWarnings("unused")
public class SchemaRegistryTransfer<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Inspect the Confluent KafkaAvroSerializer's wire-format header to copy schemas from one Schema Registry to another.";
    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryTransfer.class);

    private static final byte MAGIC_BYTE = (byte) 0x0;
    // wire-format is magic byte + an integer, then data
    private static final short WIRE_FORMAT_PREFIX_LENGTH = 1 + (Integer.SIZE / Byte.SIZE);

    public static final ConfigDef CONFIG_DEF;
    public static final String SCHEMA_CAPACITY_CONFIG_DOC = "The maximum amount of schemas to be stored for each Schema Registry client.";
    public static final Integer SCHEMA_CAPACITY_CONFIG_DEFAULT = 100;

    public static final String SRC_PREAMBLE = "For source consumer's schema registry, ";
    public static final String SRC_SCHEMA_REGISTRY_CONFIG_DOC = "A list of addresses for the Schema Registry to copy from. The consumer's Schema Registry.";
    public static final String SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC = SRC_PREAMBLE + KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DOC;
    public static final String SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT = KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT;
    public static final String SRC_USER_INFO_CONFIG_DOC = SRC_PREAMBLE + KafkaAvroSerializerConfig.SCHEMA_REGISTRY_USER_INFO_DOC;
    public static final String SRC_USER_INFO_CONFIG_DEFAULT = KafkaAvroSerializerConfig.SCHEMA_REGISTRY_USER_INFO_DEFAULT;

    public static final String TARGET_PREAMBLE = "For target producer's schema registry, ";
    public static final String TARGET_SCHEMA_REGISTRY_CONFIG_DOC = "A list of addresses for the Schema Registry to copy to. The producer's Schema Registry.";
    public static final String TARGET_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC = TARGET_PREAMBLE + KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DOC;
    public static final String TARGET_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT = KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT;
    public static final String TARGET_USER_INFO_CONFIG_DOC = TARGET_PREAMBLE + KafkaAvroSerializerConfig.SCHEMA_REGISTRY_USER_INFO_DOC;
    public static final String TARGET_USER_INFO_CONFIG_DEFAULT = KafkaAvroSerializerConfig.SCHEMA_REGISTRY_USER_INFO_DEFAULT;

    public static final String TRANSFER_KEYS_CONFIG_DOC = "Whether or not to copy message key schemas between registries.";
    public static final Boolean TRANSFER_KEYS_CONFIG_DEFAULT = true;
    public static final String INCLUDE_HEADERS_CONFIG_DOC = "Whether or not to preserve the Kafka Connect Record headers.";
    public static final Boolean INCLUDE_HEADERS_CONFIG_DEFAULT = true;

    private CachedSchemaRegistryClient sourceSchemaRegistryClient;
    private CachedSchemaRegistryClient targetSchemaRegistryClient;
    private SubjectNameStrategy subjectNameStrategy;
    private boolean transferKeys, includeHeaders;

    // caches from the source registry to the destination registry
    private Cache<Integer, SchemaAndId> schemaCache;

    public SchemaRegistryTransfer() {}

    static {
        CONFIG_DEF = (new ConfigDef())
                .define(ConfigName.SRC_SCHEMA_REGISTRY_URL, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, SRC_SCHEMA_REGISTRY_CONFIG_DOC)
                .define(ConfigName.TARGET_SCHEMA_REGISTRY_URL, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, TARGET_SCHEMA_REGISTRY_CONFIG_DOC)
                .define(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE, ConfigDef.Type.STRING, SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC)
                .define(ConfigName.SRC_USER_INFO, ConfigDef.Type.PASSWORD, SRC_USER_INFO_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, SRC_USER_INFO_CONFIG_DOC)
                .define(ConfigName.TARGET_BASIC_AUTH_CREDENTIALS_SOURCE, ConfigDef.Type.STRING, TARGET_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, TARGET_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC)
                .define(ConfigName.TARGET_USER_INFO, ConfigDef.Type.PASSWORD, TARGET_USER_INFO_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, TARGET_USER_INFO_CONFIG_DOC)
                .define(ConfigName.SCHEMA_CAPACITY, ConfigDef.Type.INT, SCHEMA_CAPACITY_CONFIG_DEFAULT, ConfigDef.Importance.LOW, SCHEMA_CAPACITY_CONFIG_DOC)
                .define(ConfigName.TRANSFER_KEYS, ConfigDef.Type.BOOLEAN, TRANSFER_KEYS_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, TRANSFER_KEYS_CONFIG_DOC)
                .define(ConfigName.INCLUDE_HEADERS, ConfigDef.Type.BOOLEAN, INCLUDE_HEADERS_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, INCLUDE_HEADERS_CONFIG_DOC)
        ;
        // TODO: Other properties might be useful, e.g. the Subject Naming Strategies
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        List<String> sourceUrls = config.getList(ConfigName.SRC_SCHEMA_REGISTRY_URL);
        final Map<String, String> sourceProps = new HashMap<>();
        sourceProps.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
            "SRC_" + config.getString(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE));
        sourceProps.put(SchemaRegistryClientConfig.USER_INFO_CONFIG,
            config.getPassword(ConfigName.SRC_USER_INFO)
                .value());

        List<String> targetUrls = config.getList(ConfigName.TARGET_SCHEMA_REGISTRY_URL);
        final Map<String, String> targetProps = new HashMap<>();
        targetProps.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
            "TARGET_" + config.getString(ConfigName.TARGET_BASIC_AUTH_CREDENTIALS_SOURCE));
        targetProps.put(SchemaRegistryClientConfig.USER_INFO_CONFIG,
            config.getPassword(ConfigName.TARGET_USER_INFO)
                .value());

        Integer schemaCapacity = config.getInt(ConfigName.SCHEMA_CAPACITY);

        this.schemaCache = new SynchronizedCache<>(new LRUCache<>(schemaCapacity));
        this.sourceSchemaRegistryClient = new CachedSchemaRegistryClient(sourceUrls, schemaCapacity, sourceProps);
        this.targetSchemaRegistryClient = new CachedSchemaRegistryClient(targetUrls, schemaCapacity, targetProps);

        this.transferKeys = config.getBoolean(ConfigName.TRANSFER_KEYS);
        this.includeHeaders = config.getBoolean(ConfigName.INCLUDE_HEADERS);

        // TODO: Make the Strategy configurable, may be different for src and target
        // Strategy for the -key and -value subjects
        this.subjectNameStrategy = new TopicNameStrategy();
    }

    @Override
    public R apply(R r) {
        final String topic = r.topic();

        // Transcribe the key's schema ID and
        // the value's schema ID
        final Object key = r.key();
        final Schema keySchema = r.keySchema();
        final Object value = r.value();
        final Schema valueSchema = r.valueSchema();

        Object updatedKey = key;
        Object updatedValue = value;

        if (transferKeys) {
            if (!(ConnectSchemaUtil.isBytesSchema(keySchema) || key instanceof byte[])) {
                throw new ConnectException("Transform failed. Record key does not have a byte[] schema");
            }
            if (key == null) {
                log.trace("Passing through null record key.");
            } else {
                byte[] keyBytes = (byte[]) key;
                if (keyBytes.length <= 5) {
                    throw new SerializationException("Unexpected byte[] length " + keyBytes.length + " for Avro record key");
                }
                ByteBuffer b = ByteBuffer.wrap(keyBytes);
                int id = copySchema(b, topic, true).orElseThrow(()
                        -> new ConnectException("Transform failed. Unable to update record schema ID. (isKey=true)"));
                b.putInt(1, id);
                updatedKey = b.array();
            }
        } else {
            log.trace("Skipping record key translation. {} has been set to false. Keys will be passed as-is.", ConfigName.TRANSFER_KEYS);
        }

        if (!(ConnectSchemaUtil.isBytesSchema(valueSchema) || value instanceof byte[])) {
            throw new ConnectException("Transform failed. Record value does not have a byte[] schema.");
        }
        if (value == null) {
            log.trace("Passing through null record value");
        } else {
            byte[] valueBytes = (byte[]) value;
            if (valueBytes.length <= 5) {
                throw new SerializationException("Unexpected byte[] length " + valueBytes.length + " for Avro record value.");
            }
            ByteBuffer b = ByteBuffer.wrap(valueBytes);
            int id = copySchema(b, topic, false).orElseThrow(()
                    -> new ConnectException("Transform failed. Unable to update record schema ID. (isKey=false)"));
            b.putInt(1, id);
            updatedValue = b.array();
        }

        return includeHeaders ?
            r.newRecord(topic, r.kafkaPartition(), keySchema, updatedKey, valueSchema, updatedValue, r.timestamp(), r.headers())
            : r.newRecord(topic, r.kafkaPartition(), keySchema, updatedKey, valueSchema, updatedValue, r.timestamp());
    }

    protected Optional<Integer> copySchema(ByteBuffer buffer, String topic, boolean isKey) {
        if (buffer.get() != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte");
        }

        int sourceSchemaId = buffer.getInt();
        SchemaAndId schemaAndTargetId = schemaCache.get(sourceSchemaId);

        if (schemaAndTargetId == null) {
            log.debug("Schema id {} has not been seen before", sourceSchemaId);
            schemaAndTargetId = new SchemaAndId();

            try {
                ParsedSchema fetchedSchema = sourceSchemaRegistryClient.getSchemaById(sourceSchemaId);
                schemaAndTargetId.setSchema((org.apache.avro.Schema) fetchedSchema.rawSchema());
            } catch (IOException | RestClientException e) {
                log.error("Unable to fetch source schema for ID {}", sourceSchemaId, e);
                throw new ConnectException(e);
            }

            try {
                ParsedSchema parsedSchema = new AvroSchema(schemaAndTargetId.getSchema());
                String subjectName = subjectNameStrategy.subjectName(topic, isKey, parsedSchema);
                int schemaId = targetSchemaRegistryClient.register(subjectName, parsedSchema);
                schemaAndTargetId.setId(schemaId);
                schemaCache.put(sourceSchemaId, schemaAndTargetId);
            } catch (IOException | RestClientException e) {
                log.error("Unable to register source schema ID {} to target registry", sourceSchemaId);
                return Optional.empty();
            }
        } else {
            log.debug("Schema id {} has been seen before. Using cached mapping.", schemaAndTargetId);
        }

        return Optional.ofNullable(schemaAndTargetId.getId());
    }

    @Override
    public void close() {
        this.sourceSchemaRegistryClient = null;
        this.targetSchemaRegistryClient = null;
    }
}
