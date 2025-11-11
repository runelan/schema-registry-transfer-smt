/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
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
    // Wire-format is magic byte + an integer, then data
    private static final short WIRE_FORMAT_PREFIX_LENGTH = 1 + (Integer.SIZE / Byte.SIZE);

    public static final ConfigDef CONFIG_DEF;
    public static final String SCHEMA_CAPACITY_CONFIG_DOC = "The maximum amount of schemas to be stored for each Schema Registry client.";
    public static final Integer SCHEMA_CAPACITY_CONFIG_DEFAULT = 100;

    public static final String SOURCE_PREAMBLE = "For source consumer's schema registry, ";
    public static final String SOURCE_SCHEMA_REGISTRY_CONFIG_DOC = "A list of addresses for the Schema Registry to copy from. The consumer's Schema Registry.";
    public static final String SOURCE_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC = SOURCE_PREAMBLE + KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DOC;
    public static final String SOURCE_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT = KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT;
    public static final String SOURCE_USER_INFO_CONFIG_DOC = SOURCE_PREAMBLE + KafkaAvroSerializerConfig.SCHEMA_REGISTRY_USER_INFO_DOC;
    public static final String SOURCE_USER_INFO_CONFIG_DEFAULT = KafkaAvroSerializerConfig.SCHEMA_REGISTRY_USER_INFO_DEFAULT;

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

    // Caches from the source registry to the destination registry
    private Cache<Integer, SchemaAndId> schemaCache;

    // Regex explanation
    // `^https?:\\/\\/`: `http://` or `https://`
    // `(?:[A-Za-z0-9._%+-]+(?::[^@]*)?@)?`: optional username and optional `:password`, ending with `@`
    // `[A-Za-z0-9.-]+`: host or domain
    // `(?::\\d+)?`: optional port
    // `(?:\\/[^\\s]*)?`: optional path segment
    private static final Pattern URL_PATTERN = Pattern.compile("^https?:\\/\\/(?:[A-Za-z0-9._%+-]+(?::[^@]*)?@)?[A-Za-z0-9.-]+(?::\\d+)?(?:\\/[^\\s]*)?$");

    public SchemaRegistryTransfer() {}

    static {
        CONFIG_DEF = (new ConfigDef())
                .define(
                        ConfigName.SOURCE_SCHEMA_REGISTRY_URL,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        (name, value) -> {
                            if(value != null && !URL_PATTERN.matcher((String) value).matches()) {
                              throw new ConfigException(name, value);
                            }
                        },
                        ConfigDef.Importance.HIGH,
                        SOURCE_SCHEMA_REGISTRY_CONFIG_DOC)
                .define(
                        ConfigName.TARGET_SCHEMA_REGISTRY_URL,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        (name, value) -> {
                            if(value != null && !URL_PATTERN.matcher((String) value).matches()) {
                                throw new ConfigException(name, value);
                            }
                        },
                        ConfigDef.Importance.HIGH,
                        TARGET_SCHEMA_REGISTRY_CONFIG_DOC)
                .define(
                        ConfigName.SOURCE_BASIC_AUTH_CREDENTIALS_SOURCE,
                        ConfigDef.Type.STRING,
                        SOURCE_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SOURCE_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC)
                .define(
                        ConfigName.SOURCE_USER_INFO,
                        ConfigDef.Type.PASSWORD,
                        SOURCE_USER_INFO_CONFIG_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        SOURCE_USER_INFO_CONFIG_DOC)
                .define(
                        ConfigName.TARGET_BASIC_AUTH_CREDENTIALS_SOURCE,
                        ConfigDef.Type.STRING,
                        TARGET_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        TARGET_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC)
                .define(
                        ConfigName.TARGET_USER_INFO,
                        ConfigDef.Type.PASSWORD,
                        TARGET_USER_INFO_CONFIG_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        TARGET_USER_INFO_CONFIG_DOC)
                .define(
                        ConfigName.SCHEMA_CAPACITY,
                        ConfigDef.Type.INT,
                        SCHEMA_CAPACITY_CONFIG_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SCHEMA_CAPACITY_CONFIG_DOC)
                .define(
                        ConfigName.TRANSFER_KEYS,
                        ConfigDef.Type.BOOLEAN,
                        TRANSFER_KEYS_CONFIG_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        TRANSFER_KEYS_CONFIG_DOC)
                .define(
                        ConfigName.INCLUDE_HEADERS,
                        ConfigDef.Type.BOOLEAN,
                        INCLUDE_HEADERS_CONFIG_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        INCLUDE_HEADERS_CONFIG_DOC)
        ;
        // TODO: Other properties might be useful, e.g. the Subject Naming Strategies
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> props) {

        StringBuilder configInfo = new StringBuilder();
        configInfo.append("##########\n# Schema Registry Transfer SMT config\n##########\n");
        props.forEach((k, v) -> {
            configInfo.append(k).append("=").append(v).append("\n");
        });
        configInfo.append("\n\n");

        log.info(configInfo.toString());

        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        final Map<String, String> sourceProps = new HashMap<>();
        sourceProps.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
            "SOURCE_" + config.getString(ConfigName.SOURCE_BASIC_AUTH_CREDENTIALS_SOURCE));
        sourceProps.put(SchemaRegistryClientConfig.USER_INFO_CONFIG,
            config.getPassword(ConfigName.SOURCE_USER_INFO)
                .value());

        final Map<String, String> targetProps = new HashMap<>();
        targetProps.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
            "TARGET_" + config.getString(ConfigName.TARGET_BASIC_AUTH_CREDENTIALS_SOURCE));
        targetProps.put(SchemaRegistryClientConfig.USER_INFO_CONFIG,
            config.getPassword(ConfigName.TARGET_USER_INFO)
                .value());

        Integer schemaCapacity = config.getInt(ConfigName.SCHEMA_CAPACITY);

        this.schemaCache = new SynchronizedCache<>(new LRUCache<>(schemaCapacity));
        this.sourceSchemaRegistryClient = new CachedSchemaRegistryClient(config.getString(ConfigName.SOURCE_SCHEMA_REGISTRY_URL), schemaCapacity, sourceProps);
        this.targetSchemaRegistryClient = new CachedSchemaRegistryClient(config.getString(ConfigName.TARGET_SCHEMA_REGISTRY_URL), schemaCapacity, targetProps);

        this.transferKeys = config.getBoolean(ConfigName.TRANSFER_KEYS);
        this.includeHeaders = config.getBoolean(ConfigName.INCLUDE_HEADERS);

        // TODO: Make the Strategy configurable, may be different for source and target
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
                throw new ConnectException("Transform failed, record key does not have a byte[] schema");
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
                        -> new ConnectException("Transform failed, unable to update record schema ID (isKey=true)"));
                b.putInt(1, id);
                updatedKey = b.array();
            }
        } else {
            log.trace("Skipping record key translation, {} has been set to false: keys will be passed as-is", ConfigName.TRANSFER_KEYS);
        }

        if (!(ConnectSchemaUtil.isBytesSchema(valueSchema) || value instanceof byte[])) {
            throw new ConnectException("Transform failed, record value does not have a byte[] schema");
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
                    -> new ConnectException("Transform failed, unable to update record schema ID (isKey=false)"));
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
                log.error("Unable to fetch source schema for ID {}: {}", sourceSchemaId, e.getMessage());
                throw new ConnectException(e);
            }

            try {
                ParsedSchema parsedSchema = new AvroSchema(schemaAndTargetId.getSchema());
                String subjectName = subjectNameStrategy.subjectName(topic, isKey, parsedSchema);
                int schemaId = targetSchemaRegistryClient.register(subjectName, parsedSchema);
                schemaAndTargetId.setId(schemaId);
                schemaCache.put(sourceSchemaId, schemaAndTargetId);
            } catch (IOException | RestClientException e) {
                log.error("Unable to register source schema ID {} to target registry: {}", sourceSchemaId, e.getMessage());
                return Optional.empty();
            }
        } else {
            log.debug("Schema id {} has been seen before, using cached mapping", schemaAndTargetId);
        }

        return Optional.ofNullable(schemaAndTargetId.getId());
    }

    @Override
    public void close() {
        try {
            this.sourceSchemaRegistryClient.close();
            this.targetSchemaRegistryClient.close();
        } catch (IOException e) {
            log.error("Error occurred while terminating registry client: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
