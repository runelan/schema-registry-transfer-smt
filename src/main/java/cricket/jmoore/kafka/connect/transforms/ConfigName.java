package cricket.jmoore.kafka.connect.transforms;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

interface ConfigName {
    String SRC_SCHEMA_REGISTRY_URL = "source." + KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG;
    String SRC_BASIC_AUTH_CREDENTIALS_SOURCE = "source." + KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
    String SRC_USER_INFO = "source." + KafkaAvroSerializerConfig.USER_INFO_CONFIG;
    String TARGET_SCHEMA_REGISTRY_URL = "target." + KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG;
    String TARGET_BASIC_AUTH_CREDENTIALS_SOURCE = "target." + KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
    String TARGET_USER_INFO = "target." + KafkaAvroSerializerConfig.USER_INFO_CONFIG;
    String SCHEMA_CAPACITY = "schema.capacity";
    String TRANSFER_KEYS = "transfer.message.keys";
    String INCLUDE_HEADERS = "include.message.headers";
}
