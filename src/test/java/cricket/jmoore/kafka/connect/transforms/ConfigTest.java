package cricket.jmoore.kafka.connect.transforms;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

public class ConfigTest {

    private static final String PREFIX = "transforms.AvroSchemaTransfer.";

    private final SchemaRegistryTransfer<SourceRecord> smt = new SchemaRegistryTransfer<>();

    @Test
    public void testConfigInvalidUrl() {
        Map<String, Object> props = getConfigProperties("smt-test-invalid-url.properties").orElseThrow();
        Assertions.assertThrows(RuntimeException.class, () -> {
            smt.configure(props);
        });
    }

    @Test
    public void testConfigEmptyUrl() {
        Map<String, Object> props = getConfigProperties("smt-test-empty-url.properties").orElseThrow();
        Assertions.assertThrows(RuntimeException.class, () -> {
           smt.configure(props);
        });
    }

    @Test
    public void testConfigValid() {
        Map<String, Object> props = getConfigProperties("smt-test-valid.properties").orElseThrow();
        Assertions.assertDoesNotThrow(() -> smt.configure(props));
    }

    // Kafka Connect SMT configuration scoping is hierarchical, where the prefix `transforms.<alias>.` is stripped
    // during instantiation. Thus, when calling `smt.configure()`, the Connect runtime's transform handling is bypassed
    // which will cause the test case to not properly detect the desired property key. To circumvent the problem, we
    // transform each key by manually prepending the desired prefix to keep the test files as realistic as possible
    private Optional<Map<String, Object>> getConfigProperties(final String file) {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(file)) {
            Properties props = new Properties();
            props.load(input);

            Map<String, Object> transformedProps = props.entrySet().stream()
                    .filter(e -> e.getKey().toString().startsWith(PREFIX))
                    .collect(Collectors.toMap(
                            e -> e.getKey().toString().substring(PREFIX.length()),
                            Map.Entry::getValue
                    ));

            Map<String, Object> smtConfig = new HashMap<>();
            transformedProps.forEach((k, v) -> {
                smtConfig.put((String) k, v);
            });
            return Optional.of(smtConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
