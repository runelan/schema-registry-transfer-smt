package cricket.jmoore.kafka.connect.transforms;

import org.apache.avro.Schema;

import java.util.Objects;

public class SchemaAndId {

    private Integer id;
    private Schema schema;

    public SchemaAndId() {}

    public SchemaAndId(int id, Schema schema) {
        this.id = id;
        this.schema = schema;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Integer getId() {
        return this.id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaAndId schemaAndId = (SchemaAndId) o;
        return Objects.equals(id, schemaAndId.id) &&
                Objects.equals(schema, schemaAndId.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, schema);
    }

    @Override
    public String toString() {
        return "SchemaAndId{" +
                "id=" + id +
                ", schema=" + schema +
                '}';
    }
}