package org.springy.springy_avro.automation;

import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Objects;
import java.util.function.Function;

@Getter
public final class AvroTree {
    private final AvroNode root;

    private AvroTree(AvroNode root) {
        this.root = root;
    }

    public void execute(GenericRecord record) {
        root.traverse(record);
    }

    public static AvroTree fromSchema(Schema schema,
                                      Function<String, AvroFunction> functionResolver) {
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(functionResolver, "functionResolver must not be null");

        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Root schema must be a RECORD: " + schema);
        }

        String rootName = schema.getName();
        AvroNode root = new AvroNode(rootName, rootName, schema);

        for (Schema.Field field : schema.getFields()) {
            AvroNode child = buildNodeForField(field, rootName, functionResolver);
            root.addChild(child);
        }

        return new AvroTree(root);
    }

    private static AvroNode buildNodeForField(Schema.Field field,
                                              String parentPath,
                                              Function<String, AvroFunction> functionResolver) {
        Schema fieldSchema = unwrapNullable(field.schema());
        String path = parentPath + "." + field.name();

        return switch (fieldSchema.getType()) {
            case RECORD -> {
                AvroNode node = new AvroNode(field.name(), path, fieldSchema);
                for (Schema.Field nestedField : fieldSchema.getFields()) {
                    AvroNode nestedNode = buildNodeForField(nestedField, path, functionResolver);
                    node.addChild(nestedNode);
                }
                yield node;
            }
            case ARRAY, MAP, UNION ->
                    new AvroLeaf(field.name(), path, fieldSchema, functionResolver.apply(path));
            default ->
                    new AvroLeaf(field.name(), path, fieldSchema, functionResolver.apply(path));
        };
    }

    private static Schema unwrapNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        return schema.getTypes()
                .stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(schema);
    }
}

