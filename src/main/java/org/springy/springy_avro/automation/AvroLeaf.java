package org.springy.springy_avro.automation;

import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

@Getter
public class AvroLeaf extends AvroNode {
    private final AvroFunction function;

    public AvroLeaf(String name, String path, Schema schema, AvroFunction function) {
        super(name, path, schema);
        this.function = function;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    protected void traverseForParent(GenericRecord parentRecord) {
        Object value = null;
        if (parentRecord != null) {
            value = parentRecord.get(getName());
        }
        if (function != null) {
            function.apply(parentRecord, getPath(), value);
        }
    }

    @Override
    public String toString() {
        return "AvroLeaf{" +
                "name='" + getName() + '\'' +
                ", path='" + getPath() + '\'' +
                ", type=" + getSchema().getType() +
                '}';
    }
}

