package org.springy.springy_avro.automation;

import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import java.util.ArrayList;
import java.util.List;

@Getter
public class AvroNode {

    private final String name;
    private final String path;
    private final Schema schema;
    private final List<AvroNode> children = new ArrayList<>();

    public AvroNode(String name, String path, Schema schema) {
        this.name = name;
        this.path = path;
        this.schema = schema;
    }

    public void addChild(AvroNode child) {
        this.children.add(child);
    }

    public void traverse(GenericRecord record) {
        for (AvroNode child : children) {
            child.traverseForParent(record);
        }
    }

    protected void traverseForParent(GenericRecord parentRecord) {
        Object nested = parentRecord != null ? parentRecord.get(name) : null;
        if (nested instanceof GenericRecord nestedRecord) {
            traverse(nestedRecord);
        } else {
            traverse(null);
        }
    }

    public boolean isLeaf() {
        return false;
    }

    @Override
    public String toString() {
        return "AvroNode{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", type=" + schema.getType() +
                ", children=" + children.size() +
                '}';
    }
}
