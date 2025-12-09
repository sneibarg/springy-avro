package org.springy.springy_avro.automation;

import org.apache.avro.generic.GenericRecord;

@FunctionalInterface
public interface AvroFunction {
    void apply(GenericRecord record, String path, Object value);
}
