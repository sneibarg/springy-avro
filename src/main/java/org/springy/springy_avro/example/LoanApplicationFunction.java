package org.springy.springy_avro.example;

import org.apache.avro.generic.GenericRecord;
import org.springy.springy_avro.automation.AvroFunction;

public class LoanApplicationFunction implements AvroFunction {
    @Override
    public void apply(GenericRecord record, String path, Object value) {

    }
}
