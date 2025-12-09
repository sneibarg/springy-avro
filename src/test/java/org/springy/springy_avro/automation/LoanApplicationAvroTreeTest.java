package org.springy.springy_avro.automation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LoanApplicationAvroTreeTest {

    @Test
    void executesLeafFunctionsForAutoLoan() throws IOException {
        Schema schema = loadLoanApplicationSchema();
        Map<String, Object> valuesByPath = new HashMap<>();
        Function<String, AvroFunction> resolver = path ->
                (record, p, value) -> valuesByPath.put(p, value);

        AvroTree tree = AvroTree.fromSchema(schema, resolver);
        GenericRecord root = buildAutoLoanRecord(schema);

        tree.execute(root);

        assertFalse(valuesByPath.isEmpty(), "No leaf functions were invoked");
        assertEquals("APP-123", toStringSafe(valuesByPath.get("LoanApplication.applicationId")));
        assertEquals("AUTO", toStringSafe(valuesByPath.get("LoanApplication.loanType")));
        assertEquals("Jane", toStringSafe(valuesByPath.get("LoanApplication.applicant.firstName")));
        assertEquals("Doe", toStringSafe(valuesByPath.get("LoanApplication.applicant.lastName")));
        assertEquals("123 Main St", toStringSafe(valuesByPath.get("LoanApplication.applicant.currentAddress.line1")));
        assertEquals("Springfield", toStringSafe(valuesByPath.get("LoanApplication.applicant.currentAddress.city")));
        assertEquals("1FTFW1E89AKD12345", toStringSafe(valuesByPath.get("LoanApplication.details.vehicleVin")));
        assertEquals("Ford", toStringSafe(valuesByPath.get("LoanApplication.details.make")));
        assertEquals("F-150", toStringSafe(valuesByPath.get("LoanApplication.details.model")));
        assertEquals("NEW", toStringSafe(valuesByPath.get("LoanApplication.details.newOrUsed")));
    }

    private Schema loadLoanApplicationSchema() throws IOException {
        String resourcePath = "avro/LoanApplication.avsc";

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream in = cl.getResourceAsStream(resourcePath);
        if (in == null) {
            in = LoanApplicationAvroTreeTest.class.getClassLoader()
                    .getResourceAsStream(resourcePath);
        }

        assertNotNull(in, "Could not find resource: " + resourcePath);

        final InputStream input = in; // required for try-with-resources

        try (input) {
            return new Schema.Parser().parse(input);
        }
    }

    private GenericRecord buildAutoLoanRecord(Schema loanSchema) {
        GenericRecord root = new GenericData.Record(loanSchema);

        root.put("applicationId", "APP-123");
        root.put("createdAt", 1_700_000_000_000L); // arbitrary timestamp
        root.put("requestedAmount", 25000.0);
        root.put("termMonths", 60);

        Schema loanTypeSchema = loanSchema.getField("loanType").schema();
        root.put("loanType", new GenericData.EnumSymbol(loanTypeSchema, "AUTO"));

        Schema applicantSchema = loanSchema.getField("applicant").schema();
        GenericRecord applicant = new GenericData.Record(applicantSchema);
        applicant.put("applicantId", "APPL-1");
        applicant.put("firstName", "Jane");
        applicant.put("lastName", "Doe");
        applicant.put("dateOfBirth", 15000); // days since epoch (arbitrary)
        applicant.put("email", "jane.doe@example.com");
        applicant.put("phoneNumber", "555-1234");
        applicant.put("annualIncome", 120000.0);

        Schema employmentStatusSchema = applicantSchema.getField("employmentStatus").schema();
        applicant.put("employmentStatus",
                new GenericData.EnumSymbol(employmentStatusSchema, "EMPLOYED"));

        applicant.put("creditScore", 750);

        // currentAddress: union [null, Address]
        Schema currentAddressUnion = applicantSchema.getField("currentAddress").schema();
        Schema addressSchema = unwrapNullable(currentAddressUnion);

        GenericRecord address = new GenericData.Record(addressSchema);
        address.put("line1", "123 Main St");
        address.put("line2", null);
        address.put("city", "Springfield");
        address.put("state", "OH");
        address.put("postalCode", "43001");
        address.put("country", "US");

        applicant.put("currentAddress", address);

        root.put("applicant", applicant);

        Schema detailsUnion = loanSchema.getField("details").schema();
        Schema autoDetailsSchema = detailsUnion.getTypes().stream()
                .filter(s -> s.getType() == Schema.Type.RECORD && "AutoLoanDetails".equals(s.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("AutoLoanDetails schema not found in union"));

        GenericRecord autoDetails = new GenericData.Record(autoDetailsSchema);
        autoDetails.put("vehicleVin", "1FTFW1E89AKD12345");
        autoDetails.put("make", "Ford");
        autoDetails.put("model", "F-150");
        autoDetails.put("modelYear", 2023);
        autoDetails.put("purchasePrice", 45000.0);
        autoDetails.put("downPayment", 5000.0);
        autoDetails.put("dealerName", "Awesome Ford Dealer");

        Schema vehicleConditionSchema = autoDetailsSchema.getField("newOrUsed").schema();
        autoDetails.put("newOrUsed", new GenericData.EnumSymbol(vehicleConditionSchema, "NEW"));

        root.put("details", autoDetails);

        return root;
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

    private static String toStringSafe(Object value) {
        return value == null ? null : value.toString();
    }
}
