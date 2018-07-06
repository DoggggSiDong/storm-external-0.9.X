package org.apache.storm.future.topology;

import backtype.storm.tuple.Tuple;
import org.apache.storm.future.windowing.TimestampExtractor;

/**
 * A {@link TimestampExtractor} that extracts timestamp from a specific field in the tuple.
 */
public final class TupleFieldTimestampExtractor implements TimestampExtractor {
    private final String fieldName;

    private TupleFieldTimestampExtractor(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public long extractTimestamp(Tuple tuple) {
        return tuple.getLongByField(fieldName);
    }

    public static TupleFieldTimestampExtractor of(String fieldName) {
        return new TupleFieldTimestampExtractor(fieldName);
    }

    @Override
    public String toString() {
        return "TupleFieldTimestampExtractor{" +
                "fieldName='" + fieldName + '\'' +
                '}';
    }
}
