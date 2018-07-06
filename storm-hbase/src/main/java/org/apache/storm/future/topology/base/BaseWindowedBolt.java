package org.apache.storm.future.topology.base;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.future.topology.IWindowedBolt;
import org.apache.storm.future.topology.TupleFieldTimestampExtractor;
import org.apache.storm.future.windowing.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class BaseWindowedBolt implements IWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaseWindowedBolt.class);

    protected final transient Map<String, Object> windowConfiguration;
    protected TimestampExtractor timestampExtractor;
    public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT = "topology.bolts.window.length.count";
    public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS = "topology.bolts.window.length.duration.ms";
    public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT = "topology.bolts.window.sliding.interval.count";
    public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS = "topology.bolts.window.sliding.interval.duration.ms";
    public static final String TOPOLOGY_BOLTS_LATE_TUPLE_STREAM = "topology.bolts.late.tuple.stream";
    public static final String TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS = "topology.bolts.tuple.timestamp.max.lag.ms";
    public static final String TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS = "topology.bolts.watermark.event.interval.ms";


    /**
     * Holds a count value for count based windows and sliding intervals.
     */
    public static class Count {
        public final int value;

        public Count(int value) {
            this.value = value;
        }

        /**
         * Returns a {@link Count} of given value.
         *
         * @param value the count value
         * @return the Count
         */
        public static Count of(int value) {
            return new Count(value);
        }

        @Override
        public String toString() {
            return "Count{" +
                    "value=" + value +
                    '}';
        }
    }

    /**
     * Holds a Time duration for time based windows and sliding intervals.
     */
    public static class Duration {
        public final int value;

        public Duration(int value, TimeUnit timeUnit) {
            this.value = (int) timeUnit.toMillis(value);
        }

        /**
         * Returns a {@link Duration} corresponding to the the given value in milli seconds.
         *
         * @param milliseconds the duration in milliseconds
         * @return the Duration
         */
        public static Duration of(int milliseconds) {
            return new Duration(milliseconds, TimeUnit.MILLISECONDS);
        }

        /**
         * Returns a {@link Duration} corresponding to the the given value in days.
         *
         * @param days the number of days
         * @return the Duration
         */
        public static Duration days(int days) {
            return new Duration(days, TimeUnit.DAYS);
        }

        /**
         * Returns a {@link Duration} corresponding to the the given value in hours.
         *
         * @param hours the number of hours
         * @return the Duration
         */
        public static Duration hours(int hours) {
            return new Duration(hours, TimeUnit.HOURS);
        }

        /**
         * Returns a {@link Duration} corresponding to the the given value in minutes.
         *
         * @param minutes the number of minutes
         * @return the Duration
         */
        public static Duration minutes(int minutes) {
            return new Duration(minutes, TimeUnit.MINUTES);
        }

        /**
         * Returns a {@link Duration} corresponding to the the given value in seconds.
         *
         * @param seconds the number of seconds
         * @return the Duration
         */
        public static Duration seconds(int seconds) {
            return new Duration(seconds, TimeUnit.SECONDS);
        }

        @Override
        public String toString() {
            return "Duration{" +
                    "value=" + value +
                    '}';
        }
    }

    protected BaseWindowedBolt() {
        windowConfiguration = new HashMap<>();
    }

    private BaseWindowedBolt withWindowLength(Count count) {
        if (count.value <= 0) {
            throw new IllegalArgumentException("Window length must be positive [" + count + "]");
        }
        windowConfiguration.put(TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, count.value);
        return this;
    }

    private BaseWindowedBolt withWindowLength(Duration duration) {
        if (duration.value <= 0) {
            throw new IllegalArgumentException("Window length must be positive [" + duration + "]");
        }

        windowConfiguration.put(TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, duration.value);
        return this;
    }

    private BaseWindowedBolt withSlidingInterval(Count count) {
        if (count.value <= 0) {
            throw new IllegalArgumentException("Sliding interval must be positive [" + count + "]");
        }
        windowConfiguration.put(TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, count.value);
        return this;
    }

    private BaseWindowedBolt withSlidingInterval(Duration duration) {
        if (duration.value <= 0) {
            throw new IllegalArgumentException("Sliding interval must be positive [" + duration + "]");
        }
        windowConfiguration.put(TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, duration.value);
        return this;
    }

    /**
     * Tuple count based sliding window configuration.
     *
     * @param windowLength    the number of tuples in the window
     * @param slidingInterval the number of tuples after which the window slides
     */
    public BaseWindowedBolt withWindow(Count windowLength, Count slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }

    /**
     * Tuple count and time duration based sliding window configuration.
     *
     * @param windowLength    the number of tuples in the window
     * @param slidingInterval the time duration after which the window slides
     */
    public BaseWindowedBolt withWindow(Count windowLength, Duration slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }

    /**
     * Time duration and count based sliding window configuration.
     *
     * @param windowLength    the time duration of the window
     * @param slidingInterval the number of tuples after which the window slides
     */
    public BaseWindowedBolt withWindow(Duration windowLength, Count slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }

    /**
     * Time duration based sliding window configuration.
     *
     * @param windowLength    the time duration of the window
     * @param slidingInterval the time duration after which the window slides
     */
    public BaseWindowedBolt withWindow(Duration windowLength, Duration slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }

    /**
     * A tuple count based window that slides with every incoming tuple.
     *
     * @param windowLength the number of tuples in the window
     */
    public BaseWindowedBolt withWindow(Count windowLength) {
        return withWindowLength(windowLength).withSlidingInterval(new Count(1));
    }

    /**
     * A time duration based window that slides with every incoming tuple.
     *
     * @param windowLength the time duration of the window
     */
    public BaseWindowedBolt withWindow(Duration windowLength) {
        return withWindowLength(windowLength).withSlidingInterval(new Count(1));
    }

    /**
     * A count based tumbling window.
     *
     * @param count the number of tuples after which the window tumbles
     */
    public BaseWindowedBolt withTumblingWindow(Count count) {
        return withWindowLength(count).withSlidingInterval(count);
    }

    /**
     * A time duration based tumbling window.
     *
     * @param duration the time duration after which the window tumbles
     */
    public BaseWindowedBolt withTumblingWindow(Duration duration) {
        return withWindowLength(duration).withSlidingInterval(duration);
    }

    /**
     * Specify a field in the tuple that represents the timestamp as a long value. If this
     * field is not present in the incoming tuple, an {@link IllegalArgumentException} will be thrown.
     *
     * @param fieldName the name of the field that contains the timestamp
     */
    public BaseWindowedBolt withTimestampField(String fieldName) {
        return withTimestampExtractor(TupleFieldTimestampExtractor.of(fieldName));
    }

    /**
     * Specify the timestamp extractor implementation.
     *
     * @param timestampExtractor the {@link TimestampExtractor} implementation
     */
    public BaseWindowedBolt withTimestampExtractor(TimestampExtractor timestampExtractor) {
        if (this.timestampExtractor != null) {
            throw new IllegalArgumentException("Window is already configured with a timestamp extractor: " + timestampExtractor);
        }
        this.timestampExtractor = timestampExtractor;
        return this;
    }

    @Override
    public TimestampExtractor getTimestampExtractor() {
        return timestampExtractor;
    }

    /**
     * Specify a stream id on which late tuples are going to be emitted. They are going to be accessible via the
     * {@link org.apache.storm.topology.WindowedBoltExecutor#LATE_TUPLE_FIELD} field.
     * It must be defined on a per-component basis, and in conjunction with the
     * {@link BaseWindowedBolt#withTimestampField}, otherwise {@link IllegalArgumentException} will be thrown.
     *
     * @param streamId the name of the stream used to emit late tuples on
     */
    public BaseWindowedBolt withLateTupleStream(String streamId) {
        windowConfiguration.put(TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, streamId);
        return this;
    }


    /**
     * Specify the maximum time lag of the tuple timestamp in milliseconds. It means that the tuple timestamps
     * cannot be out of order by more than this amount.
     *
     * @param duration the max lag duration
     */
    public BaseWindowedBolt withLag(Duration duration) {
        windowConfiguration.put(TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, duration.value);
        return this;
    }

    /**
     * Specify the watermark event generation interval. For tuple based timestamps, watermark events
     * are used to track the progress of time
     *
     * @param interval the interval at which watermark events are generated
     */
    public BaseWindowedBolt withWatermarkInterval(Duration interval) {
        windowConfiguration.put(TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, interval.value);
        return this;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // NOOP
    }

    @Override
    public void cleanup() {
        // NOOP
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // NOOP
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return windowConfiguration;
    }
}
