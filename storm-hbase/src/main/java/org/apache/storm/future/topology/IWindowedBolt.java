package org.apache.storm.future.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import org.apache.storm.future.windowing.TimestampExtractor;
import org.apache.storm.future.windowing.TupleWindow;

import java.util.Map;

/**
 * A bolt abstraction for supporting time and count based sliding & tumbling windows.
 */
public interface IWindowedBolt extends IComponent {
    /**
     * This is similar to the {@link org.apache.storm.task.IBolt#prepare(Map, TopologyContext, OutputCollector)} except
     * that while emitting, the tuples are automatically anchored to the tuples in the inputWindow.
     */
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    /**
     * Process the tuple window and optionally emit new tuples based on the tuples in the input window.
     */
    void execute(TupleWindow inputWindow);

    void cleanup();

    /**
     * Return a {@link TimestampExtractor} for extracting timestamps from a
     * tuple for event time based processing, or null for processing time.
     *
     * @return the timestamp extractor
     */
    TimestampExtractor getTimestampExtractor();
}
