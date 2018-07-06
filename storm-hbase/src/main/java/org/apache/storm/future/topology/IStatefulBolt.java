package org.apache.storm.future.topology;


import org.apache.storm.future.state.State;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * A bolt abstraction for supporting stateful computation. The state of the bolt is
 * periodically checkpointed.
 *
 * <p>The framework provides at-least once guarantee for the
 * state updates. The stateful bolts are expected to anchor the tuples while emitting
 * and ack the input tuples once its processed.</p>
 */
public interface IStatefulBolt<T extends State> extends IStatefulComponent<T> {
    /**
     * @see org.apache.storm.task.IBolt#prepare(Map, TopologyContext, OutputCollector)
     */
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
    /**
     * @see org.apache.storm.task.IBolt#execute(Tuple)
     */
    void execute(Tuple input);
    /**
     * @see org.apache.storm.task.IBolt#cleanup()
     */
    void cleanup();
}

