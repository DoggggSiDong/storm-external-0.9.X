package org.apache.storm.future.state;

import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * Used by the {@link StateFactory} to create a new state instances.
 */
public interface StateProvider {
    /**
     * Returns a new state instance. Each state belongs unique namespace which is typically
     * the componentid-task of the task, so that each task can have its own unique state.
     *
     * @param namespace a namespace of the state
     * @param stormConf the storm topology configuration
     * @param context   the {@link TopologyContext}
     * @return a previously saved state if one exists otherwise a newly initialized state.
     */
    State newState(String namespace, Map stormConf, TopologyContext context);
}

