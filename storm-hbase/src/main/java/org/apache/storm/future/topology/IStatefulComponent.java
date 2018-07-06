package org.apache.storm.future.topology;

import org.apache.storm.future.state.State;

/**
 * <p>
 * Common methods for stateful components in the topology.
 * </p>
 * A stateful component is one that has state (e.g. the result of some computation in a bolt)
 * and wants the framework to manage its state.
 */
public interface IStatefulComponent<T extends State> extends IComponent {
    /**
     * This method is invoked by the framework with the previously
     * saved state of the component. This is invoked after prepare but before
     * the component starts processing tuples.
     *
     * @param state the previously saved state of the component.
     */
    void initState(T state);

    /**
     * This is a hook for the component to perform some actions just before the
     * framework commits its state.
     */
    void preCommit(long txid);

    /**
     * This is a hook for the component to perform some actions just before the
     * framework prepares its state.
     */
    void prePrepare(long txid);

    /**
     * This is a hook for the component to perform some actions just before the
     * framework rolls back the prepared state.
     */
    void preRollback();
}
