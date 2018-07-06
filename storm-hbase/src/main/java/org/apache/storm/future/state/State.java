package org.apache.storm.future.state;

import org.apache.storm.future.topology.IStatefulBolt;

/**
 * The state of the component that is either managed by the framework (e.g in case of {@link IStatefulBolt})
 * or managed by the the individual components themselves.
 */
public interface State {
    /**
     * Invoked by the framework to prepare a transaction for commit. It should be possible
     * to commit the prepared state later.
     * <p>
     * The same txid can be prepared again, but the next txid cannot be prepared
     * when previous one is not yet committed.
     * </p>
     *
     * @param txid the transaction id
     */
    void prepareCommit(long txid);

    /**
     * Commit a previously prepared transaction. It should be possible to retrieve a committed state later.
     *
     * @param txid the transaction id
     */
    void commit(long txid);

    /**
     * Persist the current state. This is used when the component manages the state.
     */
    void commit();

    /**
     * Rollback a prepared transaction to the previously committed state.
     */
    void rollback();
}
