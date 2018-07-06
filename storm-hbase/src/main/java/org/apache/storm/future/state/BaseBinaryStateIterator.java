package org.apache.storm.future.state;
import com.google.common.collect.Iterators;
import com.google.common.primitives.UnsignedBytes;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * Base implementation of iterator over {@link KeyValueState} which encoded types of key and value are both binary type.
 */
public abstract class BaseBinaryStateIterator<K, V> extends BaseStateIterator<K, V, byte[], byte[]> {

    /**
     * Constructor.
     *
     * @param pendingPrepareIterator The iterator of pendingPrepare
     * @param pendingCommitIterator The iterator of pendingCommit
     */
    public BaseBinaryStateIterator(Iterator<Map.Entry<byte[], byte[]>> pendingPrepareIterator,
                                   Iterator<Map.Entry<byte[], byte[]>> pendingCommitIterator) {
        super(Iterators.peekingIterator(pendingPrepareIterator), Iterators.peekingIterator(pendingCommitIterator),
                new TreeSet<>(UnsignedBytes.lexicographicalComparator()));
    }

    /**
     * Load some part of state KVs from storage and returns iterator of cached data from storage.
     *
     * @return Iterator of loaded state KVs
     */
    protected abstract Iterator<Map.Entry<byte[], byte[]>> loadChunkFromStateStorage();

    /**
     * Check whether end of data is reached from storage state KVs.
     *
     * @return whether end of data is reached from storage state KVs
     */
    protected abstract boolean isEndOfDataFromStorage();

    /**
     * Decode key to convert byte array to state key type.
     *
     * @param key byte array encoded key
     * @return Decoded value of key
     */
    protected abstract K decodeKey(byte[] key);

    /**
     * Decode value to convert byte array to state value type.
     *
     * @param value byte array encoded value
     * @return Decoded value of value
     */
    protected abstract V decodeValue(byte[] value);

    /**
     * Check whether the value is tombstone (deletion mark) value.
     *
     * @param value the value to check
     * @return true if the value is tombstone, false otherwise
     */
    protected abstract boolean isTombstoneValue(byte[] value);

}