package org.apache.storm.future.state;

import java.util.Iterator;
import java.util.Map;

/**
 * A state that supports key-value mappings.
 */
public interface KeyValueState<K, V> extends State, Iterable<Map.Entry<K, V>> {
    /**
     * Maps the value with the key
     *
     * @param key   the key
     * @param value the value
     */
    void put(K key, V value);

    /**
     * Returns the value mapped to the key
     *
     * @param key the key
     * @return the value or null if no mapping is found
     */
    V get(K key);

    /**
     * Returns the value mapped to the key or defaultValue if no mapping is found.
     *
     * @param key          the key
     * @param defaultValue the value to return if no mapping is found
     * @return the value or defaultValue if no mapping is found
     */
    V get(K key, V defaultValue);

    /**
     * Deletes the value mapped to the key, if there is any
     *
     * @param key   the key
     */
    V delete(K key);
}
