package org.apache.storm.future.state;


import java.io.Serializable;

/**
 * Interface to be implemented for serlializing and de-serializing the
 * state.
 */
public interface Serializer<T> extends Serializable {
    byte[] serialize(T obj);

    T deserialize(byte[] b);
}
