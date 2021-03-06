/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.future.windowing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Eviction policy that evicts events based on time duration.
 */
public class TimeEvictionPolicy<T> implements EvictionPolicy<T> {
    private static final Logger LOG = LoggerFactory.getLogger(TimeEvictionPolicy.class);

    private final int windowLength;
    /**
     * The reference time in millis for window calculations and
     * expiring events. If not set it will default to System.currentTimeMillis()
     */
    protected Long referenceTime;
    protected EvictionContext evictionContext;
    private long delta;

    /**
     * Constructs a TimeEvictionPolicy that evicts events older
     * than the given window length in millis
     *
     * @param windowLength the duration in milliseconds
     */
    public TimeEvictionPolicy(int windowLength) {
        this.windowLength = windowLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Action evict(Event<T> event) {      
        long now = referenceTime == null ? System.currentTimeMillis() : referenceTime;
        long diff = now - event.getTimestamp();
        if (diff >= (windowLength + delta)) {
            return Action.EXPIRE;
        } else if (diff < 0) { // do not process events beyond current ts
            return Action.KEEP;
        }
        return Action.PROCESS;
    }

    @Override
    public void track(Event<T> event) {
        // NOOP
    }

    @Override
    public void setContext(EvictionContext context) {
        referenceTime = context.getReferenceTime();
        EvictionContext prevContext = evictionContext;
        evictionContext = context;
        // compute window length adjustment (delta) to account for time drift
        if (context.getSlidingInterval() != null) {
            if (prevContext == null) {
                delta = Integer.MAX_VALUE; // consider all events for the initial window
            } else {
                delta = context.getReferenceTime() - prevContext.getReferenceTime() - context.getSlidingInterval();
                if (Math.abs(delta) > 100) {
                    LOG.warn("Possible clock drift or long running computation in window; " +
                                    "Previous eviction time: {}, current eviction time: {}",
                            prevContext.getReferenceTime(),
                            context.getReferenceTime());
                }
            }
        }
    }

    @Override
    public String toString() {
        return "TimeEvictionPolicy{" +
                "windowLength=" + windowLength +
                ", referenceTime=" + referenceTime +
                '}';
    }
}
