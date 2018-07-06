/*
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

package org.apache.storm.hbase.state;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import backtype.storm.Config;

import org.apache.storm.future.state.DefaultStateSerializer;
import org.apache.storm.future.state.Serializer;
import org.apache.storm.future.state.State;
import org.apache.storm.future.state.StateProvider;
import org.apache.storm.hbase.common.HBaseClient;

import backtype.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides {@link HBaseKeyValueState}.
 */
public class HBaseKeyValueStateProvider implements StateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseKeyValueStateProvider.class);
    public static final String TOPOLOGY_STATE_PROVIDER_CONFIG = "topology.state.provider.config";
    public static final String TOPOLOGY_AUTO_CREDENTIALS = "topology.auto-credentials";
    @Override
    public State newState(String namespace, Map stormConf, TopologyContext context) {
        try {
            return getHBaseKeyValueState(namespace, stormConf, getStateConfig(stormConf));
        } catch (Exception ex) {
            LOG.error("Error loading config from storm conf {}", stormConf);
            throw new RuntimeException(ex);
        }
    }

    StateConfig getStateConfig(Map stormConf) throws Exception {
        StateConfig stateConfig = null;
        String providerConfig = null;
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        if (stormConf.containsKey(TOPOLOGY_STATE_PROVIDER_CONFIG)) {
            providerConfig = (String) stormConf.get(TOPOLOGY_STATE_PROVIDER_CONFIG);
            stateConfig = mapper.readValue(providerConfig, StateConfig.class);
        } else {
            stateConfig = new StateConfig();
        }

        // assertion
        assertMandatoryParameterNotEmpty(stateConfig.hbaseConfigKey, "hbaseConfigKey");
        assertMandatoryParameterNotEmpty(stateConfig.tableName, "tableName");
        assertMandatoryParameterNotEmpty(stateConfig.columnFamily, "columnFamily");

        return stateConfig;
    }

    private HBaseKeyValueState getHBaseKeyValueState(String namespace, Map stormConf, StateConfig config) throws Exception {
        Map<String, Object> conf = getHBaseConfigMap(stormConf, config.hbaseConfigKey);
        final Configuration hbConfig = getHBaseConfigurationInstance(conf);

        //heck for backward compatibility, we need to pass TOPOLOGY_AUTO_CREDENTIALS to hbase conf
        //the conf instance is instance of persistentMap so making a copy.
        Map<String, Object> hbaseConfMap = new HashMap<String, Object>(conf);
        hbaseConfMap.put(TOPOLOGY_AUTO_CREDENTIALS, stormConf.get(TOPOLOGY_AUTO_CREDENTIALS));
        HBaseClient hbaseClient = new HBaseClient(hbaseConfMap, hbConfig, config.tableName);

        return new HBaseKeyValueState(hbaseClient, config.columnFamily, namespace,
                getKeySerializer(config), getValueSerializer(config));
    }

    private Configuration getHBaseConfigurationInstance(Map<String, Object> conf) {
        final Configuration hbConfig = HBaseConfiguration.create();
        for(String key : conf.keySet()) {
            hbConfig.set(key, String.valueOf(conf.get(key)));
        }
        return hbConfig;
    }

    private Map<String, Object> getHBaseConfigMap(Map<String, Object> stormConfMap, String hbaseConfigKey) {
        Map<String, Object> conf = (Map<String, Object>) stormConfMap.get(hbaseConfigKey);
        if(conf == null) {
            throw new IllegalArgumentException("HBase configuration not found using key '" + hbaseConfigKey + "'");
        }

        if(conf.get("hbase.rootdir") == null) {
            LOG.warn("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
        }
        return conf;
    }

    private void assertMandatoryParameterNotEmpty(String paramValue, String fieldName) {
        if (StringUtils.isEmpty(paramValue)) {
            throw new IllegalArgumentException(fieldName + " should be provided.");
        }
    }

    private Serializer getKeySerializer(StateConfig config) throws Exception {
        Serializer serializer = null;
        if (config.keySerializerClass != null) {
            Class<?> klass = (Class<?>) Class.forName(config.keySerializerClass);
            serializer = (Serializer) klass.newInstance();
        } else if (config.keyClass != null) {
            serializer = new DefaultStateSerializer(Collections.singletonList(Class.forName(config.keyClass)));
        } else {
            serializer = new DefaultStateSerializer();
        }
        return serializer;
    }

    private Serializer getValueSerializer(StateConfig config) throws Exception {
        Serializer serializer = null;
        if (config.valueSerializerClass != null) {
            Class<?> klass = (Class<?>) Class.forName(config.valueSerializerClass);
            serializer = (Serializer) klass.newInstance();
        } else if (config.valueClass != null) {
            serializer = new DefaultStateSerializer(Collections.singletonList(Class.forName(config.valueClass)));
        } else {
            serializer = new DefaultStateSerializer();
        }
        return serializer;
    }

    static class StateConfig {
        String keyClass;
        String valueClass;
        String keySerializerClass;
        String valueSerializerClass;
        String hbaseConfigKey;
        String tableName;
        String columnFamily;

        @Override
        public String toString() {
            return "StateConfig{" +
                    "keyClass='" + keyClass + '\'' +
                    ", valueClass='" + valueClass + '\'' +
                    ", keySerializerClass='" + keySerializerClass + '\'' +
                    ", valueSerializerClass='" + valueSerializerClass + '\'' +
                    ", hbaseConfigKey='" + hbaseConfigKey + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", columnFamily='" + columnFamily + '\'' +
                    '}';
        }
    }
}
