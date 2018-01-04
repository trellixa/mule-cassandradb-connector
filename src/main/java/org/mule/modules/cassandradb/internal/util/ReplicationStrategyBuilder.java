/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.internal.util;

import org.apache.commons.lang3.StringUtils;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.modules.cassandradb.api.ReplicationStrategy;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cassandra supported replica placement strategies
 */
public class ReplicationStrategyBuilder {

    //based on http://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html rules
    public static Map<String, Object> buildReplicationStrategy(CreateKeyspaceInput input) {
        LinkedHashMap<String, Object> replicationStrategyProps = new LinkedHashMap<>();

        //set the strategy class only if exists in the input && a valid one is provided
        if (input.getReplicationStrategyClass() != null) {
            //setting 'replication_factor' || 'first_data_center' || 'next data center' is pointless if no class is provided
            addStrategyProps(input, replicationStrategyProps);
        } else {
            return buildDefaultReplicationStrategy();
        }
        return replicationStrategyProps;
    }

    private static void addStrategyProps(CreateKeyspaceInput input, LinkedHashMap<String, Object> replicationStrategyProps) {
        replicationStrategyProps.put(Constants.CLASS, input.getReplicationStrategyClass().name());
        //'replication_factor' required if class is SimpleStrategy; otherwise, not used
        if (input.getReplicationStrategyClass().equals(ReplicationStrategy.SimpleStrategy)) {
            replicationStrategyProps.put(Constants.REPLICATION_FACTOR, input.getReplicationFactor());
        }
        if (input.getFirstDataCenter() != null) {
            if (StringUtils.isNotBlank(input.getFirstDataCenter().getName())) {
                replicationStrategyProps.put(input.getFirstDataCenter().getName(), input.getFirstDataCenter().getValue());
            }
        }
        if (input.getNextDataCenter() != null) {
            if (StringUtils.isNotBlank(input.getNextDataCenter().getName())) {
                replicationStrategyProps.put(input.getNextDataCenter().getName(), input.getNextDataCenter().getValue());
            }
        }
    }

    private static Map<String, Object> buildDefaultReplicationStrategy() {
        LinkedHashMap<String, Object> replicationStrategyMap = new LinkedHashMap<String, Object>();
        replicationStrategyMap.put(Constants.CLASS, ReplicationStrategy.SimpleStrategy.name());
        replicationStrategyMap.put(Constants.REPLICATION_FACTOR, Constants.DEFAULT_REPLICATION_FACTOR);
        return replicationStrategyMap;
    }
}
