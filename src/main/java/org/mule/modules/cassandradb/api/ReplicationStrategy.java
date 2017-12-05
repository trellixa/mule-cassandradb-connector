/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.api;

import org.apache.commons.lang3.StringUtils;
import org.mule.modules.cassandradb.internal.util.Constants;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cassandra supported replica placement strategies
 */
public enum ReplicationStrategy {

    /**
     * <p>SimpleStrategy merely places the first replica at the node whose
     * token is closest to the key (as determined by the Partitioner), and
     * additional replicas on subsequent nodes along the ring in increasing
     * Token order.</p>
     * 
     * <p>Supports a single strategy option 'replication_factor' that
     * specifies the replication factor for the cluster.</p>
     */
    SIMPLE("SimpleStrategy"),
    /**
     * <p>With NetworkTopologyStrategy, for each datacenter, you can specify
     * how many replicas you want on a per-keyspace basis. Replicas are
     * placed on different racks within each DC, if possible.</p>
     * 
     * <p>Supports strategy options which specify the replication factor for
     * each datacenter. The replication factor for the entire cluster is the
     * sum of all per datacenter values. Note that the datacenter names
     * must match those used in conf/cassandra-topology.properties.</p>
     */
    NETWORK_TOPOLOGY("NetworkTopologyStrategy");

    private final String strategy;

    ReplicationStrategy(String strategy) {
        this.strategy = strategy;
    }

    public String getStrategyClass() {
        return this.strategy;
    }
}
