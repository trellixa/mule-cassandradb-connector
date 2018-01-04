/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.api;

/**
 * Cassandra supported replica placement strategies
 */
// FIXME: Enums should not have any kind of logic, they should only contain their values.
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
    SimpleStrategy,
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
    NetworkTopologyStrategy;
}
