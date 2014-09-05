/**
 * Mule Cassandra Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package com.mulesoft.mule.cassandradb;

/**
 * Cassandra supported replica placement strategies
 */
public enum ReplicationStrategy {
    /**
     * SimpleStrategy merely places the first replica at the node whose
     * token is closest to the key (as determined by the Partitioner), and
     * additional replicas on subsequent nodes along the ring in increasing
     * Token order.
     * <p/>
     * Supports a single strategy option 'replication_factor' that
     * specifies the replication factor for the cluster.
     */
    SIMPLE {
        /**
         * Returns a string representation of the object.
         * @return a string.
         */
        public String toString() {
            return "org.apache.cassandra.locator.SimpleStrategy";
        }
    },
    /**
     * With NetworkTopologyStrategy, for each datacenter, you can specify
     * how many replicas you want on a per-keyspace basis. Replicas are
     * placed on different racks within each DC, if possible.
     * <p/>
     * Supports strategy options which specify the replication factor for
     * each datacenter. The replication factor for the entire cluster is the
     * sum of all per datacenter values. Note that the datacenter names
     * must match those used in conf/cassandra-topology.properties.
     */
    NETWORK_TOPOLOGY {
        /**
         * Returns a string representation of the object.
         * @return a string.
         */
        public String toString() {
            return "org.apache.cassandra.locator.NetworkTopologyStrategy";
        }
    },
    /**
     * OldNetworkToplogyStrategy [formerly RackAwareStrategy]
     * places one replica in each of two datacenters, and the third on a
     * different rack in in the first.  Additional datacenters are not
     * guaranteed to get a replica.  Additional replicas after three are
     * placed in ring order after the third without regard to rack or
     * datacenter.
     * <p/>
     * Supports a single strategy option 'replication_factor' that
     * specifies the replication factor for the cluster.
     */
    OLD_NETWORK_TOPOLOGY {
        /**
         * Returns a string representation of the object.
         * @return a string.
         */
        public String toString() {
            return "org.apache.cassandra.locator.OldNetworkTopologyStrategy";
        }
    }
}
