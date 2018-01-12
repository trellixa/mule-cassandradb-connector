/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.api;


public class CreateKeyspaceInput {

    private String keyspaceName;

    private ReplicationStrategy replicationStrategyClass;

    private Integer replicationFactor;
    private DataCenter firstDataCenter;
    private DataCenter nextDataCenter;

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public ReplicationStrategy getReplicationStrategyClass() {
        return replicationStrategyClass;
    }

    public void setReplicationStrategyClass(ReplicationStrategy replicationStrategyClass) {
        this.replicationStrategyClass = replicationStrategyClass;
    }

    public Integer getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Integer replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public DataCenter getFirstDataCenter() {
        return firstDataCenter;
    }

    public void setFirstDataCenter(DataCenter firstDataCenter) {
        this.firstDataCenter = firstDataCenter;
    }

    public DataCenter getNextDataCenter() {
        return nextDataCenter;
    }

    public void setNextDataCenter(DataCenter nextDataCenter) {
        this.nextDataCenter = nextDataCenter;
    }
}
