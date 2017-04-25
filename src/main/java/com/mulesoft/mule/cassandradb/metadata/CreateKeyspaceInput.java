package com.mulesoft.mule.cassandradb.metadata;


public class CreateKeyspaceInput {

    private String keyspaceName;

    private String replicationStrategyClass;

    private Integer replicationFactor;
    private DataCenter firstDataCenter;
    private DataCenter nextDataCenter;

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public String getReplicationStrategyClass() {
        return replicationStrategyClass;
    }

    public void setReplicationStrategyClass(String replicationStrategyClass) {
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
