package com.mulesoft.mule.cassandradb.utils;

public class CassandraConfig {

    private String host;

    private Integer port;

    public CassandraConfig(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
}
