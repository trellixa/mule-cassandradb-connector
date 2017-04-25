package com.mulesoft.mule.cassandradb.metadata;

import java.util.List;

public class CreateCQLQueryInput {

    private String cqlQuery;
    private List<Object> parameters;

    public String getCqlQuery() {
        return cqlQuery;
    }

    public void setCqlQuery(String cqlQuery) {
        this.cqlQuery = cqlQuery;
    }

    public List<Object> getParameters() {
        return parameters;
    }

    public void setParameters(List<Object> parameters) {
        this.parameters = parameters;
    }
}
