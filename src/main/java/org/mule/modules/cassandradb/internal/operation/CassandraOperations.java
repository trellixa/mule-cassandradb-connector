package org.mule.modules.cassandradb.internal.operation;

import org.mule.modules.cassandradb.internal.config.CassandraConfig;
import org.mule.modules.cassandradb.internal.connection.CassandraConnection;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.MediaType;

import static org.mule.runtime.extension.api.annotation.param.MediaType.TEXT_PLAIN;

public class CassandraOperations {

    @MediaType(TEXT_PLAIN)
    public String test(@Config CassandraConfig config, @Connection CassandraConnection connection) {
        return "temporary operation to make connector work";
    }
}
