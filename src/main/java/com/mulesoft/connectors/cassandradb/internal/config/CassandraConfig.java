package com.mulesoft.connectors.cassandradb.internal.config;

import com.mulesoft.connectors.cassandradb.internal.connection.CassandraConnectionProvider;
import com.mulesoft.connectors.cassandradb.internal.operation.CassandraOperations;
import org.mule.connectors.commons.template.config.ConnectorConfig;
import org.mule.runtime.extension.api.annotation.Configuration;
import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;

@Configuration(name = "config")
@ConnectionProviders(CassandraConnectionProvider.class)
@Operations(CassandraOperations.class)
public class CassandraConfig implements ConnectorConfig {
}
