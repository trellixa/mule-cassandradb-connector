package org.mule.modules.cassandradb.internal.config;

import org.mule.connectors.commons.template.config.ConnectorConfig;
import org.mule.modules.cassandradb.internal.connection.BasicAuthConnectionProvider;
import org.mule.modules.cassandradb.internal.operation.CassandraOperations;
import org.mule.runtime.extension.api.annotation.Configuration;
import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;

@Configuration(name = "config")
@ConnectionProviders({
        BasicAuthConnectionProvider.class
})
@Operations({
        CassandraOperations.class,
})
public class CassandraConfig implements ConnectorConfig {
}
