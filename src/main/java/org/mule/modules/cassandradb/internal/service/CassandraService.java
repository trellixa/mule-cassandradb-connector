package org.mule.modules.cassandradb.internal.service;

import org.mule.connectors.commons.template.service.ConnectorService;
import org.mule.modules.cassandradb.api.CreateKeyspaceInput;
import org.mule.runtime.extension.api.annotation.param.Content;

public interface CassandraService extends ConnectorService {

    boolean createKeyspace(@Content CreateKeyspaceInput input);
}
