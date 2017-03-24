package com.mulesoft.mule.cassandradb.metadata;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.mulesoft.mule.cassandradb.CassandraDBConnector;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.utils.CassandraDBException;
import org.apache.commons.collections.CollectionUtils;
import org.mule.api.ConnectionException;
import org.mule.api.annotations.MetaDataKeyRetriever;
import org.mule.api.annotations.MetaDataRetriever;
import org.mule.api.annotations.components.MetaDataCategory;
import org.mule.common.metadata.*;
import org.mule.common.metadata.builder.DefaultMetaDataBuilder;
import org.mule.common.metadata.builder.DynamicObjectBuilder;
import org.mule.common.metadata.builder.PojoMetaDataBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.wsdl.WSDLException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

@MetaDataCategory
public class CassQueryMetadataCategory {

    final static Logger logger = LoggerFactory.getLogger(CassQueryMetadataCategory.class);

    @Inject
    private CassandraDBConnector cassandraConnector;

    final String keyspaceUsed = cassandraConnector.getBasicAuthConnectionStrategy().getKeyspace();

    final CassandraClient cassandraClient = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient();

    @MetaDataKeyRetriever
    public List<MetaDataKey> getMetadataKeys() {
        logger.info("Retrieving metadata keys...");
        List<MetaDataKey> keys = new ArrayList<MetaDataKey>();

        //extract metadata from database
        List<String> keyNames = cassandraClient.getTableNamesFromKeyspace(keyspaceUsed);

        //build the metadata
        if (CollectionUtils.isNotEmpty(keyNames)) {
            for (String type : keyNames) {
                final DefaultMetaDataKey dataKey = new DefaultMetaDataKey(type, type);
                keys.add(dataKey);
            }
        }

        return keys;
    }

    @MetaDataRetriever
    public MetaData getInputMetaData(final MetaDataKey key) {
        //extract metadata from database
        TableMetadata tableMetadata = cassandraClient.fetchTableMetadata(key.getCategory());
        if (tableMetadata != null) {
            MetaDataModel model =  new DefaultMetaDataBuilder().createPojo(TableMetadata.class).build();
            return new DefaultMetaData(model);
        }
        return null;
    }

    public CassandraDBConnector getCassandraConnector() {
        return cassandraConnector;
    }

    public void setCassandraConnector(CassandraDBConnector cassandraConnector) {
        this.cassandraConnector = cassandraConnector;
    }
}
