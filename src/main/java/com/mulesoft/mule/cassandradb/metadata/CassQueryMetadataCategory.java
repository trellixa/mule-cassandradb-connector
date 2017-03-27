package com.mulesoft.mule.cassandradb.metadata;

import com.datastax.driver.core.TableMetadata;
import com.mulesoft.mule.cassandradb.CassandraDBConnector;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import org.apache.commons.collections.CollectionUtils;
import org.mule.api.annotations.MetaDataKeyRetriever;
import org.mule.api.annotations.MetaDataRetriever;
import org.mule.api.annotations.components.MetaDataCategory;
import org.mule.common.metadata.*;
import org.mule.common.metadata.builder.DefaultMetaDataBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@MetaDataCategory
public class CassQueryMetadataCategory {

    final static Logger logger = LoggerFactory.getLogger(CassQueryMetadataCategory.class);

    @Inject
    private CassandraDBConnector cassandraConnector;

    /**
     * @return a list of {@link MetaDataKey} or empty list otherwise
     */
    @MetaDataKeyRetriever
    public List<MetaDataKey> getMetadataKeys() {
        logger.info("Retrieving metadata keys...");
        List<MetaDataKey> keys = new ArrayList<MetaDataKey>();
        final String keyspaceUsed = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient().getLoggedKeyspace();
        final CassandraClient cassandraClient = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient();

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

    /**
     * @param key the metadata key to build the info for
     * @return {@link MetaData} for the given {@link MetaDataKey key}.
     */
    @MetaDataRetriever
    public MetaData getInputMetaData(final MetaDataKey key) {
        logger.info("Retrieving input metadata for the key: {0}", key);
        final String keyspaceUsed = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient().getLoggedKeyspace();
        final CassandraClient cassandraClient = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient();

        //extract tables metadata from database
        TableMetadata tableMetadata = cassandraClient.fetchTableMetadata(keyspaceUsed, key.getId());

        //build the metadata
        if (tableMetadata != null) {
            MetaDataModel model =  new DefaultMetaDataBuilder().createPojo(TableMetadata.class).build();
            return new DefaultMetaData(model);
        }

        return new DefaultMetaData(null);
    }

    public CassandraDBConnector getCassandraConnector() {
        return cassandraConnector;
    }

    public void setCassandraConnector(CassandraDBConnector cassandraConnector) {
        this.cassandraConnector = cassandraConnector;
    }
}
