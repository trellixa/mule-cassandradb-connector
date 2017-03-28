package com.mulesoft.mule.cassandradb.metadata;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.mulesoft.mule.cassandradb.CassandraDBConnector;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import org.apache.commons.collections.CollectionUtils;
import org.mule.api.annotations.MetaDataKeyRetriever;
import org.mule.api.annotations.MetaDataRetriever;
import org.mule.api.annotations.components.MetaDataCategory;
import org.mule.common.metadata.*;
import org.mule.common.metadata.builder.DefaultMetaDataBuilder;
import org.mule.common.metadata.builder.DynamicObjectBuilder;
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
        logger.info("Retrieving input metadata for the key: {}", key);
        final String keyspaceUsed = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient().getLoggedKeyspace();
        final CassandraClient cassandraClient = cassandraConnector.getBasicAuthConnectionStrategy().getCassandraClient();

        //extract tables metadata from database
        TableMetadata tableMetadata = cassandraClient.fetchTableMetadata(keyspaceUsed, key.getId());

        //build the metadata
        if (tableMetadata != null && tableMetadata.getColumns() != null) {
            DynamicObjectBuilder<?> listEntityModel = new DefaultMetaDataBuilder().createList().ofDynamicObject(tableMetadata.getName());

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                addMetadataField(listEntityModel, column);
            }

            return new DefaultMetaData(listEntityModel.build());
        }

        return new DefaultMetaData(null);
    }

    private void addMetadataField(DynamicObjectBuilder<?> listEntityModel, ColumnMetadata column) {
        org.mule.common.metadata.datatype.DataType columnDataType = resolveDataType(column);
        listEntityModel.addSimpleField(column.getName(), columnDataType);
    }

    /**
     * maps Datastax column types to Mule data types
     */
    private org.mule.common.metadata.datatype.DataType resolveDataType(ColumnMetadata column) {
        switch(column.getType().getName()){
            case UUID:
            case TIMEUUID:
            case TEXT:
            case VARCHAR:
            case ASCII:
            case INET:
                return org.mule.common.metadata.datatype.DataType.STRING;
            case FLOAT:
                return org.mule.common.metadata.datatype.DataType.FLOAT;
            case DOUBLE:
                return org.mule.common.metadata.datatype.DataType.DOUBLE;
            case BOOLEAN:
                return org.mule.common.metadata.datatype.DataType.BOOLEAN;
            case INT:
            case VARINT:
            case SMALLINT:
            case TINYINT:
            case BIGINT:
                return org.mule.common.metadata.datatype.DataType.INTEGER;
            case DECIMAL:
                return org.mule.common.metadata.datatype.DataType.DECIMAL;
            case DATE:
                return org.mule.common.metadata.datatype.DataType.DATE;
            case BLOB:
                return org.mule.common.metadata.datatype.DataType.POJO;
            case TIMESTAMP:
                return org.mule.common.metadata.datatype.DataType.DATE_TIME;
            default: return org.mule.common.metadata.datatype.DataType.UNKNOWN;
        }
    }

    public CassandraDBConnector getCassandraConnector() {
        return cassandraConnector;
    }

    public void setCassandraConnector(CassandraDBConnector cassandraConnector) {
        this.cassandraConnector = cassandraConnector;
    }
}
