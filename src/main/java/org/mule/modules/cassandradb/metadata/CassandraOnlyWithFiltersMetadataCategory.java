/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.metadata;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import org.mule.modules.cassandradb.utils.Constants;
import org.mule.api.annotations.MetaDataRetriever;
import org.mule.api.annotations.components.MetaDataCategory;
import org.mule.common.metadata.DefaultMetaData;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.MetaDataKey;
import org.mule.common.metadata.builder.DefaultMetaDataBuilder;
import org.mule.common.metadata.builder.DynamicObjectBuilder;

@MetaDataCategory
public class CassandraOnlyWithFiltersMetadataCategory extends CassandraMetadataCategory {

    /**
     * @param key the metadata key to build the info for
     * @return {@link MetaData} for the given {@link MetaDataKey key}.
     */
    @Override
    @MetaDataRetriever
    public MetaData getInputMetaData(final MetaDataKey key) {

        //extract tables metadata from database
        TableMetadata tableMetadata = getTableMetadata(key);

        //build the metadata
        if (tableMetadata != null && tableMetadata.getColumns() != null) {
            DynamicObjectBuilder<?> entityModel = new DefaultMetaDataBuilder().createDynamicObject(tableMetadata.getName());

            DynamicObjectBuilder<?> whereClause = entityModel.addDynamicObjectField(Constants.WHERE);
            if (tableMetadata.getPrimaryKey().size() == 1) {
                for (ColumnMetadata column : tableMetadata.getPrimaryKey()) {
                    addMetadataListField(whereClause, column);
                }
            } else {
                addMetadataField(whereClause, tableMetadata.getPrimaryKey().get(0));
            }
            whereClause.endDynamicObject();
            return new DefaultMetaData(entityModel.build());
        }

        return new DefaultMetaData(null);
    }

    private void addMetadataListField(DynamicObjectBuilder<?> listEntityModel, ColumnMetadata column) {

        listEntityModel.addList(column.getName()).ofSimpleField(resolveDataType(column.getType()));
    }
}
