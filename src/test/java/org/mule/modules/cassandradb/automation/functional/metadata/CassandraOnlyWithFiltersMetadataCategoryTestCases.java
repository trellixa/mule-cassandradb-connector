/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional.metadata;

import org.mule.modules.cassandradb.CassandraDBConnector;
import org.mule.modules.cassandradb.automation.functional.TestDataBuilder;
import org.mule.modules.cassandradb.metadata.CassandraOnlyWithFiltersMetadataCategory;

public class CassandraOnlyWithFiltersMetadataCategoryTestCases extends CassandraAbstractMetaDataTestCases {

    public CassandraOnlyWithFiltersMetadataCategoryTestCases() {
        super(TestDataBuilder.cassandraCategoryMetadataTestKeys, CassandraOnlyWithFiltersMetadataCategory.class, CassandraDBConnector.class);
    }
}
