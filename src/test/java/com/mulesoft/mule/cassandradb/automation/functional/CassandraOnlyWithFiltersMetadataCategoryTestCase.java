/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.mule.cassandradb.automation.functional;

import com.mulesoft.mule.cassandradb.CassandraDBConnector;
import com.mulesoft.mule.cassandradb.api.CassandraClient;
import com.mulesoft.mule.cassandradb.configurations.ConnectionParameters;
import com.mulesoft.mule.cassandradb.metadata.CassandraOnlyWithFiltersMetadataCategory;
import com.mulesoft.mule.cassandradb.metadata.CreateKeyspaceInput;
import com.mulesoft.mule.cassandradb.util.ConstantsTest;
import com.mulesoft.mule.cassandradb.util.PropertiesLoaderUtil;
import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mule.tools.devkit.ctf.junit.AbstractMetaDataTestCase;

public class CassandraOnlyWithFiltersMetadataCategoryTestCase extends AbstractMetaDataTestCase<CassandraDBConnector> {

    private static CassandraClient cassClient;
    private static CassandraConfig cassConfig;

    public CassandraOnlyWithFiltersMetadataCategoryTestCase() {
        super(TestDataBuilder.cassandraCategoryMetadataTestKeys, CassandraOnlyWithFiltersMetadataCategory.class, CassandraDBConnector.class);
    }
    @BeforeClass
    public static void setup() throws Exception {
        //load required properties
        cassConfig = PropertiesLoaderUtil.resolveCassandraConnectionProps();
        assert cassConfig != null;
        //get instance of cass client based on the configs
        cassClient = CassandraClient.buildCassandraClient(new ConnectionParameters(cassConfig.getHost(), cassConfig.getPort(), null, null, null, null));
        assert cassClient != null;

        //setup db env

        CreateKeyspaceInput keyspaceInput = new CreateKeyspaceInput();
        keyspaceInput.setKeyspaceName(cassConfig.getKeyspace());

        cassClient.createKeyspace(keyspaceInput);
        cassClient.createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getPrimaryKey(), cassConfig.getKeyspace(), ConstantsTest.TABLE_NAME_2));

    }

    @AfterClass
    public static void tearDown() throws Exception {
        cassClient.dropTable(ConstantsTest.TABLE_NAME_2, cassConfig.getKeyspace());
    }
}
