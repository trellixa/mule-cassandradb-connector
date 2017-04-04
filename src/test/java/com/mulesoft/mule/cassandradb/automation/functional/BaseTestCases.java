package com.mulesoft.mule.cassandradb.automation.functional;

import org.mule.tools.devkit.ctf.junit.AbstractTestCase;
import com.mulesoft.mule.cassandradb.CassandraDBConnector;

public class BaseTestCases extends AbstractTestCase<CassandraDBConnector> {
 
    public BaseTestCases() {
        super(CassandraDBConnector.class);
    }

}
