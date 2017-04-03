package com.mulesoft.mule.cassandradb.util;

import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.mule.tools.devkit.ctf.configuration.util.ConfigurationUtils;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;

import java.util.Properties;

public class PropertiesLoaderUtil {

    public static CassandraConfig resolveCassandraConnectionProps() throws ConfigurationLoadingFailedException {
        Properties cassandraConnProps;
        cassandraConnProps = ConfigurationUtils.getAutomationCredentialsProperties();
        return new CassandraConfig(cassandraConnProps.getProperty(Constants.CASS_HOST),
                Integer.valueOf(cassandraConnProps.getProperty(Constants.CASS_PORT)));
    }
}
