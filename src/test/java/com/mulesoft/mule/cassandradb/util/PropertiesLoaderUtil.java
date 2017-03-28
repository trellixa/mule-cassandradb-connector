package com.mulesoft.mule.cassandradb.util;

import com.mulesoft.mule.cassandradb.utils.CassandraConfig;
import com.mulesoft.mule.cassandradb.utils.Constants;
import org.apache.log4j.Logger;
import org.mule.tools.devkit.ctf.configuration.util.ConfigurationUtils;
import org.mule.tools.devkit.ctf.exceptions.ConfigurationLoadingFailedException;

import java.util.Properties;

public class PropertiesLoaderUtil {

    private static final Logger logger = Logger.getLogger(PropertiesLoaderUtil.class);

    public static CassandraConfig resolveCassandraConnectionProps() {
        Properties cassandraConnProps;
        try {
            cassandraConnProps = ConfigurationUtils.getAutomationCredentialsProperties();
            return new CassandraConfig(cassandraConnProps.getProperty(Constants.CASS_HOST),
                    Integer.valueOf(cassandraConnProps.getProperty(Constants.CASS_PORT)));
        } catch (ConfigurationLoadingFailedException e) {
            logger.error("Error occurred while loading properties!", e);
        }
        return null;
    }
}
