/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.util;

import org.mule.modules.cassandradb.internal.util.Constants;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

import static java.lang.String.format;

public class PropertiesLoaderUtil {
    public static CassandraProperties resolveCassandraConnectionProps() throws IOException {
        Properties cassandraConnProps;
        cassandraConnProps = getAutomationCredentialsProperties();
        return new CassandraProperties(cassandraConnProps.getProperty(Constants.CASS_HOST), cassandraConnProps.getProperty(Constants.CASS_PORT),
                cassandraConnProps.getProperty(Constants.KEYSPACE_NAME));
    }

    private static Properties getAutomationCredentialsProperties() throws IOException {
        Properties properties = new Properties();
        String automationFile = format("%s/src/test/resources/%s", System.getProperty("user.dir"),
                Optional.ofNullable(System.getProperty("automation-credentials.properties")).orElse("automation-credentials.properties"));
        try (InputStream inputStream = new FileInputStream(automationFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new FileNotFoundException(format("property file '%s' not found in the classpath", automationFile));
        }
        return properties;
    }
}
