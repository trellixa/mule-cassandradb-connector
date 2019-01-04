/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.cassandradb.automation.functional;

import org.mule.modules.cassandradb.api.ColumnInput;
import org.mule.modules.cassandradb.api.ColumnType;
import org.mule.modules.cassandradb.api.CreateTableInput;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.Optional.ofNullable;

public class TestDataBuilder {

    public final static String KEYSPACE_DUMMY = "dummy_keyspace";
    public final static String TABLE_NAME_2 = "dummy_table_name_2";
    public static List<String> cassandraCategoryMetadataTestKeys = new LinkedList<String>();
    public static final String metadataKeyName = "dummy_table_name_2";
    public static final String insertFlowName = "insert-flow";
    public static final String deleteRowsFlowName = "deleteRows-flow";
    public static final String updateFlowName = "update-flow";

    static {
        cassandraCategoryMetadataTestKeys.add(TABLE_NAME_2);
    }

    public static List<ColumnInput> getMetadataColumns() {
        List<ColumnInput> columns = new ArrayList<ColumnInput>();
        columns.add(createColumn(true, "NUMBER", ColumnType.INT));
        columns.add(createColumn(false, "TEXT", ColumnType.TEXT));
        columns.add(createColumn(false, "BOOL", ColumnType.BOOLEAN));
        return columns;
    }

    public static ColumnInput createColumn(boolean isPrimary, String name, ColumnType type) {
        ColumnInput column = new ColumnInput();
        column.setPrimaryKey(isPrimary);
        column.setName(name);
        column.setType(type);
        return column;
    }

    public static CreateTableInput getBasicCreateTableInput(List<ColumnInput> columns, String keyspaceName, String tableName) {
        CreateTableInput input = new CreateTableInput();
        input.setColumns(columns);
        input.setKeyspaceName(keyspaceName);
        input.setTableName(tableName);
        return input;
    }

    protected static Properties getAutomationCredentialsProperties() throws IOException {
        Properties properties = new Properties();
        String automationFile = format("%s/src/test/resources/%s", getProperty("user.dir"),
                ofNullable(getProperty("automation-credentials.properties")).orElse("automation-credentials.properties"));
        try (InputStream inputStream = new FileInputStream(automationFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new FileNotFoundException(format("property file '%s' not found in the classpath", automationFile));
        }
        return properties;
    }
}
