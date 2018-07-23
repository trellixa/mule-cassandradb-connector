/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package com.mulesoft.connectors.cassandradb.automation.functional;

import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mule.runtime.api.component.location.Location;
import org.mule.runtime.api.meta.model.operation.OperationModel;
import org.mule.runtime.api.metadata.MetadataKey;
import org.mule.runtime.api.metadata.MetadataService;
import org.mule.runtime.api.metadata.descriptor.ComponentMetadataDescriptor;
import org.mule.runtime.api.metadata.resolving.MetadataResult;

import javax.inject.Inject;
import java.io.File;

import static com.google.common.collect.Iterables.find;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.commons.io.FileUtils.write;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mule.runtime.api.component.location.Location.builder;
import static org.mule.runtime.api.metadata.MetadataKeyBuilder.newKey;

@Ignore
public class MetadataTestCase extends AbstractTestCases{

    public static final String FAIL_MESSAGE = "No assertions file was found for metadata key =  '%s'. It was created in the file %s. Please move it into src/test/resources/datasense/%s and re-run the test.";
    public static final String PATH_TEMPLATE = "/datasense/%s/%s-%s.json";

    private static Location location;
    @Inject
    protected MetadataService metadataService;
    private MetadataKey metadataKey = newKey(TestDataBuilder.metadataKeyName).withDisplayName(TestDataBuilder.metadataKeyName).build();

    @Before
    public void setUpMetadata() throws Exception {
        createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getMetadataColumns(), testKeyspace, TestDataBuilder.TABLE_NAME_2));
        //required delay to make sure the setup is ok
        sleep(5000);
    }

    protected File createSerializedMetadataFile(String operation){
        File metadataFile = new File(getClass().getResource("/").getFile(), format(PATH_TEMPLATE, getMetadataCategory(), operation, metadataKey.getId()));
        metadataFile.getParentFile().mkdirs();
        return metadataFile;
    }

    public void assertMetadataContents(File serializedMetadataFile, String inputField) throws Exception {
        MetadataResult<ComponentMetadataDescriptor<OperationModel>> result = metadataService.getOperationMetadata(location, metadataKey);
        assertThat(result.isSuccess(), is(true));
        assertThat(result.getFailures(), hasSize(equalTo(0)));
        JSONObject actualMetadataJson = new JSONObject(find(result.get().getModel().getAllParameterModels(), input -> input.getName().equals(inputField)).getType());
        if (serializedMetadataFile.createNewFile()) {
            write(serializedMetadataFile, actualMetadataJson.toString());
            fail(format(FAIL_MESSAGE, metadataKey.getId(), serializedMetadataFile.getAbsolutePath(), getMetadataCategory()));
        } else {
            assertThat(actualMetadataJson.toMap(), is(new JSONObject(readFileToString(serializedMetadataFile)).toMap()));
        }
    }

    public void testInputMetadata(String flowName, String inputField) throws Exception {
        location = builder().globalName(flowName).addProcessorsPart().addIndexPart(0).build();
        assertMetadataContents(createSerializedMetadataFile(flowName), inputField);
    }

    @Test
    public void testCassandraMetadataResolverInputMetadata() throws Exception {
        testInputMetadata(TestDataBuilder.insertFlowName, "entityToInsert");
    }

    @Test
    public void testCassandraOnlyWithFiltersInputMetadata() throws Exception {
        testInputMetadata(TestDataBuilder.deleteRowsFlowName, "whereClause");
    }

    @Test
    public void testCassadraWithFiltersInputMetadata() throws Exception {
        testInputMetadata(TestDataBuilder.updateFlowName, "entityToUpdate");
    }

    @After
    public void tearDownMetadata() throws Exception {
        dropTable(testKeyspace, TestDataBuilder.TABLE_NAME_2);
    }

    public String getMetadataCategory() {
        return "invokemetadataresolver";
    }
}
