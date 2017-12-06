package org.mule.modules.cassandradb.automation.functional;

import org.json.JSONArray;
import org.junit.Before;
import org.mule.runtime.api.meta.model.operation.OperationModel;
import org.mule.runtime.api.meta.model.parameter.ParameterModel;
import org.mule.runtime.api.metadata.descriptor.ComponentMetadataDescriptor;
import org.mule.runtime.api.metadata.resolving.MetadataResult;

import java.io.File;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.commons.io.FileUtils.write;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mule.runtime.api.component.location.Location.builder;

public class CassandraMetadataResolverTestCase extends AbstractMetadataTestCases{

    @Before
    public void setUpTest() throws Exception {
       setLocation(builder().globalName("insert-flow").addProcessorsPart().addIndexPart(0).build());
    }

    public void assertMetadataContents(File serializedMetadataFile) throws Exception {
        MetadataResult<ComponentMetadataDescriptor<OperationModel>> result = metadataService.getOperationMetadata(getLocation(), metadataKey);
        assertThat(result.isSuccess(), is(true));
        assertThat(result.getFailures(), hasSize(equalTo(0)));
        JSONArray actualMetadataJson = new JSONArray(result.get().getModel().getAllParameterModels().stream()
                .filter(parameterModel -> parameterModel.getName().equals("parameters"))
                .map(ParameterModel::getType)
                .collect(toList()));
        if (serializedMetadataFile.createNewFile()) {
            write(serializedMetadataFile, actualMetadataJson.toString());
            fail(format(FAIL_MESSAGE, metadataKey.getId(), serializedMetadataFile.getAbsolutePath(), getMetadataCategory()));
        } else {
            assertThat(actualMetadataJson.toList(), is(new JSONArray(readFileToString(serializedMetadataFile)).toList()));
        }
    }
}
