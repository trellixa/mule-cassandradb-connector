package org.mule.modules.cassandradb.automation.functional;

import org.junit.After;
import org.junit.Before;
import org.mule.modules.cassandradb.automation.util.TestsConstants;
import org.mule.runtime.api.component.location.Location;
import org.mule.runtime.api.metadata.MetadataKey;
import org.mule.runtime.api.metadata.MetadataService;

import javax.inject.Inject;
import java.io.File;

import static java.lang.Thread.sleep;

public class AbstractMetadataTestCases extends AbstractTestCases{

    public static final String FAIL_MESSAGE = "No assertions file was found for metadata key =  '%s'. It was created in the file %s. Please move it into src/test/resources/datasense/%s and re-run the test.";
    public static final String PATH_TEMPLATE = "/datasense/%s/%s.json";

    private static Location location;
    @Inject
    protected MetadataService metadataService;
    private File serializedMetadataFile;
    private MetadataKey metadataKey;
    @Before
    public void setUpMetadata() throws InterruptedException {
        getCassandraService().createTable(TestDataBuilder.getBasicCreateTableInput(TestDataBuilder.getMetadataColumns(), getCassandraProperties().getKeyspace(), TestsConstants.TABLE_NAME_2));
        //required delay to make sure the setup is ok
        sleep(5000);
    }

    @After
    public void tearDownMetadata(){
        getCassandraService().dropTable(TestsConstants.TABLE_NAME_2, getCassandraProperties().getKeyspace());
    }

    public static Location getLocation() {
        return location;
    }

    public MetadataService getMetadataService() {
        return metadataService;
    }

    public File getSerializedMetadataFile() {
        return serializedMetadataFile;
    }

    public MetadataKey getMetadataKey() {
        return metadataKey;
    }

    public static void setLocation(Location location) {
        AbstractMetadataTestCases.location = location;
    }

    public void setMetadataService(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    public void setSerializedMetadataFile(File serializedMetadataFile) {
        this.serializedMetadataFile = serializedMetadataFile;
    }

    public void setMetadataKey(MetadataKey metadataKey) {
        this.metadataKey = metadataKey;
    }

    public String getMetadataCategory() {
        return "invokemetadataresolver";
    }
}
