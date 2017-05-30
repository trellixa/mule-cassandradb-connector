
package org.mule.modules.cassandradb.generated.adapters;

import javax.annotation.Generated;
import org.mule.api.MetadataAware;
import org.mule.modules.cassandradb.CassandraDBConnector;


/**
 * A <code>CassandraDBConnectorMetadataAdapter</code> is a wrapper around {@link CassandraDBConnector } that adds support for querying metadata about the extension.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.1", date = "2017-05-30T02:13:49-03:00", comments = "Build UNNAMED.2797.33dc1d7")
public class CassandraDBConnectorMetadataAdapter
    extends CassandraDBConnectorCapabilitiesAdapter
    implements MetadataAware
{

    private final static String MODULE_NAME = "CassandraDB";
    private final static String MODULE_VERSION = "2.0.0";
    private final static String DEVKIT_VERSION = "3.9.1";
    private final static String DEVKIT_BUILD = "UNNAMED.2797.33dc1d7";
    private final static String MIN_MULE_VERSION = "3.6";

    public String getModuleName() {
        return MODULE_NAME;
    }

    public String getModuleVersion() {
        return MODULE_VERSION;
    }

    public String getDevkitVersion() {
        return DEVKIT_VERSION;
    }

    public String getDevkitBuild() {
        return DEVKIT_BUILD;
    }

    public String getMinMuleVersion() {
        return MIN_MULE_VERSION;
    }

}
