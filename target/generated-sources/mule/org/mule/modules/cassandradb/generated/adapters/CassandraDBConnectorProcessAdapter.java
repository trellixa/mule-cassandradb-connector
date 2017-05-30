
package org.mule.modules.cassandradb.generated.adapters;

import javax.annotation.Generated;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.devkit.ProcessAdapter;
import org.mule.api.devkit.ProcessTemplate;
import org.mule.api.lifecycle.Initialisable;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.routing.filter.Filter;
import org.mule.devkit.internal.lic.LicenseValidatorFactory;
import org.mule.devkit.internal.lic.validator.LicenseValidator;
import org.mule.modules.cassandradb.CassandraDBConnector;
import org.mule.security.oauth.callback.ProcessCallback;


/**
 * A <code>CassandraDBConnectorProcessAdapter</code> is a wrapper around {@link CassandraDBConnector } that enables custom processing strategies.
 * 
 */
@SuppressWarnings("all")
@Generated(value = "Mule DevKit Version 3.9.1", date = "2017-05-30T02:13:49-03:00", comments = "Build UNNAMED.2797.33dc1d7")
public class CassandraDBConnectorProcessAdapter
    extends CassandraDBConnectorLifecycleInjectionAdapter
    implements ProcessAdapter<CassandraDBConnectorCapabilitiesAdapter> , Initialisable
{


    public<P >ProcessTemplate<P, CassandraDBConnectorCapabilitiesAdapter> getProcessTemplate() {
        final CassandraDBConnectorCapabilitiesAdapter object = this;
        return new ProcessTemplate<P,CassandraDBConnectorCapabilitiesAdapter>() {


            @Override
            public P execute(ProcessCallback<P, CassandraDBConnectorCapabilitiesAdapter> processCallback, MessageProcessor messageProcessor, MuleEvent event)
                throws Exception
            {
                return processCallback.process(object);
            }

            @Override
            public P execute(ProcessCallback<P, CassandraDBConnectorCapabilitiesAdapter> processCallback, Filter filter, MuleMessage message)
                throws Exception
            {
                return processCallback.process(object);
            }

        }
        ;
    }

    @Override
    public void initialise()
        throws InitialisationException
    {
        super.initialise();
        checkMuleLicense();
    }

    /**
     * Obtains the expression manager from the Mule context and initialises the connector. If a target object  has not been set already it will search the Mule registry for a default one.
     * 
     * @throws InitialisationException
     */
    private void checkMuleLicense() {
        LicenseValidator licenseValidator = LicenseValidatorFactory.getValidator("CassandraDB");
        licenseValidator.checkEnterpriseLicense(true);
    }

}
