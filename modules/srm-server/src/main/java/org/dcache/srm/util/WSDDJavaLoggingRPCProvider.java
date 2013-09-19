package org.dcache.srm.util;

import org.apache.axis.EngineConfiguration;
import org.apache.axis.Handler;
import org.apache.axis.deployment.wsdd.WSDDProvider;
import org.apache.axis.deployment.wsdd.WSDDService;


/**
 * Factory class for producing LoggingRPCProvider instances.  This class must
 * be included in the
 *
 *     META-INF/services/org.apache.axis.deployment.wsdd.Provider
 *
 * file within a jar file to allow Axis to discover it.  The line is the
 * fully qualified class name.
 */
public class WSDDJavaLoggingRPCProvider extends WSDDProvider
{

    /*
     * The name to use in the 'provider' attribute of the 'service' element in
     * the WSDD file.
     *
     * Note that the name is the XML local-name of a QName.  The namespace
     * "http://xml.apache.org/axis/wsdd/providers/java" is added by Axis
     * automatically.  Since by default, WSDD files register this URI
     * with the XML-NS prefix 'java', the correct 'provider' attribute is
     *
     *     provider='java:logging-RPC'
     */
    @Override
    public String getName() {
        return "logging-RPC";
    }


    /**
     *  Create new instance of our provider.
     */
    @Override
    public Handler newProviderInstance(WSDDService service,
            EngineConfiguration registry) throws Exception
    {
        return new LoggingRPCProvider();
    }
}

