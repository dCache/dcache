package org.dcache.services.info;

/**
 *  Information related to dCache configuration.
 */
public class Configuration
{
    public static final String PROPERTY_NAME_SERVICE_TOPO = "info.service.topo";
    public static final String PROPERTY_NAME_SERVICE_POOLMANAGER = "info.service.poolmanager";
    public static final String PROPERTY_NAME_SERVICE_SPACEMANAGER = "info.service.spacemanager";

    private Configuration()
    {
        // prevent instantiation.
    }
}
