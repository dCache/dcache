package org.dcache.services.topology;

/**
 * A service that provides all hostnames of a dCacheInstance
 * @author jans
 */
public interface HostnameService {

    void updateHostnames();

    String getHostnames();
}
