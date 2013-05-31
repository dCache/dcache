package org.dcache.gplazma.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.configuration.ConfigurationItemControl;

/**
 * This is a LoginMonitor that stores logging information in the logger at
 * debug level.
 */
public class LoggingLoginMonitor extends IgnoringLoginMonitor
{
    private static final Logger _log =
            LoggerFactory.getLogger(LoggingLoginMonitor.class);


    @Override
    public void authBegins(Set<Object> publicCredentials,
            Set<Object> privateCredentials, Set<Principal> principals)
    {
        _log.debug("phase starts (public: {}, private: {}, principals: {})", publicCredentials, privateCredentials,
                   principals);
    }

    @Override
    public void authPluginBegins(String name, ConfigurationItemControl control,
            Set<Object> publicCredentials, Set<Object> privateCredentials,
            Set<Principal> principals)
    {
        _log.debug("calling (publicCred: {}, privateCred: {}, principals: {})", publicCredentials, privateCredentials,
                   principals);
    }

    @Override
    public void mapBegins(Set<Principal> principals)
    {
        _log.debug("phase starts (principals: {})", principals);
    }

    @Override
    public void mapPluginBegins(String name, ConfigurationItemControl control,
            Set<Principal> principals)
    {
        _log.debug("calling (principals: {})", principals);
    }

    @Override
    public void accountBegins(Set<Principal> principals)
    {
        _log.debug("phase starts (principals: {})", principals);
    }

    @Override
    public void accountPluginBegins(String name,
            ConfigurationItemControl control, Set<Principal> principals)
    {
        _log.debug("calling");
    }

    @Override
    public void sessionBegins(Set<Principal> principals)
    {
        _log.debug("phase starts (principals: {})", principals);
    }

    @Override
    public void sessionPluginBegins(String name,
            ConfigurationItemControl control, Set<Principal> principals,
            Set<Object> attributes)
    {
        _log.debug("calling (principals: {}, attributes: {})", principals,
                attributes);

    }

    @Override
    public void sessionEnds(Set<Principal> principals, Set<Object> attributes,
            Result result)
    {
        _log.debug("phase ends (principals: {}, attributes: {})", principals,
                attributes);
    }
}
