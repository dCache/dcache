package org.dcache.gplazma.monitor;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.configuration.ConfigurationItemControl;

/**
 * This class provides a mechanism to combine multiple LoginMonitor
 * monitors so that all monitors see all events.
 */
public class CombinedLoginMonitor implements LoginMonitor
{
    private final LoginMonitor[] _inner;

    /**
     * A convenience method for creating a new CombinedLoginMonitor instance.
     */
    public static LoginMonitor of(LoginMonitor... monitors)
    {
        return new CombinedLoginMonitor(monitors);
    }

    public CombinedLoginMonitor(LoginMonitor... monitors)
    {
        _inner = monitors;
    }

    @Override
    public void authBegins(Set<Object> publicCredentials,
            Set<Object> privateCredentials, Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.authBegins(publicCredentials, privateCredentials,
                    principals);
        }
    }

    @Override
    public void authPluginBegins(String name, ConfigurationItemControl control,
            Set<Object> publicCredentials, Set<Object> privateCredentials,
            Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.authPluginBegins(name, control, publicCredentials,
                    privateCredentials, principals);
        }
    }

    @Override
    public void authPluginEnds(String name, ConfigurationItemControl control,
            Result result, String error, Set<Object> publicCredentials,
            Set<Object> privateCredentials, Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.authPluginEnds(name, control, result, error,
                    publicCredentials, privateCredentials, principals);
        }
    }

    @Override
    public void authEnds(Set<Principal> principals, Result result)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.authEnds(principals, result);
        }
    }

    @Override
    public void mapBegins(Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.mapBegins(principals);
        }
    }

    @Override
    public void mapPluginBegins(String name, ConfigurationItemControl control,
            Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.mapPluginBegins(name, control, principals);
        }
    }

    @Override
    public void mapPluginEnds(String name, ConfigurationItemControl control,
            Result result, String error, Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.mapPluginEnds(name, control, result, error, principals);
        }
    }

    @Override
    public void mapEnds(Set<Principal> principals, Result result)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.mapEnds(principals, result);
        }
    }

    @Override
    public void accountBegins(Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.accountBegins(principals);
        }
    }

    @Override
    public void accountPluginBegins(String name,
            ConfigurationItemControl control, Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.accountPluginBegins(name, control, principals);
        }
    }

    @Override
    public void accountPluginEnds(String name,
            ConfigurationItemControl control, Result result, String error,
            Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.accountPluginEnds(name, control, result, error, principals);
        }
    }

    @Override
    public void accountEnds(Set<Principal> principals, Result result)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.accountEnds(principals, result);
        }
    }

    @Override
    public void sessionBegins(Set<Principal> principals)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.sessionBegins(principals);
        }
    }

    @Override
    public void sessionPluginBegins(String name,
            ConfigurationItemControl control, Set<Principal> principals,
            Set<Object> attributes)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.sessionPluginBegins(name, control, principals, attributes);
        }
    }

    @Override
    public void sessionPluginEnds(String name,
            ConfigurationItemControl control, Result result, String error,
            Set<Principal> principals, Set<Object> attributes)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.sessionPluginEnds(name, control, result, error,
                    principals, attributes);
        }
    }

    @Override
    public void sessionEnds(Set<Principal> principals, Set<Object> attributes,
            Result result)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.sessionEnds(principals, attributes, result);
        }
    }

    @Override
    public void validationResult(Result result, String error)
    {
        for(LoginMonitor monitor : _inner) {
            monitor.validationResult(result, error);
        }
    }
}
