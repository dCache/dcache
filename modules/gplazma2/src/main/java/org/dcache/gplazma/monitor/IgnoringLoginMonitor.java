package org.dcache.gplazma.monitor;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.configuration.ConfigurationItemControl;

/**
 * This is a LoginMonitor that ignores all input.
 */
public class IgnoringLoginMonitor implements LoginMonitor
{
    @Override
    public void authBegins(Set<Object> publicCredentials,
            Set<Object> privateCredentials, Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void authPluginBegins(String name, ConfigurationItemControl control,
            Set<Object> publicCredentials, Set<Object> privateCredentials,
            Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void authPluginEnds(String name, ConfigurationItemControl control,
            Result result, String error, Set<Object> publicCredentials,
            Set<Object> privateCredentials, Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void authEnds(Set<Principal> principals, Result result)
    {
        // ignored
    }

    @Override
    public void mapBegins(Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void mapPluginBegins(String name, ConfigurationItemControl control,
            Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void mapPluginEnds(String name, ConfigurationItemControl control,
            Result result, String error, Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void mapEnds(Set<Principal> principals, Result result)
    {
        // ignored
    }

    @Override
    public void accountBegins(Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void accountPluginBegins(String name,
            ConfigurationItemControl control, Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void accountPluginEnds(String name,
            ConfigurationItemControl control, Result result, String error,
            Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void accountEnds(Set<Principal> principals, Result result)
    {
        // ignored
    }

    @Override
    public void sessionBegins(Set<Principal> principals)
    {
        // ignored
    }

    @Override
    public void sessionPluginBegins(String name,
            ConfigurationItemControl control, Set<Principal> principals,
            Set<Object> attributes)
    {
        // ignored
    }

    @Override
    public void sessionPluginEnds(String name,
            ConfigurationItemControl control, Result result, String error,
            Set<Principal> principals, Set<Object> attributes)
    {
        // ignored
    }

    @Override
    public void sessionEnds(Set<Principal> principals, Set<Object> attributes,
            Result result)
    {
        // ignored
    }

    @Override
    public void validationResult(Result result, String error)
    {
        // ignored
    }
}
