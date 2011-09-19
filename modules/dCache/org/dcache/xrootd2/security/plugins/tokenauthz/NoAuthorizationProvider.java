package org.dcache.xrootd2.security.plugins.tokenauthz;

import java.util.Properties;

import org.dcache.xrootd2.security.AuthorizationProvider;
import org.dcache.xrootd2.security.AuthorizationFactory;

import com.google.common.collect.ImmutableSet;

public class NoAuthorizationProvider implements AuthorizationProvider
{
    private final static ImmutableSet<String> PLUGINS =
        ImmutableSet.of(NoAuthorizationFactory.NAME,
                        "org.dcache.xrootd2.security.plugins.tokenauthz.NoAuthorizationFactory");

    @Override
    public AuthorizationFactory createFactory(String plugin, Properties properties)
    {
        return PLUGINS.contains(plugin) ? new NoAuthorizationFactory() : null;
    }
}