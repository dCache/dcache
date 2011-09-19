package org.dcache.xrootd2.security.plugins.tokenauthz;

import java.util.Properties;
import java.io.File;
import java.io.IOException;

import org.dcache.xrootd2.security.AuthorizationProvider;
import org.dcache.xrootd2.security.AuthorizationFactory;
import org.dcache.xrootd2.util.ParseException;

import com.google.common.collect.ImmutableSet;

public class TokenAuthorizationProvider implements AuthorizationProvider
{
    private final static ImmutableSet<String> PLUGINS =
        ImmutableSet.of(TokenAuthorizationFactory.NAME,
                        "org.dcache.xrootd2.security.plugins.tokenauthz.TokenAuthorizationFactory");

    @Override
    public AuthorizationFactory createFactory(String plugin, Properties properties)
        throws ParseException, IOException
    {
        if (!PLUGINS.contains(plugin)) {
            return null;
        }
        String file = properties.getProperty("xrootdAuthzKeystore");
        return new TokenAuthorizationFactory(new File(file));
    }
}