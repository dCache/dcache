package org.dcache.auth;

import org.dcache.gplazma.GPlazma;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.configuration.FromFileConfigurationLoadingStrategy;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.LoginAttribute;

import javax.security.auth.Subject;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;
import java.security.Principal;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * A LoginStrategy that wraps a org.dcache.gplazma.GPlazma
 *
 */
public class Gplazma2LoginStrategy implements LoginStrategy
{
    private String configurationFile;
    private GPlazma gplazma;

    public void setConfigurationFile(String configurationFile)
    {
        if (configurationFile == null) {
            throw new NullPointerException();
        }
        this.configurationFile = configurationFile;
        gplazma = new GPlazma(
                new FromFileConfigurationLoadingStrategy(configurationFile));
    }

    public String getConfigurationFile()
    {
        return configurationFile;
    }

    private LoginReply
        convertLoginReply(org.dcache.gplazma.LoginReply gPlazmaLoginReply)
    {
        Set<Object> sessionAttributes =
            gPlazmaLoginReply.getSessionAttributes();
        Set<LoginAttribute> loginAttributes =
            Sets.newHashSet(Iterables.filter(sessionAttributes, LoginAttribute.class));
        return new LoginReply(gPlazmaLoginReply.getSubject(), loginAttributes);
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException
    {
        try {
            return convertLoginReply(gplazma.login(subject));
        } catch (AuthenticationException e) {
            throw new PermissionDeniedCacheException("Login failed: " + e.getMessage());
        }
    }

    @Override
    public Principal map(Principal principal) throws CacheException
    {
        return null;
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException
    {
        return Collections.emptySet();
    }
}