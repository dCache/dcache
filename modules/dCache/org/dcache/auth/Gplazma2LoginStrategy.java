package org.dcache.auth;

import dmg.cells.nucleus.EnvironmentAware;

import org.dcache.gplazma.GPlazma;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.configuration.FromFileConfigurationLoadingStrategy;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.LoginAttribute;

import javax.security.auth.Subject;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.io.File;
import java.security.Principal;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.springframework.beans.factory.annotation.Required;

/**
 * A LoginStrategy that wraps a org.dcache.gplazma.GPlazma
 *
 */
public class Gplazma2LoginStrategy
    implements LoginStrategy, EnvironmentAware
{
    private String _configurationFile;
    private GPlazma _gplazma;
    private Map<String,Object> _environment = Collections.emptyMap();

    @Required
    public void setConfigurationFile(String configurationFile)
    {
        if ((configurationFile == null) || (configurationFile.length() == 0)) {
            throw new IllegalArgumentException(
                    "configuration file argument wasn't specified correctly");
        } else if (!new File(configurationFile).exists()) {
            throw new IllegalArgumentException(
                    "configuration file does not exists at " + configurationFile);
        }
        _configurationFile = configurationFile;
    }

    public String getConfigurationFile()
    {
        return _configurationFile;
    }

    @Override
    public void setEnvironment(Map<String,Object> environment)
    {
        _environment = environment;
    }

    public Map<String,Object> getEnvironment()
    {
        return _environment;
    }

    public void init()
    {
        _gplazma =
            new GPlazma(new FromFileConfigurationLoadingStrategy(_configurationFile));
        // TODO: set environment
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
            return convertLoginReply(_gplazma.login(subject));
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