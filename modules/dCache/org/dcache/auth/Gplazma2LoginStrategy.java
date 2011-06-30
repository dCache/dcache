package org.dcache.auth;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;

import org.dcache.gplazma.GPlazma;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.configuration.FromFileConfigurationLoadingStrategy;
import org.dcache.gplazma.configuration.ConfigurationLoadingStrategy;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.LoginAttribute;

import javax.security.auth.Subject;
import java.util.Set;
import java.util.Map;
import java.util.Properties;
import java.util.Collections;
import java.io.File;
import java.security.Principal;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.dcache.gplazma.NoSuchPrincipalException;
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

    public Properties getEnvironmentAsProperties()
    {
        Replaceable replaceable = new Replaceable() {
                @Override
                public String getReplacement(String name)
                {
                    Object value =  _environment.get(name);
                    return (value == null) ? null : value.toString();
                }
            };

        Properties properties = new Properties();
        for (Map.Entry<String,Object> e: _environment.entrySet()) {
            String key = e.getKey();
            String value = String.valueOf(e.getValue());
            properties.put(key, Formats.replaceKeywords(value, replaceable));
        }

        return properties;
    }

    public void init()
    {
        ConfigurationLoadingStrategy configuration =
            new FromFileConfigurationLoadingStrategy(_configurationFile);
        _gplazma =
            new GPlazma(configuration, getEnvironmentAsProperties());
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
        try {
            return _gplazma.map(principal);
        } catch (NoSuchPrincipalException e) {
            return null;
        }
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException
    {
        try {
            return _gplazma.reverseMap(principal);
        } catch (NoSuchPrincipalException e) {
            return Collections.emptySet();
        }
    }
}
