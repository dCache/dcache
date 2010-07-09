package org.dcache.auth;

import org.dcache.gplazma.GPlazma;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.configuration.FromFileConfigurationLoadingStrategy;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.auth.attributes.ReadOnly;

import javax.security.auth.Subject;
import java.util.HashSet;

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

    private LoginReply convertLoginReply(org.dcache.gplazma.LoginReply gPlazmaLoginReply)
    {
        LoginReply reply =
            new LoginReply(
                gPlazmaLoginReply.getSubject(),
                new HashSet<LoginAttribute>());

        for(org.dcache.gplazma.SessionAttribute sessionAttribute:
            gPlazmaLoginReply.getSessionAttributes()) {
            if(sessionAttribute instanceof
                   org.dcache.gplazma.HomeDirectory ) {
                reply.getLoginAttributes().add(
                    new HomeDirectory((String)sessionAttribute.getValue()));
            }
            if(sessionAttribute instanceof
                   org.dcache.gplazma.RootDirectory ) {
                reply.getLoginAttributes().add(
                    new RootDirectory((String)sessionAttribute.getValue()));
            }
            if(sessionAttribute instanceof
                   org.dcache.gplazma.ReadOnly ) {
                reply.getLoginAttributes().add(
                    new ReadOnly((Boolean)sessionAttribute.getValue()));
            }
        }
        return reply;
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

    // map
    // reverse
}