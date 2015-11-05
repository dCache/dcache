package org.dcache.auth;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.File;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.GPlazma;
import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.gplazma.configuration.ConfigurationLoadingStrategy;
import org.dcache.gplazma.configuration.FromFileConfigurationLoadingStrategy;
import org.dcache.gplazma.loader.DcacheAwarePluginFactory;
import org.dcache.gplazma.loader.PluginFactory;
import org.dcache.gplazma.monitor.LoginResult;
import org.dcache.gplazma.monitor.LoginResultPrinter;
import org.dcache.gplazma.monitor.RecordingLoginMonitor;
import org.dcache.util.Args;

/**
 * A LoginStrategy that wraps a org.dcache.gplazma.GPlazma
 *
 */
public class Gplazma2LoginStrategy
    implements LoginStrategy, EnvironmentAware, CellCommandListener
{
    private String _configurationFile;
    private GPlazma _gplazma;
    private Map<String,Object> _environment = Collections.emptyMap();
    private PluginFactory _factory;

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

    @Required
    public void setNameSpace(NameSpaceProvider namespace)
    {
        _factory = new DcacheAwarePluginFactory(namespace);
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
            new GPlazma(configuration, getEnvironmentAsProperties(), _factory);
    }

    static LoginReply
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
            // We deliberately hide the reason why the login failed from the
            // rest of dCache.  This is to prevent a brute-force attack
            // discovering whether certain user accounts exist.
            throw new PermissionDeniedCacheException("login failed");
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

    public static final String fh_explain_login =
            "This command runs a test login with the supplied principals\n" +
            "The result is tracked and an explanation is provided of how \n" +
            "the result was obtained.\n";
    public static final String hh_explain_login = "<principal> [<principal> ...] # explain the result of login";
    public String ac_explain_login_$_1_99(Args args)
    {
        Subject subject = Subjects.subjectFromArgs(args.getArguments());
        RecordingLoginMonitor monitor = new RecordingLoginMonitor();
        try {
            _gplazma.login(subject, monitor);
        } catch (AuthenticationException e) {
            // ignore exception: we'll show this in the explanation.
        }

        LoginResult result = monitor.getResult();
        LoginResultPrinter printer = new LoginResultPrinter(result);
        return printer.print();
    }
}
