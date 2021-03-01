package org.dcache.auth;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.File;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.PrefixRestriction;
import org.dcache.auth.attributes.RootDirectory;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(Gplazma2LoginStrategy.class);

    private String _configurationFile;
    private GPlazma _gplazma;
    private Map<String,Object> _environment = Collections.emptyMap();
    private PluginFactory _factory;
    private Function<FsPath, PrefixRestriction> _createPrefixRestriction;

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
        Replaceable replaceable = name -> Objects.toString(_environment.get(name), null);

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

    public void shutdown()
    {
        if (_gplazma != null) {
            _gplazma.shutdown();
        }
    }

    private LoginReply convertLoginReply(org.dcache.gplazma.LoginReply gPlazmaLoginReply)
    {
        Set<Object> sessionAttributes =
            gPlazmaLoginReply.getSessionAttributes();
        Set<LoginAttribute> loginAttributes =
                sessionAttributes.stream()
                        .filter(LoginAttribute.class::isInstance)
                        .map(LoginAttribute.class::cast)
                        .collect(Collectors.toSet());

        sessionAttributes.stream()
                .filter(RootDirectory.class::isInstance)
                .map(RootDirectory.class::cast)
                .filter(att -> !att.getRoot().equals("/"))
                .map(att -> FsPath.create(att.getRoot()))
                .map(_createPrefixRestriction)
                .forEach(loginAttributes::add);

        // Filter IGTFStatusPrincipal and IGTFPolicyPrincipal until no longer
        // need backwards compatibility with dCache v3.0 pools
        Subject replyUser = new Subject();
        Subject loggedInUser = gPlazmaLoginReply.getSubject();
        replyUser.getPublicCredentials().addAll(loggedInUser.getPublicCredentials());
        replyUser.getPrivateCredentials().addAll(loggedInUser.getPrivateCredentials());
        Set<Principal> replyPrincipals = replyUser.getPrincipals();
        loggedInUser.getPrincipals().stream()
                .filter(p -> !(p instanceof IGTFStatusPrincipal))
                .filter(p -> !(p instanceof IGTFPolicyPrincipal))
                .forEach(replyPrincipals::add);
        return new LoginReply(replyUser, loginAttributes);
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException
    {
        try {
            return convertLoginReply(_gplazma.login(subject));
        } catch (AuthenticationException e) {
            LOGGER.info("Login failed: {}", e.getMessage());
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
            "the result was obtained.\n\n" +
            "Examples:\n" +
            "  explain login \"dn:/C=DE/O=GermanGrid/OU=DESY/CN=testUser\" fqan:/test\n" +
            "  explain login user:testuser\n";
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

    public void setUploadPath(String s)
    {
        if (Strings.isNullOrEmpty(s) || !s.startsWith("/")) {
            _createPrefixRestriction = path -> new PrefixRestriction(path);
        } else {
            FsPath uploadPath = FsPath.create(s);
            _createPrefixRestriction = path -> new PrefixRestriction(path, uploadPath);
        }
    }
}
