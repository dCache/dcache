package org.dcache.gplazma;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.lang.reflect.Modifier;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.commons.util.NDC;
import org.dcache.gplazma.configuration.Configuration;
import org.dcache.gplazma.configuration.ConfigurationItem;
import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.configuration.ConfigurationItemType;
import org.dcache.gplazma.configuration.ConfigurationLoadingStrategy;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import org.dcache.gplazma.loader.PluginFactory;
import org.dcache.gplazma.loader.PluginLoader;
import org.dcache.gplazma.loader.PluginLoadingException;
import org.dcache.gplazma.loader.XmlResourcePluginLoader;
import org.dcache.gplazma.monitor.CombinedLoginMonitor;
import org.dcache.gplazma.monitor.LoggingLoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor.Result;
import org.dcache.gplazma.monitor.LoginResult;
import org.dcache.gplazma.monitor.LoginResultPrinter;
import org.dcache.gplazma.monitor.RecordingLoginMonitor;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import org.dcache.gplazma.strategies.AccountStrategy;
import org.dcache.gplazma.strategies.AuthenticationStrategy;
import org.dcache.gplazma.strategies.GPlazmaPluginService;
import org.dcache.gplazma.strategies.IdentityStrategy;
import org.dcache.gplazma.strategies.MappingStrategy;
import org.dcache.gplazma.strategies.SessionStrategy;
import org.dcache.gplazma.strategies.StrategyFactory;
import org.dcache.gplazma.validation.ValidationStrategy;
import org.dcache.gplazma.validation.ValidationStrategyFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.removeIf;

public class GPlazma
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger( GPlazma.class);

    private static final LoginMonitor LOGGING_LOGIN_MONITOR =
            new LoggingLoginMonitor();

    private KnownFailedLogins _failedLogins = new KnownFailedLogins();

    private Properties _globalProperties;
    private boolean _globalPropertiesHaveUpdated;

    private final PluginFactory _customPluginFactory;

    private GPlazmaInternalException _lastLoadPluginsProblem;

    private final ConfigurationLoadingStrategy configurationLoadingStrategy;

    private ValidationStrategy validationStrategy;

    private Setup setup;

    /**
     * Storage class for failed login attempts.  This allows gPlazma to
     * refrain from filling up log files should a client attempt multiple
     * login attempts that all fail.  We must be careful about how we store
     * the incoming Subjects.
     *
     * This class is thread-safe.
     */
    private static class KnownFailedLogins
    {
        private final Set<Subject> _failedLogins =
                new CopyOnWriteArraySet<>();

        /**
         * In general, this class does not store any private credential since
         * doing this would be against the general security advise of
         * only storing sensitive material (e.g., passwords) for as long as
         * is necessary.
         *
         * However, the class may wish to distinguish between different login
         * attempts based information contained in private credentials.  To
         * support this, principals may be added that contain
         * non-sensitive information contained in a private credential.
         */
        private static void addPrincipalsForPrivateCredentials(
                Set<Principal> principals, Set<Object> privateCredentials)
        {
            PasswordCredential password =
                    getFirst(Iterables.filter(privateCredentials,
                    PasswordCredential.class), null);

            if(password != null) {
                Principal loginName =
                        new LoginNamePrincipal(password.getUsername());
                principals.add(loginName);
            }
        }

        /**
         * Calculate the storage Subject, given an incoming subject.  The
         * storage subject is similar to the supplied Subject but has sensitive
         * material (like passwords) removed and is location agnostic
         * (e.g., any Origin principals are removed).
         */
        private static Subject storageSubjectFor(Subject subject)
        {
            Subject storage = new Subject();

            storage.getPublicCredentials().addAll(subject.getPublicCredentials());

            /*
             * Do not store any private credentials as doing so would be a
             * security risk.
             */

            Collection<Principal> allExceptOrigin =
                    filter(subject.getPrincipals(), not(instanceOf(Origin.class)));

            storage.getPrincipals().addAll(allExceptOrigin);

            addPrincipalsForPrivateCredentials(storage.getPrincipals(),
                    subject.getPrivateCredentials());

            return storage;
        }

        private boolean has(Subject subject)
        {
            Subject storage = storageSubjectFor(subject);
            return _failedLogins.contains(storage);
        }

        private void add(Subject subject)
        {
            Subject storage = storageSubjectFor(subject);
            _failedLogins.add(storage);
        }

        private void remove(Subject subject)
        {
            Subject storage = storageSubjectFor(subject);
            _failedLogins.remove(storage);
        }

        private void clear()
        {
            _failedLogins.clear();
        }
    }



    /**
     * @param configurationLoadingStrategy The strategy for loading the plugin configuration.
     * @param properties General configuration for plugins
     */
    public GPlazma(ConfigurationLoadingStrategy configurationLoadingStrategy,
                   Properties properties)
    {
        this(configurationLoadingStrategy, properties, null);
    }

    /**
     * @param configurationLoadingStrategy The strategy for loading the plugin configuration.
     * @param properties General configuration for plugins
     * @param factory Custom PluginFactory to allow customisation of plugins
     */
    public GPlazma(ConfigurationLoadingStrategy configurationLoadingStrategy,
                   Properties properties, PluginFactory factory)
    {
        this.configurationLoadingStrategy = configurationLoadingStrategy;
        _globalProperties = properties;
        _customPluginFactory = factory;
        try {
            reload();
        } catch (GPlazmaInternalException e) {
            /* Ignore this error.  Subsequent attempts to use gPlazma will
             * fail with the same error.  gPlazma will try to rectify the
             * problem if configuration file is edited.
             */
        }
    }

    public void shutdown()
    {
        Setup setup = this.setup;
        if (setup != null) {
            setup.stop();
        }
    }

    public LoginReply login(Subject subject) throws AuthenticationException
    {
        RecordingLoginMonitor record = new RecordingLoginMonitor();
        LoginMonitor combined = CombinedLoginMonitor.of(record,
                LOGGING_LOGIN_MONITOR);

        try {
            LoginReply reply = login(subject, combined);
            _failedLogins.remove(subject);
            return reply;
        } catch(AuthenticationException e) {
            if(!_failedLogins.has(subject)) {
                _failedLogins.add(subject);

                LoginResult result = record.getResult();

                if(result.hasStarted()) {
                    LoginResultPrinter printer = new LoginResultPrinter(result);
                    LOGGER.warn("Login attempt failed; " +
                            "detailed explanation follows:\n{}",
                            printer.print());
                } else {
                    LOGGER.warn("Login attempt failed: {}", e.getMessage());
                }
            }
            throw e;
        }
    }

    public LoginReply login(Subject subject, LoginMonitor monitor)
            throws AuthenticationException
    {
        checkNotNull(subject, "subject is null");

        Setup setup;

        synchronized (configurationLoadingStrategy) {
            try {
                checkPluginConfig();
            } catch(GPlazmaInternalException e) {
                throw new AuthenticationException("internal gPlazma error: " + e.getMessage());
            }

            setup = this.setup;
        }

        Set<Principal> principals = new HashSet<>();
        setup.doAuthPhase(monitor, subject, principals);
        setup.doMapPhase(monitor, principals);
        setup.doAccountPhase(monitor, principals);
        Set<Object> attributes = setup.doSessionPhase(monitor, principals);
        removeIf(principals, p -> !isPublic(p));

        return buildReply(monitor, subject, principals, attributes);
    }

    private static boolean isPublic(Principal p)
    {
        return Modifier.isPublic(p.getClass().getModifiers());
    }

    public LoginReply buildReply(LoginMonitor monitor, Subject originalSubject,
            Set<Principal> principals, Set<Object> attributes)
            throws AuthenticationException
    {
        Set<Object> publicCredentials = originalSubject.getPublicCredentials();
        Set<Object> privateCredentials = originalSubject.getPrivateCredentials();

        LoginReply reply = new LoginReply();

        Subject subject = new Subject(false, principals, publicCredentials,
                privateCredentials);
        reply.setSubject(subject);
        reply.setSessionAttributes(attributes);

        Result result = Result.FAIL;
        String error = null;
        NDC.push("VALIDATION");
        try {
            validationStrategy.validate(reply);
            result = Result.SUCCESS;
        } catch(AuthenticationException e) {
            error = e.getMessage();
            throw e;
        } finally {
            NDC.pop();
            monitor.validationResult(result, error);
        }

        return reply;
    }


    public Principal map(Principal principal) throws NoSuchPrincipalException
    {
        try {
            return getIdentityStrategy().map(principal);
        } catch (GPlazmaInternalException e) {
            throw new NoSuchPrincipalException("internal gPlazma error: " +
                    e.getMessage());
        }
    }

    public Set<Principal> reverseMap(Principal principal)
            throws NoSuchPrincipalException
    {
        try {
            return getIdentityStrategy().reverseMap(principal);
        } catch (GPlazmaInternalException e) {
            throw new NoSuchPrincipalException("internal gPlazma error: " +
                    e.getMessage());
        }
    }

    private IdentityStrategy getIdentityStrategy() throws GPlazmaInternalException
    {
        synchronized (configurationLoadingStrategy) {
            checkPluginConfig();
            return setup.identityStrategy;
        }
    }

    private void reload() throws GPlazmaInternalException
    {
        LOGGER.debug("reloading plugins");

        try {
            validationStrategy = ValidationStrategyFactory.getInstance().newValidationStrategy();

            Setup newSetup = buildSetup();
            try {
                newSetup.start();
            } catch (GPlazmaInternalException e) {
                newSetup.stop();
                throw e;
            }

            Setup oldSetup = this.setup;
            if (oldSetup != null) {
                oldSetup.stop();
            }
            this.setup = newSetup;

            if(isPreviousLoadPluginsProblematic()) {
                /* FIXME: this should be logged at info level but we want it to
                 *        appear in the log file. */
                LOGGER.warn("gPlazma configuration successfully loaded");
                _lastLoadPluginsProblem = null;
            }
        } catch(GPlazmaInternalException e) {
            LOGGER.error(e.getMessage());
            _lastLoadPluginsProblem = e;
            throw e;
        }
    }

    private Setup buildSetup() throws GPlazmaInternalException
    {
        PluginLoader pluginLoader = XmlResourcePluginLoader.newPluginLoader();
        if (_customPluginFactory != null) {
            pluginLoader.setPluginFactory(_customPluginFactory);
        }
        pluginLoader.init();

        SetupBuilder setup = new SetupBuilder();
        Configuration configuration = configurationLoadingStrategy.load();
        List<ConfigurationItem> items = configuration.getConfigurationItemList();

        for (ConfigurationItem item : items) {
            String pluginName = item.getPluginName();

            Properties pluginProperties = item.getPluginConfiguration();
            Properties combinedProperties = new Properties(_globalProperties);
            combinedProperties.putAll(pluginProperties);

            GPlazmaPlugin plugin;

            try {
                plugin = pluginLoader.newPluginByName(pluginName, combinedProperties);
            } catch (PluginLoadingException e) {
                throw new PluginLoadingException("failed to create " + pluginName + ": " + e.getMessage(), e);
            }

            ConfigurationItemControl control = item.getControl();
            ConfigurationItemType type = item.getType();

            setup.add(type, plugin, pluginName, control);
        }
        return setup.build();
    }

    private void checkPluginConfig() throws GPlazmaInternalException
    {
        if (_globalPropertiesHaveUpdated || configurationLoadingStrategy.hasUpdated()) {
            _globalPropertiesHaveUpdated = false;
            _failedLogins.clear();
            reload();
        }

        if(isPreviousLoadPluginsProblematic()) {
            throw _lastLoadPluginsProblem;
        }
    }

    private boolean isPreviousLoadPluginsProblematic()
    {
        return _lastLoadPluginsProblem != null;
    }

    /**
     * Container for plugins of a particular type.
     */
    private static class Plugins<T extends GPlazmaPlugin> extends ArrayList<GPlazmaPluginService<T>>
    {
        private static final long serialVersionUID = 3696582098049455967L;

        void add(T plugin, String pluginName, ConfigurationItemControl control)
        {
            add(new GPlazmaPluginService<>(plugin, pluginName, control));
        }
    }

    private static class SetupBuilder
    {
        private final Plugins<GPlazmaAuthenticationPlugin> authenticationPlugins = new Plugins<>();
        private final Plugins<GPlazmaMappingPlugin> mappingPlugins = new Plugins<>();
        private final Plugins<GPlazmaAccountPlugin> accountPlugins = new Plugins<>();
        private final Plugins<GPlazmaSessionPlugin> sessionPlugins = new Plugins<>();
        private final Plugins<GPlazmaIdentityPlugin> identityPlugins = new Plugins<>();

        void add(ConfigurationItemType type, GPlazmaPlugin plugin, String pluginName, ConfigurationItemControl control)
                throws PluginLoadingException
        {
            if (!type.getType().isAssignableFrom(plugin.getClass())) {
                throw new PluginLoadingException("plugin " + pluginName + " (java class  " +
                                                 plugin.getClass().getCanonicalName() +
                                                 ") does not support being loaded as type " + type);
            }
            switch (type) {
            case AUTHENTICATION:
                authenticationPlugins.add((GPlazmaAuthenticationPlugin) plugin, pluginName, control);
                break;
            case MAPPING:
                mappingPlugins.add((GPlazmaMappingPlugin) plugin, pluginName, control);
                break;
            case ACCOUNT:
                accountPlugins.add((GPlazmaAccountPlugin) plugin, pluginName, control);
                break;
            case SESSION:
                sessionPlugins.add((GPlazmaSessionPlugin) plugin, pluginName, control);
                break;
            case IDENTITY:
                identityPlugins.add((GPlazmaIdentityPlugin) plugin, pluginName, control);
                break;
            default:
                throw new PluginLoadingException("unknown plugin type " + type);
            }
        }

        Setup build() throws GPlazmaInternalException
        {
            return new Setup(authenticationPlugins, mappingPlugins, accountPlugins, sessionPlugins, identityPlugins);
        }
    }

    /**
     * A particular gPlazma setup with plugins grouped into several phases.
     */
    private static class Setup extends ServiceManager.Listener
    {
        private final AuthenticationStrategy authStrategy;
        private final MappingStrategy mapStrategy;
        private final AccountStrategy accountStrategy;
        private final SessionStrategy sessionStrategy;
        private final IdentityStrategy identityStrategy;

        private final ServiceManager manager;
        private Throwable failure;

        Setup(Plugins<GPlazmaAuthenticationPlugin> authenticationPlugins,
              Plugins<GPlazmaMappingPlugin> mappingPlugins, Plugins<GPlazmaAccountPlugin> accountPlugins,
              Plugins<GPlazmaSessionPlugin> sessionPlugins, Plugins<GPlazmaIdentityPlugin> identityPlugins)
                throws FactoryConfigurationException
        {
            StrategyFactory factory = StrategyFactory.getInstance();
            authStrategy = factory.newAuthenticationStrategy();
            mapStrategy = factory.newMappingStrategy();
            accountStrategy = factory.newAccountStrategy();
            sessionStrategy = factory.newSessionStrategy();
            identityStrategy = factory.newIdentityStrategy();
            authStrategy.setPlugins(authenticationPlugins);
            mapStrategy.setPlugins(mappingPlugins);
            accountStrategy.setPlugins(accountPlugins);
            sessionStrategy.setPlugins(sessionPlugins);
            identityStrategy.setPlugins(identityPlugins);
            manager = new ServiceManager(
                    concat(authenticationPlugins, mappingPlugins, accountPlugins, sessionPlugins, identityPlugins));
            manager.addListener(this);
        }

        @Override
        public void failure(Service service)
        {
            failure = service.failureCause();
        }

        void start() throws GPlazmaInternalException
        {
            try {
                manager.startAsync().awaitHealthy();
            } catch (IllegalStateException e) {
                if (failure != null) {
                    throw new PluginLoadingException(failure.getMessage(), failure);
                }
                throw new PluginLoadingException(e.getMessage(), e);
            }
        }

        void stop()
        {
            manager.stopAsync().awaitStopped();
        }

        void doAuthPhase(LoginMonitor monitor, Subject subject, Set<Principal> principals)
                throws AuthenticationException
        {
            Set<Object> publicCredentials = subject.getPublicCredentials();
            Set<Object> privateCredentials = subject.getPrivateCredentials();

            principals.addAll(subject.getPrincipals());

            NDC.push("AUTH");
            Result result = Result.FAIL;
            try {
                monitor.authBegins(publicCredentials, privateCredentials, principals);
                authStrategy.authenticate(monitor, publicCredentials, privateCredentials, principals);
                result = Result.SUCCESS;
            } finally {
                NDC.pop();
                monitor.authEnds(principals, result);
            }
        }


        void doMapPhase(LoginMonitor monitor, Set<Principal> principals)
                throws AuthenticationException
        {
            NDC.push("MAP");
            Result result = Result.FAIL;
            try {
                monitor.mapBegins(principals);
                mapStrategy.map(monitor, principals);
                result = Result.SUCCESS;
            } finally {
                NDC.pop();
                monitor.mapEnds(principals, result);
            }
        }

        void doAccountPhase(LoginMonitor monitor, Set<Principal> principals)
                throws AuthenticationException
        {
            NDC.push("ACCOUNT");
            Result result = Result.FAIL;
            try {
                monitor.accountBegins(principals);
                accountStrategy.account(monitor, principals);
                result = Result.SUCCESS;
            } finally {
                NDC.pop();
                monitor.accountEnds(principals, result);
            }
        }

        Set<Object> doSessionPhase(LoginMonitor monitor, Set<Principal> principals)
                throws AuthenticationException
        {
            Set<Object> attributes = new HashSet<>();

            NDC.push("SESSION");
            Result result = Result.FAIL;
            try {
                monitor.sessionBegins(principals);
                sessionStrategy.session(monitor, principals, attributes);
                result = Result.SUCCESS;
            } finally {
                NDC.pop();
                monitor.sessionEnds(principals, attributes, result);
            }

            return attributes;
        }
    }
}
