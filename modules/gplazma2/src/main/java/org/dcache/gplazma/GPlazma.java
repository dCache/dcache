package org.dcache.gplazma;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.removeIf;
import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import java.lang.reflect.Modifier;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
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
import org.dcache.util.AppendOnlySet;
import org.dcache.util.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GPlazma {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(GPlazma.class);

    private static final LoginMonitor LOGGING_LOGIN_MONITOR =
          new LoggingLoginMonitor();

    private Properties _globalProperties;
    private boolean _globalPropertiesHaveUpdated;

    private final PluginFactory _customPluginFactory;

    private GPlazmaInternalException _lastLoadPluginsProblem;

    private final ConfigurationLoadingStrategy configurationLoadingStrategy;

    private ValidationStrategy validationStrategy;

    private Setup setup;

    private final List<LoginObserver> loginObservers = new CopyOnWriteArrayList();
    private final List<ReloadObserver> reloadObservers = new CopyOnWriteArrayList();

    /**
     * @param configurationLoadingStrategy The strategy for loading the plugin configuration.
     * @param properties                   General configuration for plugins
     */
    public GPlazma(ConfigurationLoadingStrategy configurationLoadingStrategy,
          Properties properties) {
        this(configurationLoadingStrategy, properties, null);
    }

    /**
     * @param configurationLoadingStrategy The strategy for loading the plugin configuration.
     * @param properties                   General configuration for plugins
     * @param factory                      Custom PluginFactory to allow customisation of plugins
     */
    public GPlazma(ConfigurationLoadingStrategy configurationLoadingStrategy,
          Properties properties, PluginFactory factory) {
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

    public void shutdown() {
        Setup setup = this.setup;
        if (setup != null) {
            setup.stop();
        }
    }

    public void setObservers(Iterable<Object> observers) {
        loginObservers.clear();
        reloadObservers.clear();

        for (Object observer : observers) {
            if (observer instanceof LoginObserver) {
                loginObservers.add((LoginObserver)observer);
            }

            if (observer instanceof ReloadObserver) {
                reloadObservers.add((ReloadObserver)observer);
            }
        }
    }

    public LoginReply login(Subject subject) throws AuthenticationException {
        RecordingLoginMonitor record = new RecordingLoginMonitor();
        LoginResult result = record.getResult();
        LoginMonitor combined = CombinedLoginMonitor.of(record,
              LOGGING_LOGIN_MONITOR);

        try {
            LoginReply reply = login(subject, combined);
            loginObservers.forEach(o -> o.accept(result));
            return reply;
        } catch (AuthenticationException e) {
            loginObservers.forEach(o -> o.accept(result));
            throw e;
        }
    }

    public LoginReply login(Subject subject, LoginMonitor monitor)
          throws AuthenticationException {
        requireNonNull(subject, "subject is null");

        Setup setup;

        synchronized (configurationLoadingStrategy) {
            try {
                checkPluginConfig();
            } catch (GPlazmaInternalException e) {
                throw new AuthenticationException("internal gPlazma error: " + e.getMessage());
            }

            setup = this.setup;
        }

        Set<Principal> principals = new HashSet<>();
        Set<Restriction> restrictions = new HashSet<>();
        setup.doAuthPhase(monitor, subject, principals, new AppendOnlySet(restrictions));
        setup.doMapPhase(monitor, principals);
        setup.doAccountPhase(monitor, principals);
        Set<Object> attributes = setup.doSessionPhase(monitor, principals);
        attributes.addAll(restrictions);
        removeIf(principals, p -> !isPublic(p));

        return buildReply(monitor, subject, principals, attributes);
    }

    private static boolean isPublic(Principal p) {
        return Modifier.isPublic(p.getClass().getModifiers());
    }

    public LoginReply buildReply(LoginMonitor monitor, Subject originalSubject,
          Set<Principal> principals, Set<Object> attributes)
          throws AuthenticationException {
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
        } catch (AuthenticationException e) {
            error = e.getMessage();
            throw e;
        } finally {
            NDC.pop();
            monitor.validationResult(result, error);
        }

        return reply;
    }


    public Principal map(Principal principal) throws NoSuchPrincipalException {
        try {
            return getIdentityStrategy().map(principal);
        } catch (GPlazmaInternalException e) {
            throw new NoSuchPrincipalException("internal gPlazma error: " +
                  e.getMessage());
        }
    }

    public Set<Principal> reverseMap(Principal principal)
          throws NoSuchPrincipalException {
        try {
            return getIdentityStrategy().reverseMap(principal);
        } catch (GPlazmaInternalException e) {
            throw new NoSuchPrincipalException("internal gPlazma error: " +
                  e.getMessage());
        }
    }

    private IdentityStrategy getIdentityStrategy() throws GPlazmaInternalException {
        synchronized (configurationLoadingStrategy) {
            checkPluginConfig();
            return setup.identityStrategy;
        }
    }

    private void reload() throws GPlazmaInternalException {
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

            if (isPreviousLoadPluginsProblematic()) {
                /* FIXME: this should be logged at info level but we want it to
                 *        appear in the log file. */
                LOGGER.warn("gPlazma configuration successfully loaded");
                _lastLoadPluginsProblem = null;
            }
        } catch (GPlazmaInternalException e) {
            LOGGER.error(e.getMessage());
            _lastLoadPluginsProblem = e;
            throw e;
        }
    }

    private Setup buildSetup() throws GPlazmaInternalException {
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
                throw new PluginLoadingException(
                      "failed to create " + pluginName + ": " + e.getMessage(), e);
            }

            ConfigurationItemControl control = item.getControl();
            ConfigurationItemType type = item.getType();

            setup.add(type, plugin, pluginName, control);
        }
        return setup.build();
    }

    private void checkPluginConfig() throws GPlazmaInternalException {
        if (_globalPropertiesHaveUpdated || configurationLoadingStrategy.hasUpdated()) {
            _globalPropertiesHaveUpdated = false;
            reload();
            reloadObservers.forEach(ReloadObserver::configReloaded);
        }

        if (isPreviousLoadPluginsProblematic()) {
            throw _lastLoadPluginsProblem;
        }
    }

    private boolean isPreviousLoadPluginsProblematic() {
        return _lastLoadPluginsProblem != null;
    }

    /**
     * Container for plugins of a particular type.
     */
    private static class Plugins<T extends GPlazmaPlugin> extends
          ArrayList<GPlazmaPluginService<T>> {

        private static final long serialVersionUID = 3696582098049455967L;

        void add(T plugin, String pluginName, ConfigurationItemControl control) {
            add(new GPlazmaPluginService<>(plugin, pluginName, control));
        }
    }

    private static class SetupBuilder {

        private final Plugins<GPlazmaAuthenticationPlugin> authenticationPlugins = new Plugins<>();
        private final Plugins<GPlazmaMappingPlugin> mappingPlugins = new Plugins<>();
        private final Plugins<GPlazmaAccountPlugin> accountPlugins = new Plugins<>();
        private final Plugins<GPlazmaSessionPlugin> sessionPlugins = new Plugins<>();
        private final Plugins<GPlazmaIdentityPlugin> identityPlugins = new Plugins<>();

        void add(ConfigurationItemType type, GPlazmaPlugin plugin, String pluginName,
              ConfigurationItemControl control)
              throws PluginLoadingException {
            if (!type.getType().isAssignableFrom(plugin.getClass())) {
                throw new PluginLoadingException("plugin " + pluginName + " (java class  " +
                      plugin.getClass().getCanonicalName() +
                      ") does not support being loaded as type " + type);
            }
            switch (type) {
                case AUTHENTICATION:
                    authenticationPlugins.add((GPlazmaAuthenticationPlugin) plugin, pluginName,
                          control);
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

        Setup build() throws GPlazmaInternalException {
            return new Setup(authenticationPlugins, mappingPlugins, accountPlugins, sessionPlugins,
                  identityPlugins);
        }
    }

    /**
     * A particular gPlazma setup with plugins grouped into several phases.
     */
    private static class Setup extends ServiceManager.Listener {

        private final AuthenticationStrategy authStrategy;
        private final MappingStrategy mapStrategy;
        private final AccountStrategy accountStrategy;
        private final SessionStrategy sessionStrategy;
        private final IdentityStrategy identityStrategy;

        private final ServiceManager manager;
        private Throwable failure;

        Setup(Plugins<GPlazmaAuthenticationPlugin> authenticationPlugins,
              Plugins<GPlazmaMappingPlugin> mappingPlugins,
              Plugins<GPlazmaAccountPlugin> accountPlugins,
              Plugins<GPlazmaSessionPlugin> sessionPlugins,
              Plugins<GPlazmaIdentityPlugin> identityPlugins)
              throws FactoryConfigurationException {
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
                  concat(authenticationPlugins, mappingPlugins, accountPlugins, sessionPlugins,
                        identityPlugins));
            manager.addListener(this);
        }

        @Override
        public void failure(Service service) {
            failure = service.failureCause();
        }

        void start() throws GPlazmaInternalException {
            try {
                manager.startAsync().awaitHealthy();
            } catch (IllegalStateException e) {
                if (failure != null) {
                    throw new PluginLoadingException(failure.getMessage(), failure);
                }
                throw new PluginLoadingException(e.getMessage(), e);
            }
        }

        void stop() {
            manager.stopAsync().awaitStopped();
        }

        void doAuthPhase(LoginMonitor monitor, Subject subject, Set<Principal> principals,
              Set<Restriction> restrictionStore) throws AuthenticationException {
            Set<Object> publicCredentials = subject.getPublicCredentials();
            Set<Object> privateCredentials = subject.getPrivateCredentials();

            principals.addAll(subject.getPrincipals());

            NDC.push("AUTH");
            Result result = Result.FAIL;
            try {
                monitor.authBegins(publicCredentials, privateCredentials, principals);
                authStrategy.authenticate(monitor, publicCredentials, privateCredentials,
                      principals, restrictionStore);
                result = Result.SUCCESS;
            } finally {
                NDC.pop();
                monitor.authEnds(principals, result);
            }
        }


        void doMapPhase(LoginMonitor monitor, Set<Principal> principals)
              throws AuthenticationException {
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
              throws AuthenticationException {
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
              throws AuthenticationException {
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
