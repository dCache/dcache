package org.dcache.gplazma;

import static com.google.common.base.Preconditions.checkNotNull;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.security.auth.Subject;

import org.dcache.commons.util.NDC;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import org.dcache.gplazma.configuration.Configuration;
import org.dcache.gplazma.configuration.ConfigurationItem;
import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.configuration.ConfigurationItemType;
import org.dcache.gplazma.configuration.ConfigurationLoadingStrategy;
import org.dcache.gplazma.loader.CachingPluginLoaderDecorator;
import org.dcache.gplazma.loader.PluginLoader;
import org.dcache.gplazma.loader.PluginLoadingException;
import org.dcache.gplazma.loader.XmlResourcePluginLoader;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import org.dcache.gplazma.strategies.AccountStrategy;
import org.dcache.gplazma.strategies.AuthenticationStrategy;
import org.dcache.gplazma.strategies.GPlazmaPluginElement;
import org.dcache.gplazma.strategies.IdentityStrategy;
import org.dcache.gplazma.strategies.MappingStrategy;
import org.dcache.gplazma.strategies.SessionStrategy;
import org.dcache.gplazma.strategies.StrategyFactory;
import org.dcache.gplazma.validation.ValidationStrategy;
import org.dcache.gplazma.validation.ValidationStrategyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GPlazma
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger( GPlazma.class);

    private Properties _globalProperties;
    private boolean _globalPropertiesHaveUpdated = false;

    private PluginLoader pluginLoader;

    private GPlazmaInternalException _lastLoadPluginsProblem;

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>>
            authenticationPluginElements;
    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>>
            mappingPluginElements;
    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>>
            accountPluginElements;
    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>>
            sessionPluginElements;
    private List<GPlazmaPluginElement<GPlazmaIdentityPlugin>>
            identityPluginElements;

    private final ConfigurationLoadingStrategy configurationLoadingStrategy;
    private AuthenticationStrategy authenticationStrategy;
    private MappingStrategy mappingStrategy;
    private AccountStrategy accountStrategy;
    private SessionStrategy sessionStrategy;
    private ValidationStrategy validationStrategy;
    private IdentityStrategy identityStrategy;

    /**
     * @param configurationLoadingStrategy The strategy for loading the plugin configuration.
     * @param properties General configuration for plugins
     */
    public GPlazma(ConfigurationLoadingStrategy configurationLoadingStrategy,
                   Properties properties)
    {
        this.configurationLoadingStrategy = configurationLoadingStrategy;
        _globalProperties = properties;
        try {
            loadPlugins();
        } catch (GPlazmaInternalException e) {
            /* Ignore this error.  Subsequent attempts to use gPlazma will
             * fail with this error.  gPlazma will try to rectify the problem
             * if configuration file is edited.
             */
        }
    }

    public LoginReply login(Subject subject) throws AuthenticationException
    {
        checkNotNull(subject, "subject is null");

        AuthenticationStrategy currentAuthenticationStrategy;
        MappingStrategy currentMappingStrategy;
        AccountStrategy currentAccountStrategy;
        SessionStrategy currentSessionStrategy;

        synchronized (configurationLoadingStrategy) {
            try {
                checkPluginConfig();
            } catch(GPlazmaInternalException e) {
                throw new AuthenticationException("internal gPlazma error: " +
                        e.getMessage());
            }

            currentAuthenticationStrategy = this.authenticationStrategy;
            currentMappingStrategy = this.mappingStrategy;
            currentAccountStrategy = this.accountStrategy;
            currentSessionStrategy = this.sessionStrategy;
        }

        NDC ndc = NDC.cloneNdc();

        Set<Principal> identifiedPrincipals = new HashSet<Principal>();
        identifiedPrincipals.addAll(subject.getPrincipals());
        Set<Principal>  authorizedPrincipals = new HashSet<Principal>();
        Set<Object> attributes = new HashSet<Object>();

        try {
             NDC.push("AUTH");
             LOGGER.debug("phase starts (public: {}, private: {}, principals: {})",
                     new Object[]{subject.getPublicCredentials(),
                     subject.getPrivateCredentials(),identifiedPrincipals});
             currentAuthenticationStrategy.authenticate(
                     subject.getPublicCredentials(),
                     subject.getPrivateCredentials(),
                     identifiedPrincipals);
             NDC.pop();

             NDC.push("MAP");
             LOGGER.debug("phase starts (principals: {})", identifiedPrincipals);
             currentMappingStrategy.map(
                     identifiedPrincipals,
                     authorizedPrincipals);
             NDC.pop();

             NDC.push("ACCOUNT");
             LOGGER.debug("phase starts (principals: {})", authorizedPrincipals);
             currentAccountStrategy.account(
                     authorizedPrincipals);
             NDC.pop();

             NDC.push("SESSION");
             LOGGER.debug("phase starts (principals: {}, attributes: {})",
                     authorizedPrincipals, attributes);
             currentSessionStrategy.session(
                     authorizedPrincipals,
                     attributes);
             NDC.pop();
         } finally {
             NDC.set(ndc);
         }

         LoginReply reply = new LoginReply();
         Subject replySubject = new Subject(
                 false,
                 authorizedPrincipals,
                 subject.getPublicCredentials(),
                 subject.getPrivateCredentials());
         reply.setSubject(replySubject);
         reply.setSessionAttributes(attributes);

         validationStrategy.validate(reply);

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
            return this.identityStrategy;
        }
    }

    private void loadPlugins() throws GPlazmaInternalException
    {
        LOGGER.debug("reloading plugins");

        pluginLoader = new CachingPluginLoaderDecorator(
                XmlResourcePluginLoader.newPluginLoader());
        pluginLoader.init();

        resetPlugins();

        try {
            Configuration configuration = configurationLoadingStrategy.load();
            List<ConfigurationItem> items = configuration.getConfigurationItemList();

            for(ConfigurationItem item : items) {
                String pluginName = item.getPluginName();

                Properties pluginProperties = item.getPluginConfiguration();
                Properties combinedProperties = new Properties(_globalProperties);
                combinedProperties.putAll(pluginProperties);

                GPlazmaPlugin plugin;

                try {
                    plugin = pluginLoader.newPluginByName(pluginName,
                        combinedProperties);
                } catch(PluginLoadingException e) {
                    throw new PluginLoadingException("failed to create "
                            + pluginName + ": " + e.getMessage(), e);
                }

                ConfigurationItemControl control = item.getControl();
                ConfigurationItemType type = item.getType();

                classifyPlugin(type, plugin, pluginName, control);
            }

            initStrategies();
        } catch(GPlazmaInternalException e) {
            LOGGER.error(e.getMessage());
            _lastLoadPluginsProblem = e;
            throw e;
        }


        if(isPreviousLoadPluginsProblematic()) {
            /* FIXME: this should be logged at info level but we want it to
             *        appear in the log file. */
            LOGGER.warn("gPlazma configuration successfully loaded");

            _lastLoadPluginsProblem = null;
        }
    }


    private void resetPlugins()
    {
        authenticationPluginElements = new ArrayList<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>>();
        mappingPluginElements = new ArrayList<GPlazmaPluginElement<GPlazmaMappingPlugin>>();
        accountPluginElements = new ArrayList<GPlazmaPluginElement<GPlazmaAccountPlugin>>();
        sessionPluginElements = new ArrayList<GPlazmaPluginElement<GPlazmaSessionPlugin>>();
        identityPluginElements = new ArrayList<GPlazmaPluginElement<GPlazmaIdentityPlugin>>();
    }


    private void initStrategies() throws FactoryConfigurationException
    {
        StrategyFactory factory = StrategyFactory.getInstance();
        authenticationStrategy = factory.newAuthenticationStrategy();
        authenticationStrategy.setPlugins(authenticationPluginElements);
        mappingStrategy = factory.newMappingStrategy();
        mappingStrategy.setPlugins(mappingPluginElements);
        accountStrategy = factory.newAccountStrategy();
        accountStrategy.setPlugins(accountPluginElements);
        sessionStrategy = factory.newSessionStrategy();
        sessionStrategy.setPlugins(sessionPluginElements);
        identityStrategy = factory.newIdentityStrategy();
        identityStrategy.setPlugins(identityPluginElements);

        ValidationStrategyFactory validationFactory =
                ValidationStrategyFactory.getInstance();
        validationStrategy = validationFactory.newValidationStrategy();
    }

    private void checkPluginConfig() throws GPlazmaInternalException
    {
        if (_globalPropertiesHaveUpdated || configurationLoadingStrategy.hasUpdated()) {
            _globalPropertiesHaveUpdated = false;
            loadPlugins();
        }

        if(isPreviousLoadPluginsProblematic()) {
            throw _lastLoadPluginsProblem;
        }
    }

    private boolean isPreviousLoadPluginsProblematic()
    {
        return _lastLoadPluginsProblem != null;
    }

    private void classifyPlugin( ConfigurationItemType type,
            GPlazmaPlugin plugin, String pluginName,
            ConfigurationItemControl control) throws PluginLoadingException
    {
        if(!type.getType().isAssignableFrom(plugin.getClass())) {
                    throw new PluginLoadingException("plugin " + pluginName +
                            " (java class  " +
                            plugin.getClass().getCanonicalName() +
                            ") does not support being loaded as type " + type );
        }
        switch (type) {
            case AUTHENTICATION:
            {
                storePluginElement(plugin, pluginName, control,
                        authenticationPluginElements);
                break;
            }
            case MAPPING:
            {
                storePluginElement(plugin, pluginName, control,
                        mappingPluginElements);
                break;
            }
            case ACCOUNT:
            {
                storePluginElement(plugin, pluginName, control,
                        accountPluginElements);
                break;
            }
            case SESSION:
            {
                storePluginElement(plugin, pluginName, control,
                        sessionPluginElements);
                break;
            }
            case IDENTITY: {
                storePluginElement(plugin, pluginName, control,
                        identityPluginElements);
                break;
            }
            default:
            {
                throw new PluginLoadingException("unknown plugin type " + type);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends GPlazmaPlugin> void storePluginElement(
            GPlazmaPlugin plugin, String pluginName,
            ConfigurationItemControl control,
            List<GPlazmaPluginElement<T>> pluginElements)
            throws PluginLoadingException
    {
        // we are forced to use unchecked cast here, as the generics do not support
        // instanceof, but we have checked the type before calling storePluginElement
        T authPlugin = (T) plugin;
        GPlazmaPluginElement<T> pluginElement = new GPlazmaPluginElement<T>(authPlugin, pluginName, control);
        pluginElements.add(pluginElement);
    }
}
