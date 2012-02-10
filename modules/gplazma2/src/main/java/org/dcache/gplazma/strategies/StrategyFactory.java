package org.dcache.gplazma.strategies;

import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;

/**
 * getInstance of StrategyFactory
 * @author timur
 */
public abstract class StrategyFactory {


    private static final String DEFAULT_PROPERTY_NAME =
            "org.dcache.gplazma.strategies.StrategyFactory";
    private static final String DEFAULT_FACTORY =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    /**
     *
     * @return an instance of the
     * StrategyFactory,
     * by default it is
     * org.dcache.gplazma.configuration.parser.DefaultConfigurationParserFactory
     * but this behavior can be overridden with definition of the system property
     * "org.dcache.gplazma.configuration.parser.StrategyFactory"
     */
    public static StrategyFactory getInstance()
            throws FactoryConfigurationException
    {
        try {
            return (StrategyFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
        } catch (ClassNotFoundException cnfe) {
            throw new FactoryConfigurationException("parser factory class not found", cnfe);
        } catch (InstantiationException ie) {
            throw new FactoryConfigurationException("parser factory class is not concrete class", ie);
        } catch (IllegalAccessException iae) {
            throw new FactoryConfigurationException("do not have access to parser factory constructor", iae);
        }
    }


    /**
     *
     * @param factoryClassName
     * @return an instanc of the StrategyFactory specified by the
     *  factoryClassName
     */
    public static StrategyFactory getInstance(String factoryClassName)
            throws FactoryConfigurationException
    {
        try {
            return (StrategyFactory) FactoryFinder.newInstance(factoryClassName);
        } catch (ClassNotFoundException cnfe) {
            throw new FactoryConfigurationException("parser factory class not found", cnfe);
        } catch (InstantiationException ie) {
            throw new FactoryConfigurationException("parser factory class is not concrete class", ie);
        } catch (IllegalAccessException iae) {
            throw new FactoryConfigurationException("do not have access to parser factory constructor", iae);
        }
    }

    public abstract AuthenticationStrategy newAuthenticationStrategy();

    public abstract MappingStrategy newMappingStrategy();

    public abstract AccountStrategy newAccountStrategy();

    public abstract SessionStrategy newSessionStrategy();

    public abstract IdentityStrategy newIdentityStrategy();
}
