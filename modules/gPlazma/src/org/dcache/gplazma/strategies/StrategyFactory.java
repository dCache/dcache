package org.dcache.gplazma.strategies;

import org.dcache.gplazma.configuration.parser.FactoryConfigurationError;
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
    public static StrategyFactory getInstanse() {
        try {
            return (StrategyFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
        } catch (ClassNotFoundException cnfe) {
            throw new FactoryConfigurationError("configuration error", cnfe);
        } catch (InstantiationException ie) {
            throw new FactoryConfigurationError("configuration error", ie);
        } catch (IllegalAccessException iae) {
            throw new FactoryConfigurationError("configuration error", iae);
        }
    }
    /**
     *
     * @param factoryClassName
     * @return an instanc of the StrategyFactory specified by the
     *  factoryClassName
     */
    public static StrategyFactory getInstance(String factoryClassName) {
        try {
            return (StrategyFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
        } catch (ClassNotFoundException cnfe) {
            throw new FactoryConfigurationError("configuration error", cnfe);
        } catch (InstantiationException ie) {
            throw new FactoryConfigurationError("configuration error", ie);
        } catch (IllegalAccessException iae) {
            throw new FactoryConfigurationError("configuration error", iae);
        }
    }

    public abstract AuthenticationStrategy newAuthenticationStrategy();

    public abstract MappingStrategy newMappingStrategy();

    public abstract AccountStrategy newAccountStrategy();

    public abstract SessionStrategy newSessionStrategy();

}
