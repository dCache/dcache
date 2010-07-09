package org.dcache.gplazma.validation;

import org.dcache.gplazma.configuration.parser.FactoryConfigurationError;
/**
 * getInstance of StrategyFactory
 * @author timur
 */
public abstract class ValidationStrategyFactory {


    private static final String DEFAULT_PROPERTY_NAME =
            "org.dcache.gplazma.validation.ValidationStrategyFactory";
    private static final String DEFAULT_FACTORY =
            "org.dcache.gplazma.validation.DoorValidationStrategyFactory";
    /**
     *
     * @return an instance of the
     * StrategyFactory,
     * by default it is
     * org.dcache.gplazma.validation.DoorValidationStrategyFactory
     * but this behavior can be overridden with definition of the system property
     * "org.dcache.gplazma.validation.ValidationStrategyFactory"
     */
    public static ValidationStrategyFactory getInstance() {
        try {
            return (ValidationStrategyFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
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
     * @return an instanc of the ValidationStrategyFactory specified by the
     *  factoryClassName
     */
    public static ValidationStrategyFactory getInstance(String factoryClassName) {
        try {
            return (ValidationStrategyFactory) FactoryFinder.newInstance(factoryClassName);
        } catch (ClassNotFoundException cnfe) {
            throw new FactoryConfigurationError("configuration error", cnfe);
        } catch (InstantiationException ie) {
            throw new FactoryConfigurationError("configuration error", ie);
        } catch (IllegalAccessException iae) {
            throw new FactoryConfigurationError("configuration error", iae);
        }
    }

    public abstract ValidationStrategy newValidationStrategy();

}
