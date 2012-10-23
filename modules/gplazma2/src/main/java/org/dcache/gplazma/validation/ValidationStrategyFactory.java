package org.dcache.gplazma.validation;

import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;

/**
 * getInstance of StrategyFactory
 * @author timur
 */
public abstract class ValidationStrategyFactory
{
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
    public static ValidationStrategyFactory getInstance()
            throws FactoryConfigurationException
    {
        try {
            return (ValidationStrategyFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException cnfe) {
            throw new FactoryConfigurationException("configuration error", cnfe);
        }
    }


    /**
     *
     * @param factoryClassName
     * @return an instanc of the ValidationStrategyFactory specified by the
     *  factoryClassName
     */
    public static ValidationStrategyFactory getInstance(String factoryClassName)
            throws FactoryConfigurationException
    {
        try {
            return (ValidationStrategyFactory) FactoryFinder.newInstance(factoryClassName);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException cnfe) {
            throw new FactoryConfigurationException("configuration error", cnfe);
        }
    }

    public abstract ValidationStrategy newValidationStrategy();
}
