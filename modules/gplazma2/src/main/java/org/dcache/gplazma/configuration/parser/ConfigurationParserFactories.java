package org.dcache.gplazma.configuration.parser;

import java.util.function.Supplier;

/**
 * Class containing utility methods for creating ConfigurationParser objects.
 */
public final class ConfigurationParserFactories {

    private static final String DEFAULT_PROPERTY_NAME =
          "org.dcache.gplazma.configuration.parser.ConfigurationParserFactory";
    private static final String DEFAULT_FACTORY =
          "org.dcache.gplazma.configuration.parser.PAMStyleConfigurationParserFactory";

    private ConfigurationParserFactories() {} // Prevent instantiation.

    /**
     * Provide a supplier of ConfigurationParser objects.  By default, the
     * supplier will be {@link PAMStyleConfigurationParserFactory}; however,
     * but this behaviour may be overridden via the system property
     * {@literal org.dcache.gplazma.configuration.parser.ConfigurationParserFactory}
     * @return a supplier of ConfigurationParser objects.
     * @throws FactoryConfigurationException if there was a problem creating the supplier
     */
    public static Supplier<ConfigurationParser> getInstance()
          throws FactoryConfigurationException {
        try {
            return (Supplier<ConfigurationParser>) FactoryFinder.find(DEFAULT_PROPERTY_NAME,
                  DEFAULT_FACTORY);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException cnfe) {
            throw new FactoryConfigurationException("configuration error: " + cnfe,
                    cnfe);
        }
    }
}
