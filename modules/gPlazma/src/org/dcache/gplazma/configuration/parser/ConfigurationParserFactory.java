/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.gplazma.configuration.parser;
/**
 * getInstance of ConfigurationParserFactory
 * @author timur
 */
public abstract class ConfigurationParserFactory {


    private static final String DEFAULT_PROPERTY_NAME =
            "org.dcache.gplazma.configuration.parser.ConfigurationParserFactory";
    private static final String DEFAULT_FACTORY =
            "org.dcache.gplazma.configuration.parser.PAMStyleConfigurationParserFactory";
    /**
     *
     * @return an instance of the
     * ConfigurationParserFactory,
     * by default it is
     * org.dcache.gplazma.configuration.parser.DefaultConfigurationParserFactory
     * but this behavior can be overridden with definition of the system property
     * "org.dcache.gplazma.configuration.parser.ConfigurationParserFactory"
     */
    public static ConfigurationParserFactory getInstanse() {
        try {
            return (ConfigurationParserFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
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
     * @return an instanc of the ConfigurationParserFactory specified by the
     *  factoryClassName
     */
    public static ConfigurationParserFactory getInstance(String factoryClassName) {
        try {
            return (ConfigurationParserFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
        } catch (ClassNotFoundException cnfe) {
            throw new FactoryConfigurationError("configuration error", cnfe);
        } catch (InstantiationException ie) {
            throw new FactoryConfigurationError("configuration error", ie);
        } catch (IllegalAccessException iae) {
            throw new FactoryConfigurationError("configuration error", iae);
        }
    }
    public abstract ConfigurationParser newConfigurationParser();


}
