package org.dcache.commons.plot.renderer;

import org.dcache.commons.plot.FactoryConfigurationError;

public abstract class PlotRendererFactory {


    private static final String DEFAULT_PROPERTY_NAME =
            "org.dcache.commons.plot.renderer.PlotRendererFactory";
    private static final String DEFAULT_FACTORY =
            "org.dcache.commons.plot.renderer.SpringRendererFactory";
    /**
     *
     * @return an instance of the
     * ConfigurationParserFactory,
     * by default it is
     * org.dcache.gplazma.configuration.parser.DefaultConfigurationParserFactory
     * but this behavior can be overridden with definition of the system property
     * "org.dcache.gplazma.configuration.parser.ConfigurationParserFactory"
     */
    public static PlotRendererFactory getInstance() {
        try {
            return (PlotRendererFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
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
    public static PlotRendererFactory getInstance(String factoryClassName) {
        try {
            return (PlotRendererFactory) FactoryFinder.newInstance(factoryClassName);
        } catch (ClassNotFoundException cnfe) {
            throw new FactoryConfigurationError("configuration error", cnfe);
        } catch (InstantiationException ie) {
            throw new FactoryConfigurationError("configuration error", ie);
        } catch (IllegalAccessException iae) {
            throw new FactoryConfigurationError("configuration error", iae);
        }
    }

    public abstract Renderer getPlotRenderer(PlotOutputType plotOutputType);

}
