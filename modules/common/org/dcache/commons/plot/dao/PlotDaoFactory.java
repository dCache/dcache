package org.dcache.commons.plot.dao;

import org.dcache.commons.plot.FactoryConfigurationError;
import org.dcache.commons.plot.ParamPlotType;

public abstract class PlotDaoFactory {

    private static final String DEFAULT_PROPERTY_NAME =
            "org.dcache.commons.plot.dao.PlotDaoFactory";
    private static final String DEFAULT_FACTORY =
            "org.dcache.commons.plot.dao.SpringPlotDaoFactory";

    /**
     *
     * @return an instance of the
     * ConfigurationParserFactory,
     * by default it is
     */
    public static PlotDaoFactory getInstance() {
        try {
            return (PlotDaoFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
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
     * @return an instance of the ConfigurationParserFactory specified by the
     *  factoryClassName
     */
    public static PlotDaoFactory getInstance(String factoryClassName) {
        try {
            return (PlotDaoFactory) FactoryFinder.find(DEFAULT_PROPERTY_NAME, DEFAULT_FACTORY);
        } catch (ClassNotFoundException cnfe) {
            throw new FactoryConfigurationError("configuration error", cnfe);
        } catch (InstantiationException ie) {
            throw new FactoryConfigurationError("configuration error", ie);
        } catch (IllegalAccessException iae) {
            throw new FactoryConfigurationError("configuration error", iae);
        }
    }

    public abstract PlotDao getPlotDao(ParamPlotType paramPlotType);
}
