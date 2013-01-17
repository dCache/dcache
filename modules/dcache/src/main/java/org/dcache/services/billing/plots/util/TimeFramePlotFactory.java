package org.dcache.services.billing.plots.util;

import java.util.Properties;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.plots.exceptions.TimeFrameFactoryInitializationException;

/**
 * For constructing Plots based on  {@link ITimeFramePlot}. Requires
 * {@link IBillingInfoAccess} implemenation.
 *
 * @author arossi
 */
public class TimeFramePlotFactory {

    private static TimeFramePlotFactory instance;

    private IBillingInfoAccess access;

    /*
     * static singleton
     */
    private TimeFramePlotFactory() {
    }

    public static synchronized TimeFramePlotFactory getInstance(
                    IBillingInfoAccess access) {
        if (instance == null) {
            instance = new TimeFramePlotFactory();
            instance.access = access;
        }
        return instance;
    }

    /**
     * Get the histogram factory instance based on the type.
     */
    public ITimeFrameHistogramFactory create(String className)
                    throws ClassNotFoundException, InstantiationException,
                    IllegalAccessException,
                    TimeFrameFactoryInitializationException {
        return create(className, null);
    }

    /**
     * Get the histogram factory instance based on the type.
     */
    public ITimeFrameHistogramFactory create(String className,
                    Properties properties) throws ClassNotFoundException,
                    InstantiationException, IllegalAccessException,
                    TimeFrameFactoryInitializationException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<? extends ITimeFrameHistogramFactory> clzz =
                Class.forName(className, true, classLoader).asSubclass(ITimeFrameHistogramFactory.class);
        ITimeFrameHistogramFactory instance = clzz.newInstance();
        if (properties == null) {
            instance.initialize(access);
        } else {
            instance.initialize(access, properties);
        }
        return instance;
    }
}
