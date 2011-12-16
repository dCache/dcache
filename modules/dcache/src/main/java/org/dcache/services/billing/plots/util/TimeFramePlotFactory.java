package org.dcache.services.billing.plots.util;

import java.util.Properties;

import org.dcache.services.billing.db.IBillingInfoAccess;

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

    /**
     * @param access
     * @return singleton instance of this class
     */
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
     *
     * @param className
     * @return implementation factory
     * @throws Throwable
     */
    public ITimeFrameHistogramFactory create(String className) throws Throwable {
        return create(className, null);
    }

    /**
     * Get the histogram factory instance based on the type.
     *
     * @param className
     * @param properties
     * @return implementation factory
     * @throws Throwable
     */
    public ITimeFrameHistogramFactory create(String className,
                    Properties properties) throws Throwable {
        ClassLoader classLoader = Thread.currentThread()
                        .getContextClassLoader();
        Class clzz = Class.forName(className, true, classLoader);
        ITimeFrameHistogramFactory instance = (ITimeFrameHistogramFactory) clzz
                        .newInstance();
        if (properties == null)
            instance.initialize(access);
        else
            instance.initialize(access, properties);
        return instance;
    }
}