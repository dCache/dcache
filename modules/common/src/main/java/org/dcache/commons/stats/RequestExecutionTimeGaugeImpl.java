package org.dcache.commons.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.Formatter;
import java.util.concurrent.TimeUnit;

import org.dcache.util.TimeUtils;

/**
 * This class stores an average and other statistics of the execution time of the request.
 */
public class RequestExecutionTimeGaugeImpl implements RequestExecutionTimeGaugeMXBean {

    private static final Logger LOG = LoggerFactory.getLogger(RequestExecutionTimeGaugeImpl.class);

    private final String name;

    private final Statistics statistics = new Statistics();

    /**
     * last value fed to the gauge
     */
    private long lastExecutionTime = 0;
    private long startTime;

    /**
     *
     * @param name
     */
    public  RequestExecutionTimeGaugeImpl(String name, String family) {
        this.name = name;
        String mxName = String.format("%s:type=RequestExecutionTimeGauge,family=%s,name=%s",
                this.getClass().getPackage().getName(), family, this.name);
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName mxBeanName = new ObjectName(mxName);
            if (!server.isRegistered(mxBeanName)) {
                server.registerMBean(this, mxBeanName);
            }
        } catch (MalformedObjectNameException ex) {
            LOG.warn("Failed to create a MXBean with name: {} : {}" , mxName, ex.toString());
        } catch (InstanceAlreadyExistsException | MBeanRegistrationException ex) {
            LOG.warn("Failed to register a MXBean: {}", ex.toString());
        } catch (NotCompliantMBeanException ex) {
            LOG.warn("Failed to create a MXBean: {}", ex.toString());
        }
        startTime = System.currentTimeMillis();
    }

    /**
     *
     * @param nextExecTime
     */
    @Override
    public synchronized void update(long nextExecTime) {

        if (nextExecTime < 0) {
            LOG.info("possible backwards time shift detected; discarding invalid data ({})", nextExecTime);
            return;
        }

        statistics.update(nextExecTime);
        lastExecutionTime = nextExecTime;
    }

    /**
     * Returns average over the lifetime of the gauge.
     */
    @Override
    public synchronized double getAverageExecutionTime() {
        return statistics.getMean();
    }

    /**
     * Returns average over the last period and reset the gauge.
     */
    @Override
    public synchronized double resetAndGetAverageExecutionTime() {
        double avg = getAverageExecutionTime();
        reset();
        return  avg;
    }

    /**
     * Returns string representation of this RequestExecutionTimeGauge
     *  Only long term statistics is printed
     */
        @Override
    public synchronized String toString() {

        String aName = (name.length() > 34) ? name.substring(0, 34) : name;
        long updatePeriod = System.currentTimeMillis() - startTime;
        StringBuilder sb = new StringBuilder();
        try (Formatter formatter = new Formatter(sb)) {
            formatter.format("%-34s %,12.2f\u00B1%,10.2f %,12d %,12d %,12.2f %,12d %12s",
                             aName,
                             statistics.getMean(),
                             statistics.getStandardError(),
                             getMinExecutionTime(), getMaxExecutionTime(),
                             statistics.getSampleStandardDeviation(),
                             statistics.getSampleSize(),
                             TimeUtils.duration(updatePeriod, TimeUnit.MILLISECONDS, TimeUtils.TimeUnitFormat.SHORT));
        }
        return sb.toString();
    }

    /**
     * @return the minExecutionTime
     */
    @Override
    public synchronized long getMinExecutionTime() {
        return Math.round(statistics.getMin());
    }

    /**
     * @return the maxExecutionTime
     */
    @Override
    public synchronized long getMaxExecutionTime() {
        return Math.round(statistics.getMax());
    }

    /**
     * @return the name
     */
    @Override
    public String getName() {
        return name;
    }

    @Override
    public synchronized double getStandardDeviation() {
        return statistics.getSampleStandardDeviation();
    }

    /**
     *
     * @return standard error of the mean
     */
    @Override
    public synchronized double getStandardError() {
        return statistics.getStandardError();
    }
    /**
     * @return the updateNum
     */
    @Override
    public synchronized long getUpdateNum() {
        return statistics.getSampleSize();
    }

    /**
     * @return the lastExecutionTime
     */
    @Override
    public synchronized long getLastExecutionTime() {
        return lastExecutionTime;
    }

    /**
     * @return the startTime
     */
    @Override
    public synchronized long getStartTime() {
        return startTime;
    }

    @Override
    public synchronized void reset() {
        startTime = System.currentTimeMillis();
        lastExecutionTime = 0;
        statistics.reset();
    }

    /**
     * Encapsulates an online algorithm for maintaining various statistics about
     * samples.
     *
     * See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
     * for an explanation.
     */
    private static class Statistics
    {
        private double mean;              // Running mean
        private double m2;                // Sum of squares of differences from mean
        private double min = Double.NaN;  // Smallest sample
        private double max = Double.NaN;  // Largest sample
        private long n;                   // Number of samples

        public void reset()
        {
            mean = m2 = 0;
            min = max = Double.NaN;
            n = 0;
        }

        public void update(double x)
        {
            min = (n == 0) ? x : Math.min(x, min);
            max = (n == 0) ? x : Math.max(x, max);

            n++;

            double nextMean = mean + (x - mean) / n;
            double nextM2 = m2 + (x - mean) * (x - nextMean);
            mean = nextMean;
            m2 = nextM2;
        }

        public double getMean()
        {
            return (n > 0) ? mean : Double.NaN;
        }

        public double getSampleVariance()
        {
            return (n > 1) ? m2 / (n - 1) : Double.NaN;
        }

        public double getPopulationVariance()
        {
            return (n > 0) ? m2 / n : Double.NaN;
        }

        public double getSampleStandardDeviation()
        {
            return Math.sqrt(getSampleVariance());
        }

        public double getPopulationStandardDeviation()
        {
            return Math.sqrt(getPopulationVariance());
        }

        public double getStandardError()
        {
            return getSampleStandardDeviation() / Math.sqrt(n);
        }

        public long getSampleSize()
        {
            return n;
        }

        public double getMin()
        {
            return min;
        }

        public double getMax()
        {
            return max;
        }
    }
}
