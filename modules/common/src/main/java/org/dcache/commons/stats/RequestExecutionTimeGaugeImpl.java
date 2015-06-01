
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
 * this class stores an average of the execution time of the request
 * if the num is the num of updates that took place before this update
 * then the next average  is calculated using the formula:
 *         newaverage =
 *          (previousaverage*num +nextmeasurment) /(num+1);
 * there is a utility method to read an average(mean), max, mean, RMS, standard deviation
 * and error on mean.
 *
 * @author timur
 */
public class RequestExecutionTimeGaugeImpl implements RequestExecutionTimeGaugeMXBean {

    private static final Logger LOG = LoggerFactory.getLogger(RequestExecutionTimeGaugeImpl.class);

    private final String name;

    private long sumExecutionTime =0;
    private long sumExecutionTimeSquared =0;

    /**
     * Minimum
     */
    private long minExecutionTime=0;
    /**
     * Maximum
     */
    private long maxExecutionTime=0;

    /**
     * number of updates
     */
    private long  updateNum=0;
    /**
     * last value fed to the gauge
     */
    private long lastExecutionTime=0;
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

        try {
            if (nextExecTime < 0) {
                LOG.info("possible backwards time shift detected; discarding invalid data ({})",
                         nextExecTime);
                return;
            }

            minExecutionTime = (updateNum == 0) ? nextExecTime : Math.min(minExecutionTime, nextExecTime);
            maxExecutionTime = Math.max(maxExecutionTime, nextExecTime);

            sumExecutionTime = Math.addExact(sumExecutionTime, nextExecTime);
            sumExecutionTimeSquared = Math.addExact(sumExecutionTimeSquared, nextExecTime * nextExecTime);

            updateNum++;

            lastExecutionTime = nextExecTime;
        } catch (ArithmeticException e) {
            startTime = System.currentTimeMillis();
            sumExecutionTime = nextExecTime;
            minExecutionTime = nextExecTime;
            maxExecutionTime = nextExecTime;
            sumExecutionTimeSquared = nextExecTime * nextExecTime;
            lastExecutionTime = nextExecTime;
            updateNum = 1;
        }
    }

    /**
     * Returns average over the lifetime of the gauge.
     */
    @Override
    public synchronized double getAverageExecutionTime() {
        return (updateNum == 0) ? 0 : (double) sumExecutionTime / updateNum;
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
                    aName, getAverageExecutionTime(),getStandardError(),
                    minExecutionTime,maxExecutionTime,
                    getStandardDeviation(), updateNum,
                    TimeUtils.duration(updatePeriod, TimeUnit.MILLISECONDS, TimeUtils.TimeUnitFormat.SHORT));
        }
        return sb.toString();
    }

    /**
     * @return the minExecutionTime
     */
    @Override
    public synchronized long getMinExecutionTime() {
        return minExecutionTime;
    }

    /**
     * @return the maxExecutionTime
     */
    @Override
    public synchronized long getMaxExecutionTime() {
        return maxExecutionTime;
    }

    /**
     * @return the name
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * @return the RMS of executionTime
     */
    @Override
    public synchronized double getExecutionTimeRMS() {
        return (updateNum == 0) ? 0 : Math.sqrt((double) sumExecutionTimeSquared / updateNum);
    }

    @Override
    public synchronized double getStandardDeviation() {
        if (updateNum == 0) {
            return 0;
        }
        double averageExecutionTime = getAverageExecutionTime();
        double deviationSquare = ((double) sumExecutionTimeSquared / updateNum) - (averageExecutionTime * averageExecutionTime);
        assert (deviationSquare >= 0);
        return Math.sqrt(deviationSquare);
    }

    /**
     *
     * @return standard error of the mean
     */
    @Override
    public synchronized double getStandardError() {
        return (updateNum == 0) ? 0 : getStandardDeviation() / Math.sqrt(updateNum);
    }
    /**
     * @return the updateNum
     */
    @Override
    public synchronized long getUpdateNum() {
        return updateNum;
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
        sumExecutionTime = 0;
        minExecutionTime = 0;
        maxExecutionTime = 0;
        sumExecutionTimeSquared = 0;
        lastExecutionTime = 0;
        updateNum = 0;
    }
}
