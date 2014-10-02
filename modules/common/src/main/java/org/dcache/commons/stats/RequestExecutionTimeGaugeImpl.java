
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

    /**
     * average
     */
    private long averageExecutionTime=0;
    /**
     * Minimum
     */
    private long minExecutionTime=0;
    /**
     * Maximum
     */
    private long maxExecutionTime=0;
    /**
     *  Square of the RMS (Root Mean Square)
     *  sum(value_i)/n
     * RMSS(i+1)=(RMSS(i)+value(i+1)**2)/(i+1)
     */
    private long executionTimeRMSS=0;
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

        if(nextExecTime <0) {
            LOG.info("possible backwards timeshift detected; discarding invalid data ({})",
                    nextExecTime);
            return;
        }

        minExecutionTime = updateNum == 0 ? nextExecTime : Math.min(getMinExecutionTime(), nextExecTime);
        maxExecutionTime = Math.max(getMaxExecutionTime(), nextExecTime);

        averageExecutionTime
                = (averageExecutionTime * updateNum + nextExecTime) / (updateNum + 1);
        executionTimeRMSS
                = (executionTimeRMSS * updateNum + nextExecTime * nextExecTime)
                / (updateNum + 1);

        updateNum++;

        lastExecutionTime = nextExecTime;
    }

    /**
     * return average over the lifetime of the gauge
     * @return
     */
    @Override
    public synchronized long getAverageExecutionTime() {
        return averageExecutionTime;
    }

    /**
     * Returns average over the last period and reset the gauge.
     * @return
     */
    @Override
    public synchronized long resetAndGetAverageExecutionTime() {
        long avg = getAverageExecutionTime();
        reset();
        return  avg;
    }

    /**
     *
     * @return String representation of this RequestExecutionTimeGauge
     *  Only long term statistics is printed
     */
        @Override
    public synchronized String toString() {

        String aName = name;
        if(name.length() >34) {
             aName = aName.substring(0,34);
        }
        long updatePeriod= System.currentTimeMillis() -
                startTime;
        StringBuilder sb = new StringBuilder();
        try (Formatter formatter = new Formatter(sb)) {
            formatter.format("%-34s %,12d\u00B1%,10.2f %,12d %,12d %,12d %,12d %12s",
                    aName, averageExecutionTime,getStandardError(),
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
        return Math.sqrt(executionTimeRMSS);
    }

    @Override
    public synchronized long getStandardDeviation() {
        long deviationSquare = executionTimeRMSS - averageExecutionTime*averageExecutionTime;
        assert (deviationSquare >=0);
        return (long) Math.sqrt(executionTimeRMSS - averageExecutionTime*averageExecutionTime);
    }

    /**
     *
     * @return standard error of the mean
     */
    @Override
    public synchronized double getStandardError() {
        return getStandardDeviation() / Math.sqrt(updateNum);
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
        averageExecutionTime=0;
        minExecutionTime=0;
        maxExecutionTime=0;
        executionTimeRMSS=0;
        lastExecutionTime = 0;
        updateNum = 0;
    }
}
