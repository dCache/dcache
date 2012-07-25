/*
 * Counter.java
 *
 * Created on April 4, 2009, 5:36 PM
 */

package org.dcache.commons.stats;
import java.lang.management.ManagementFactory;
import java.util.Formatter;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

/**
 * This class encapsulates two integer counters and  provides utility methods
 * for increments and discovery of the count of  request invocations and
 * failures 
 * This class is thread safe.
 * @author timur
 */
public class RequestCounterImpl implements RequestCounterMXBean {
    private final String name;
    private int   requests;
    private int    failed;
    private ObjectName mxBeanName;
    
    /** Creates a new instance of Counter
     * @param name
     */
    public RequestCounterImpl(String name, String family) {
        this.name = name;

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            String mxName = String.format("%s:type=RequestCounter,family=%s,name=%s",
                    this.getClass().getPackage().getName(), family, this.name);
            mxBeanName = new ObjectName(mxName);
            if (!server.isRegistered(mxBeanName)) {
                server.registerMBean(this, mxBeanName);
            }
        } catch (MalformedObjectNameException ex) {
            mxBeanName = null;
        } catch (InstanceAlreadyExistsException ex) {
            mxBeanName = null;
        } catch (MBeanRegistrationException ex) {
            mxBeanName = null;
        } catch (NotCompliantMBeanException ex) {
            mxBeanName = null;
        }
    }

    /**
     *
     * @return name of this counter
     */
    @Override
    public String getName() {
        return name;
    }

    @Override
    public synchronized String toString() {
        String aName = name;
        if(name.length() >34) {
             aName = aName.substring(0,34);
        }
        StringBuilder sb = new StringBuilder();

        Formatter formatter = new Formatter(sb);
         

        formatter.format("%-34s %9d %9d", aName, requests,  failed);
        formatter.flush();
        formatter.close();

        return sb.toString();
    }

    /**
     *
     * @return number of request invocations known to this counter
     */
    @Override
    public synchronized int getTotalRequests() {
        return requests;
    }

    @Override
    public synchronized void reset() {
        requests = 0;
        failed = 0;
    }

    @Override
    public synchronized void shutdown() {
        if(mxBeanName != null) {
            try {
                MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                server.unregisterMBean( mxBeanName);
            }catch( InstanceNotFoundException e) {
                // ignored
            }catch(MBeanRegistrationException e) {
                // ignored
            }
        }
    }

    /**
     * increments the number of request invocations known to this counter
     * @param requests number by which to increment
     */
    public synchronized void incrementRequests(int requests) {
        this.requests += requests;
    }

    /**
     * increments the number of request invocations known to this counter by 1
     */
    public synchronized void incrementRequests() {
        requests++;
    }

    /**
     *
     * @return number of faild request invocations known to this counter
     */
    @Override
    public synchronized int getFailed() {
        return failed;
    }

    /**
     * increments the number of failed request invocations known to this
     * counter
     * @param failed number by which to increment
     */
    public synchronized void incrementFailed(int failed) {
        this.failed += failed;
    }

    /**
     * increments the number of failed request invocations known to this counter
     * by 1
     */
    public synchronized void incrementFailed() {
        failed++;
    }

    /**
     *
     * @return number of requests that succeed
     *  This number is calculated as a difference between the
     *  total number of requests executed and the failed requests.
     *  The number of Successful requests  is accurate only if both
     *  number of requests executed and the failed requests are recorded
     *  accurately
     */
    public synchronized int getSuccessful() {
        return requests - failed;
    }
        
}
