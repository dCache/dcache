package org.dcache.commons.stats;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MonitoringProxy can be used to decorate a any class implementing an interface
 * T for measuring  the number of invocations
 * or the execution times or both of each of the method of T, using RequestCounters
 * and RequestExecutionTimeGauges classes.
 * This class provides a utility method for decorating a class that implements
 * interface T with a MonitoringProxy proxy.
 * <p/>
 * The  article
 * <a href=http://www.ibm.com/developerworks/java/library/j-jtp08305.html>
 * Java theory and practice: Decorating with dynamic proxies</a>
 * from IBM Developer Network claims that performance impact from dynamic proxies
 * should be minimal.
 * @author timur
 */
public class MonitoringProxy  <T> implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(MonitoringProxy.class);
    private final T monitoredObject;
    private final RequestCounters<Method> counter;
    private final RequestExecutionTimeGauges<Method> gauge;
    private MonitoringProxy(T monitoredObject,
            RequestCounters<Method> counter,
            RequestExecutionTimeGauges<Method> gauge) {
        this.monitoredObject = monitoredObject;
        this.counter = counter;
        this.gauge = gauge;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        long startTimeStamp = System.currentTimeMillis();
        if(counter != null) {
            counter.incrementRequests(method);
        }
        try {
            Object result = method.invoke(monitoredObject, args);
            return result;
        } catch (InvocationTargetException e) {
            if(counter != null) {
                counter.incrementFailed(method);
            }
            throw e.getTargetException();
        } finally {
            long execTime = System.currentTimeMillis() - startTimeStamp;
            if (logger.isDebugEnabled()) {
                logger.debug("invocation of "+method+" took "+execTime+" ms");
            }
            if(gauge != null) {
                gauge.update(method, execTime);
            }
        }
    }

    public static <T> T decorateWithMonitoringProxy( Class[] interfaces,
            T monitoringObject,
            RequestCounters<Method> counter,
            RequestExecutionTimeGauges<Method> gauge) {
        MonitoringProxy <T> monitoringHandler = new MonitoringProxy<T> (
                monitoringObject,counter,gauge);
        return (T) Proxy.newProxyInstance(monitoringObject.getClass().getClassLoader(),
                                                         interfaces,
                                                          monitoringHandler);


    }

}
