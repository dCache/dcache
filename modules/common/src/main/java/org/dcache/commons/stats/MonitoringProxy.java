package org.dcache.commons.stats;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

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
        this.monitoredObject = checkNotNull(monitoredObject);
        this.counter = checkNotNull(counter);
        this.gauge = checkNotNull(gauge);
    }

    @Override
    public Object invoke(Object proxy, final Method method, Object[] args)
            throws Throwable {
        counter.incrementRequests(method);
        final long startTimeStamp = System.currentTimeMillis();
        Object result = null;
        try {
            result = method.invoke(monitoredObject, args);
        } catch (InvocationTargetException e) {
            counter.incrementFailed(method);
            throw e.getTargetException();
        } catch (Error | RuntimeException e) {
            counter.incrementFailed(method);
            throw e;
        } finally {
            if (result instanceof ListenableFuture) {
                final ListenableFuture<?> future = (ListenableFuture<?>) result;
                future.addListener(
                        new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                try {
                                    Uninterruptibles.getUninterruptibly(future);
                                } catch (ExecutionException | Error | RuntimeException e) {
                                    counter.incrementFailed(method);
                                }
                                updateTime(method, startTimeStamp);
                            }
                        },
                        MoreExecutors.directExecutor());
            } else {
                updateTime(method, startTimeStamp);
            }
        }
        return result;
    }

    private void updateTime(Method method, long startTimeStamp)
    {
        long execTime = System.currentTimeMillis() - startTimeStamp;
        logger.debug("invocation of {} took {} ms", method, execTime);
        gauge.update(method, execTime);
    }

    public static <T> T decorateWithMonitoringProxy(Class<?>[] interfaces,
                                                    T monitoringObject,
                                                    RequestCounters<Method> counter,
                                                    RequestExecutionTimeGauges<Method> gauge) {
        MonitoringProxy <T> monitoringHandler = new MonitoringProxy<>(monitoringObject,counter,gauge);
        return (T) Proxy.newProxyInstance(monitoringObject.getClass().getClassLoader(),
                                          interfaces, monitoringHandler);


    }

}
