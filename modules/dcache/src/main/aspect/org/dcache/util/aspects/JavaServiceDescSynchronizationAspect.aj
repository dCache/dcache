package org.dcache.util.aspects;

import org.apache.axis.description.JavaServiceDesc;

/**
 * Axis fails to synchronize access to internal collections in JavaServiceDesc. Instances
 * of that class are shared by concurrent requests and access thus has to be synchronized.
 */
public aspect JavaServiceDescSynchronizationAspect
{
    pointcut publicMethodExecution(JavaServiceDesc o) :
            execution(public * JavaServiceDesc.*(..)) && this(o);

    Object around(JavaServiceDesc o) : publicMethodExecution(o) {
        synchronized (o) {
            return proceed(o);
        }
    }
}
