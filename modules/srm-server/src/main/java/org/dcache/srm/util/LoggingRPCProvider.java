package org.dcache.srm.util;

import org.apache.axis.AxisFault;
import org.apache.axis.MessageContext;
import org.apache.axis.providers.java.RPCProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This class wraps the default Axis Java dispatcher (RPCProvider) to provide
 * reasonable logging of problems via slf4j.
 *
 * Apache Axis allows for custom dispatch through a class that implements
 * the org.apache.axis.Handler interface.  The standard handler, RPCProvider,
 * invokes a method on the user-supplied class with the same name as the RPC
 * call.  RPCProvider uses a method invokeMethod that contains a single call to
 * the method, via reflection.  This allows superclasses to decorate how the
 * method call happens; for example, by adding additional security checks.
 *
 * Unfortunately, the error reporting in Axis is less than ideal.  Exceptions
 * that do not represent SOAP Faults are not logged on the server-side, but are
 * reported back to the client as a SOAP Fault.
 *
 * This class fixes the problem by wrapping the RPC invocation and logging
 * activity and problems.  It follows the anti-pattern of catch-log-throw,
 * which risks logging the same problem twice; however, this seems to be
 * required in order to guarantee that problems are reported in a meaningful
 * way.
 */
public class LoggingRPCProvider extends RPCProvider
{
    private static final long serialVersionUID = 1L;

    private static final Logger _log =
            LoggerFactory.getLogger(LoggingRPCProvider.class);

    @Override
    protected Object invokeMethod(MessageContext msgContext,
                                  Method method, Object obj,
                                  Object[] args) throws Exception
    {
        String className = obj.getClass().getCanonicalName();
        String methodName = method.getName();

        _log.trace("Invoking {}#{} with {}", className, methodName, args);
        try {
            Object result = super.invokeMethod(msgContext, method, obj, args);


            if (method.getReturnType().equals(Void.TYPE)) {
                _log.trace("Invocation of {}#{} completed", className,
                        methodName);
            } else {
                if (result == null) {
                    _log.trace("Invocation of {}#{} returned null", className,
                            methodName);
                } else {
                    _log.trace("Invocation of {}#{} returned {}: {}", className,
                            methodName, result.getClass().getCanonicalName(),
                            result);
                }
            }
            return result;
        } catch(InvocationTargetException e) {
            Throwable t = e.getCause();
            if(t instanceof AxisFault) {
                AxisFault fault = (AxisFault) t;
                /*
                * All exceptions that are to be delivered as a SOAP Fault are
                * subclasses of AxisFault.
                */
                _log.trace("Invocation produced AxisFault {}: code={}, " +
                        "reason={}, string={}",
                        fault.getClass().getSimpleName(), fault.getFaultCode(),
                        fault.getFaultReason(), fault.getFaultString());
            } else if(t instanceof RuntimeException) {
                _log.error("Bug detected, please report this to " +
                        "support@dCache.org", t);
            } else {
                _log.error("Unexpected invocation exception", t);
            }

            throw e;
        } catch(RuntimeException e) {
            _log.error("Bug detected, please report this to " +
                    "support@dCache.org", e);
            throw e;
        }
    }
}
