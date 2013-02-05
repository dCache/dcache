package org.dcache.srm.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.axis.AxisFault;
import org.apache.axis.MessageContext;
import org.apache.axis.providers.java.RPCProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        _log.trace("Invoking {}#{} with args {}",
                obj.getClass().getCanonicalName(), method.getName(), args);
        try {
            Object result = super.invokeMethod(msgContext, method, obj, args);
            _log.trace("Invokation of {}#{} returned {} ({})",
                    obj.getClass().getCanonicalName(), method.getName(),
                    result.getClass().getCanonicalName(), result);
            return result;
        } catch(InvocationTargetException e) {
            Throwable t = e.getCause();
            if(t instanceof AxisFault) {
                AxisFault fault = (AxisFault) t;
                /*
                * All exceptions that are to be delivered as a SOAP Fault are
                * subclasses of AxisFault.
                */
                _log.debug("Invocation produced AxisFault {}: code={}, reason={}, string={}",
                        fault.getClass().getSimpleName(), fault.getFaultCode(),
                        fault.getFaultReason(), fault.getFaultString());
            } else if(t instanceof RuntimeException) {
                _log.error("Bug detected, please report to support@dCache.org",
                        t);
            } else {
                _log.error("Unexpected invocation exception", t);
            }

            throw e;
        } catch(Exception e) {
            _log.error("Unexpected exception", e);
            throw e;
        }
    }

}
