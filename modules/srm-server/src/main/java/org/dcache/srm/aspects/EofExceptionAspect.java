package org.dcache.srm.aspects;

import org.apache.axis.Message;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;

/**
 * Advices Axis 1 to not log a stack trace when the client disconnects before a reply
 * was sent.
 */
@Aspect
public class EofExceptionAspect
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Message.class);

    @Around("withincode(void org.apache.axis.Message.writeTo(java.io.OutputStream)) && call(void org.apache.axis.SOAPPart.writeTo(java.io.OutputStream))")
    public void swallowEofException(ProceedingJoinPoint thisJoinPoint) throws Throwable
    {
        try {
            thisJoinPoint.proceed();
        } catch (EOFException e) {
            LOGGER.warn("Client disconnected before SRM response was sent.");
        }
    }
}
