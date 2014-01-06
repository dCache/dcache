package org.dcache.srm.aspects;

import java.io.EOFException;
import java.io.OutputStream;

import org.apache.axis.Message;
import org.apache.axis.SOAPPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Advices Axis 1 to not log a stack trace when the client disconnects before a reply
 * was sent.
 */
aspect EofExceptionAspect
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Message.class);

    pointcut writeToCalls() :
            withincode(void Message.writeTo(OutputStream)) && call(void SOAPPart.writeTo(OutputStream));

    void around() : writeToCalls() {
        try {
            proceed();
        } catch (EOFException e) {
            LOGGER.warn("Client disconnected before SRM response was sent.");
        }
    }
}
