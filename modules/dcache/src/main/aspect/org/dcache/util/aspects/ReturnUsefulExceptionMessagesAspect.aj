package org.dcache.util.aspects;

import org.ietf.jgss.GSSException;

import javax.net.ssl.SSLException;

public aspect ReturnUsefulExceptionMessagesAspect
{
    String around(SSLException e) : call(String getMessage()) && target(e) {
        String message = proceed(e);

        Throwable t = e;
        Throwable cause = e.getCause();
        if (cause != null && (message == null ||
                message.equals("General SSLEngine problem"))) {
            t = cause;
            message = cause.getMessage();
        }

        return message == null ? t.getClass().getName() : message;
    }


    String around(GSSException e) : call(String getMessage()) && target(e) {
        String message = proceed(e);

        Throwable t = e;
        Throwable cause = e.getCause();
        if (cause != null && (message == null ||
                (e.getMajor() == GSSException.FAILURE && e.getMinor() == 0 &&
                message.equals("Failure unspecified at GSS-API level")))) {
            t = cause;
            message = cause.getMessage();
        }

        return message == null ? t.getClass().getName() : message;
    }
}
