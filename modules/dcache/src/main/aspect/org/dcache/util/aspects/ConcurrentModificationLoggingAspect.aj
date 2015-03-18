package org.dcache.util.aspects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;

/**
 * Aspect that will log whenever a ConcurrentModificationException is thrown.
 */
public aspect ConcurrentModificationLoggingAspect
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentModificationLoggingAspect.class);

    before(Throwable e): (handler(java.lang.Throwable) || handler(java.lang.Exception) || handler(java.lang.RuntimeException) || handler(ConcurrentModificationException)) && args(e) {
        if (e instanceof ConcurrentModificationException) {
            LOGGER.warn("Likely bug detected. Please report this to support@dcache.org.", e);
        }
    }
}
