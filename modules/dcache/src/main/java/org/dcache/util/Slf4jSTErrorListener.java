package org.dcache.util;

import org.slf4j.Logger;
import org.stringtemplate.v4.STErrorListener;
import org.stringtemplate.v4.misc.ErrorType;
import org.stringtemplate.v4.misc.STMessage;

/**
 * StringTemplate error listener that logs to SLF4J.
 */
public class Slf4jSTErrorListener implements STErrorListener
{
    private final Logger logger;

    public Slf4jSTErrorListener(Logger logger)
    {
        this.logger = logger;
    }

    @Override
    public void compileTimeError(STMessage msg)
    {
        logger.error(msg.toString());
    }

    @Override
    public void runTimeError(STMessage msg)
    {
        if (msg.error != ErrorType.NO_SUCH_PROPERTY) {
            logger.error(msg.toString());
        }
    }

    @Override
    public void IOError(STMessage msg)
    {
        logger.error(msg.toString());
    }

    @Override
    public void internalError(STMessage msg)
    {
        logger.error(msg.toString());
    }
}
