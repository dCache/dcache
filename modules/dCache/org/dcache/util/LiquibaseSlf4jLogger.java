package org.dcache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;

import liquibase.logging.LogLevel;

/**
 * This class provides a bridge between the Liquibase Logger and slf4j.
 */
public class LiquibaseSlf4jLogger implements liquibase.logging.Logger {

    private static final int PRIORITY = 2; // this needs to be > 1 for
                                           // Liquibase to choose this class
                                           // over built-in stderr logger.

    private Logger _inner;

    private LogLevel _logLevel; // we allow this to be set and queried but
                                // setting the log level is outside slf4j's
                                // remit

    @Override
    public int getPriority()
    {
        return PRIORITY;
    }

    private void guardInner()
    {
        checkState(_inner != null, "logger not set; use setName");
    }

    @Override
    public void debug(String message)
    {
        guardInner();
        _inner.debug(message);
    }

    @Override
    public void debug(String message, Throwable t)
    {
        guardInner();
        _inner.debug(message, t);
    }

    @Override
    public LogLevel getLogLevel()
    {
        return _logLevel;
    }

    @Override
    public void info(String message)
    {
        guardInner();
        _inner.info(message);
    }

    @Override
    public void info(String message, Throwable t)
    {
        guardInner();
        _inner.info(message, t);
    }

    @Override
    public void setLogLevel(String logLevel)
    {
        _logLevel = LogLevel.valueOf(logLevel.toUpperCase());
    }

    @Override
    public void setLogLevel(LogLevel logLevel)
    {
        _logLevel = logLevel;
    }

    @Override
    public void setLogLevel(String logLevel, String logFile)
    {
        setLogLevel(logLevel);
    }

    @Override
    public void setName(String name)
    {
        _inner = LoggerFactory.getLogger(name);
    }

    @Override
    public void severe(String arg0)
    {
        guardInner();
        _inner.error(arg0);
    }

    @Override
    public void severe(String arg0, Throwable arg1)
    {
        guardInner();
        _inner.error(arg0, arg1);
    }

    @Override
    public void warning(String arg0)
    {
        guardInner();
        _inner.warn(arg0);
    }

    @Override
    public void warning(String arg0, Throwable arg1)
    {
        guardInner();
        _inner.warn(arg0, arg1);
    }
}
