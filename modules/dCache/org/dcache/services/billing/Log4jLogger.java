package org.dcache.services.billing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;

public class Log4jLogger extends TextLogger
{
    private Logger _logger = LoggerFactory.getLogger(Log4jLogger.class);

    public Log4jLogger()
    {
    }

    protected void log(Date date, String output)
    {
        _logger.info(output.toString());
    }
}
