package org.dcache.services.billing;

import org.apache.log4j.Logger;
import java.util.Date;

public class Log4jLogger extends TextLogger
{
    private Logger _logger = Logger.getLogger(Log4jLogger.class);

    public Log4jLogger()
    {
    }

    protected void log(Date date, String output)
    {
        _logger.info(output);
    }
}
