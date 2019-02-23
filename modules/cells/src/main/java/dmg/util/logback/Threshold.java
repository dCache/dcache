package dmg.util.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.core.Appender;

/**
 * Simple record like class for pairing a LoggerName and a Level.
 */
public class Threshold
{
    private LoggerName _LOGGER;
    private Level _level;
    private String _appender;

    public void setLOGGER(LoggerName LOGGER)
    {
        _LOGGER = LOGGER;
    }

    public LoggerName getLogger()
    {
        return _LOGGER;
    }

    public void setLevel(Level level)
    {
        _level = level;
    }

    public Level getLevel()
    {
        return _level;
    }

    public void setAppender(String appender)
    {
        _appender = appender;
    }

    public String getAppender()
    {
        return _appender;
    }

    public boolean isApplicableToAppender(Appender<?> appender)
    {
        return (_appender == null || _appender.equals(appender.getName()));
    }
}
