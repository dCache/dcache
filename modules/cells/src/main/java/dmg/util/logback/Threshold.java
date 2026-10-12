package dmg.util.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.core.Appender;

/**
 * Simple record like class for pairing a LoggerName and a Level.
 */
public class Threshold {

    private LoggerName _logger;
    private Level _level;
    private String _appender;

    public void setLogger(LoggerName logger) {
        _logger = logger;
    }

    public LoggerName getLogger() {
        return _logger;
    }

    public void setLevel(Level level) {
        _level = level;
    }

    public Level getLevel() {
        return _level;
    }

    /**
     * Restricts the threshold to the appender with the given name. The property isn't called
     * 'appender', as logback interprets any element of that name as an appender definition.
     */
    public void setAppenderName(String appender) {
        _appender = appender;
    }

    public String getAppenderName() {
        return _appender;
    }

    public boolean isApplicableToAppender(Appender<?> appender) {
        return (_appender == null || _appender.equals(appender.getName()));
    }
}
