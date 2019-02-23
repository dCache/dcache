package dmg.util.logback;

import org.slf4j.Logger;

public class LoggerName
{
    public static final LoggerName ROOT =
        new LoggerName(LOGGER.ROOT_LOGGER_NAME);

    private String _name;

    public static LoggerName getInstance(Logger LOGGER)
    {
        return getInstance(LOGGER.getName());
    }

    public static LoggerName getInstance(String name)
    {
        if (name.equalsIgnoreCase(LOGGER.ROOT_LOGGER_NAME)) {
            return ROOT;
        } else {
            return new LoggerName(name);
        }
    }

    public static LoggerName valueOf(String name)
    {
        return getInstance(name);
    }

    private LoggerName(String name)
    {
        _name = name;
    }

    public boolean isNameOfLogger(Logger LOGGER)
    {
        return LOGGER.getName().equals(_name);
    }

    @Override
    public String toString()
    {
        return _name;
    }

    @Override
    public boolean equals(Object that)
    {
        if (this == that) {
            return true;
        }

        if (that == null || !that.getClass().equals(LoggerName.class)) {
            return false;
        }

        LoggerName other = (LoggerName) that;
        return _name.equals(other._name);
    }

    @Override
    public int hashCode()
    {
        return _name.hashCode();
    }

    public LoggerName getParent()
    {
        if (this == ROOT) {
            return null;
        }
        int pos = Math.max(_name.lastIndexOf('.'), _name.lastIndexOf('$'));
        if (pos > -1) {
            return new LoggerName(_name.substring(0, pos));
        } else {
            return ROOT;
        }
    }
}
