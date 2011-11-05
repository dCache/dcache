package dmg.util.logback;

import ch.qos.logback.classic.Level;

/**
 * Utility class holding the singleton root of the FilterThresholds
 * hierarchy.
 */
public abstract class RootFilterThresholds
{
    private final static FilterThresholds _instance =
        new FilterThresholds();

    public static FilterThresholds getInstance()
    {
        return _instance;
    }

    public static void addFilter(String filter)
    {
        _instance.addFilter(filter);
    }

    public static void setThreshold(LoggerName logger, String filter, Level level)
    {
        _instance.setThreshold(logger, filter, level);
    }
}