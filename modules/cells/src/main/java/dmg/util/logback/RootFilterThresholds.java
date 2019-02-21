package dmg.util.logback;

import ch.qos.logback.classic.Level;

/**
 * Utility class holding the singleton root of the FilterThresholds
 * hierarchy.
 */
public abstract class RootFilterThresholds
{
    private static final FilterThresholdSet _instance =
        new FilterThresholdSet();

    public static FilterThresholdSet getInstance()
    {
        return _instance;
    }

    public static void addAppender(String appender)
    {
        _instance.addAppender(appender);
    }

    public static void setThreshold(LOGGERName LOGGER, String appender, Level level)
    {
        _instance.setThreshold(LOGGER, appender, level);
    }

    public static void setRoot(LOGGERName LOGGER, boolean isRoot)
    {
        _instance.setRoot(LOGGER, isRoot);
    }
}
