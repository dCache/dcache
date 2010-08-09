package dmg.util.logback;

import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CDC;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Marker;
import org.slf4j.MDC;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

public class CellThresholdFilter extends TurboFilter
{
    private FilterReply _onHigherOrEqual = FilterReply.NEUTRAL;
    private FilterReply _onLower = FilterReply.DENY;

    private final List<Threshold> _thresholds =
        new ArrayList<Threshold>();

    /**
     * Adds a default threshold that will be used by all filters
     * unless overridden.
     */
    public void addThreshold(Threshold threshold)
    {
        if (isStarted()) {
            throw new IllegalStateException("Cannot add threshold after start");
        }
        _thresholds.add(threshold);
    }

    /**
     * Get the FilterReply when the level of the logging request is
     * higher or equal to the effective threshold.
     *
     * @return FilterReply
     */
    public FilterReply getOnHigherOrEqual()
    {
        return _onHigherOrEqual;
    }

    public void setOnHigherOrEqual(FilterReply onHigherOrEqual)
    {
        if (onHigherOrEqual == null) {
            throw new IllegalArgumentException("Null value not allowed");
        }
        _onHigherOrEqual = onHigherOrEqual;
    }

    /**
     * Get the FilterReply when the level of the logging request is
     * lower than the effective threshold.
     *
     * @return FilterReply
     */
    public FilterReply getOnLower()
    {
        return _onLower;
    }

    public void setOnLower(FilterReply onLower)
    {
        if (onLower == null) {
            throw new IllegalArgumentException("Null value not allowed");
        }
        _onLower = onLower;
    }

    private Set<Appender<ILoggingEvent>>
        getAppenders(LoggerContext context)
    {
        Set<Appender<ILoggingEvent>> appenders =
            new HashSet<Appender<ILoggingEvent>>();
        for (Logger logger: context.getLoggerList()) {
            Iterator<Appender<ILoggingEvent>> i = logger.iteratorForAppenders();
            while (i.hasNext()) {
                Appender<ILoggingEvent> appender = i.next();
                appenders.add(appender);
            }
        }
        return appenders;
    }

    @Override
    public void start()
    {
        LoggerContext context = (LoggerContext) getContext();

        for (Appender<ILoggingEvent> appender: getAppenders(context)) {
            String name = appender.getName();

            RootFilterThresholds.addFilter(name);
            for (Threshold threshold: _thresholds) {
                if (threshold.isApplicableToAppender(appender)) {
                    RootFilterThresholds.setThreshold(threshold.getLogger(), name, threshold.getLevel());
                }
            }

            CellThresholdFilterCompanion filter =
                new CellThresholdFilterCompanion(name);
            filter.start();
            appender.addFilter(filter);
        }

        super.start();
    }

    @Override
    public FilterReply decide(Marker marker, Logger logger, Level level,
                              String format, Object[] params, Throwable t)
    {
        if (!isStarted()) {
            return FilterReply.NEUTRAL;
        }

        String cell = MDC.get(CDC.MDC_CELL);
        CellNucleus nucleus = CellNucleus.getLogTargetForCell(cell);
        if (nucleus == null) {
            return FilterReply.NEUTRAL;
        }

        FilterThresholds thresholds = nucleus.getLoggingThresholds();
        if (thresholds == null) {
            return FilterReply.NEUTRAL;
        }

        Level threshold =
            thresholds.getThreshold(LoggerName.getInstance(logger));
        if (threshold == null) {
            return FilterReply.NEUTRAL;
        }

        if (level.isGreaterOrEqual(threshold)) {
            return _onHigherOrEqual;
        } else {
            return _onLower;
        }
    }
}