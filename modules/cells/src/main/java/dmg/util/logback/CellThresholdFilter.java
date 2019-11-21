package dmg.util.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.spi.FilterReply;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.MDC;
import org.slf4j.Marker;

import java.util.List;
import java.util.Set;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellNucleus;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CellThresholdFilter extends TurboFilter
{
    private FilterReply _onHigherOrEqual = FilterReply.NEUTRAL;
    private FilterReply _onLower = FilterReply.DENY;

    private final List<Threshold> _thresholds = Lists.newArrayList();

    /**
     * Adds a default threshold that will be used by all filters
     * unless overridden.
     */
    public void addThreshold(Threshold threshold)
    {
        checkState(!isStarted(), "Cannot add threshold after start");
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
        requireNonNull(onHigherOrEqual);
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
        requireNonNull(onLower);
        _onLower = onLower;
    }

    private Set<Appender<ILoggingEvent>>
        getAppenders(LoggerContext context)
    {
        Set<Appender<ILoggingEvent>> appenders = Sets.newHashSet();
        for (Logger logger: context.getLoggerList()) {
            Iterators.addAll(appenders, logger.iteratorForAppenders());
        }
        return appenders;
    }

    @Override
    public void start()
    {
        LoggerContext context = (LoggerContext) getContext();

        for (Logger logger: context.getLoggerList()) {
            RootFilterThresholds.setRoot(LoggerName.getInstance(logger.getName()), !logger.isAdditive());
        }

        for (Appender<ILoggingEvent> appender: getAppenders(context)) {
            String appenderName = appender.getName();

            RootFilterThresholds.addAppender(appenderName);
            for (Threshold threshold: _thresholds) {
                if (threshold.isApplicableToAppender(appender)) {
                    RootFilterThresholds.setThreshold(
                            threshold.getLogger(),
                            appenderName,
                            threshold.getLevel());
                }
            }

            CellThresholdFilterCompanion filter =
                new CellThresholdFilterCompanion(appenderName);
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

        String cell = getOrDiscoverCell();
        CellNucleus nucleus = CellNucleus.getLogTargetForCell(cell);
        if (nucleus == null) {
            return FilterReply.NEUTRAL;
        }

        FilterThresholdSet thresholds = nucleus.getLoggingThresholds();
        if (thresholds == null) {
            return FilterReply.NEUTRAL;
        }

        Level threshold =
            thresholds.getThreshold(logger);
        if (threshold == null) {
            return FilterReply.NEUTRAL;
        }

        if (level.isGreaterOrEqual(threshold)) {
            return _onHigherOrEqual;
        } else {
            return _onLower;
        }
    }

    private String getOrDiscoverCell()
    {
        String cellName = getCell();

        if (cellName == null) {
            CDC.discoverAndReset();
            cellName = getCell();
        }

        return cellName;
    }

    private String getCell()
    {
        return MDC.get(CDC.MDC_CELL);
    }
}
