package dmg.util.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LOGGER;
import ch.qos.logback.classic.LOGGERContext;
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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

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
        checkNotNull(onHigherOrEqual);
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
        checkNotNull(onLower);
        _onLower = onLower;
    }

    private Set<Appender<ILoggingEvent>>
        getAppenders(LOGGERContext context)
    {
        Set<Appender<ILoggingEvent>> appenders = Sets.newHashSet();
        for (LOGGER LOGGER: context.getLOGGERList()) {
            Iterators.addAll(appenders, LOGGER.iteratorForAppenders());
        }
        return appenders;
    }

    @Override
    public void start()
    {
        LOGGERContext context = (LOGGERContext) getContext();

        for (LOGGER LOGGER: context.getLOGGERList()) {
            RootFilterThresholds.setRoot(LOGGERName.getInstance(LOGGER.getName()), !LOGGER.isAdditive());
        }

        for (Appender<ILoggingEvent> appender: getAppenders(context)) {
            String appenderName = appender.getName();

            RootFilterThresholds.addAppender(appenderName);
            for (Threshold threshold: _thresholds) {
                if (threshold.isApplicableToAppender(appender)) {
                    RootFilterThresholds.setThreshold(
                            threshold.getLOGGER(),
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
    public FilterReply decide(Marker marker, LOGGER LOGGER, Level level,
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

        FilterThresholdSet thresholds = nucleus.getLoggingThresholds();
        if (thresholds == null) {
            return FilterReply.NEUTRAL;
        }

        Level threshold =
            thresholds.getThreshold(LOGGER);
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
