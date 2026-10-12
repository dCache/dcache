package dmg.util.logback;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.spi.ConfigurationEvent;
import ch.qos.logback.core.spi.ConfigurationEventListener;
import ch.qos.logback.core.spi.FilterReply;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellNucleus;
import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.GuardedBy;
import org.slf4j.MDC;
import org.slf4j.Marker;

/**
 * Applies the per-cell, per-appender log thresholds.
 * <p>
 * Logback instantiates turbo filters before appenders and loggers, independently of their order in
 * the configuration file. Since this filter has to know all appenders, it defers its
 * initialization until logback reports that the configuration is completed. Until then the filter
 * is not started and doesn't influence logging.
 */
public class CellThresholdFilter extends TurboFilter implements ConfigurationEventListener {

    private FilterReply _onHigherOrEqual = FilterReply.NEUTRAL;
    private FilterReply _onLower = FilterReply.DENY;

    private final List<Threshold> _thresholds = Lists.newArrayList();

    /**
     * True once start was requested and until the filter is stopped.
     */
    @GuardedBy("this")
    private boolean _isStartRequested;

    /**
     * The flag in the base class isn't volatile, while the filter is started by the thread that
     * configures logback and is consulted by all logging threads.
     */
    private volatile boolean _isStarted;

    /**
     * Adds a default threshold that will be used by all filters unless overridden.
     */
    public void addThreshold(Threshold threshold) {
        checkState(!isStartRequested(), "Cannot add threshold after start");
        _thresholds.add(threshold);
    }

    /**
     * Get the FilterReply when the level of the logging request is higher or equal to the effective
     * threshold.
     *
     * @return FilterReply
     */
    public FilterReply getOnHigherOrEqual() {
        return _onHigherOrEqual;
    }

    public void setOnHigherOrEqual(FilterReply onHigherOrEqual) {
        requireNonNull(onHigherOrEqual);
        _onHigherOrEqual = onHigherOrEqual;
    }

    /**
     * Get the FilterReply when the level of the logging request is lower than the effective
     * threshold.
     *
     * @return FilterReply
     */
    public FilterReply getOnLower() {
        return _onLower;
    }

    public void setOnLower(FilterReply onLower) {
        requireNonNull(onLower);
        _onLower = onLower;
    }

    private Set<Appender<ILoggingEvent>>
    getAppenders(LoggerContext context) {
        Set<Appender<ILoggingEvent>> appenders = Sets.newHashSet();
        for (Logger logger : context.getLoggerList()) {
            Iterators.addAll(appenders, logger.iteratorForAppenders());
        }
        return appenders;
    }

    private synchronized boolean isStartRequested() {
        return _isStartRequested;
    }

    @Override
    public synchronized void start() {
        if (!_isStartRequested) {
            _isStartRequested = true;
            getContext().addConfigurationEventListener(this);
        }
    }

    @Override
    public synchronized void stop() {
        if (_isStartRequested) {
            getContext().removeConfigurationEventListener(this);
            _isStartRequested = false;
        }
        _isStarted = false;
        super.stop();
    }

    @Override
    public boolean isStarted() {
        return _isStarted;
    }

    @Override
    public synchronized void listen(ConfigurationEvent event) {
        switch (event.getEventType()) {
            case CONFIGURATION_ENDED_SUCCESSFULLY:
            case CONFIGURATION_ENDED_WITH_XML_PARSING_ERRORS:
                /* A configuration with errors may still have created some of the
                 * appenders; those should respect their thresholds.
                 */
                if (_isStartRequested && !_isStarted) {
                    applyThresholds();
                    super.start();
                    _isStarted = true;
                }
                break;
            default:
                break;
        }
    }

    @GuardedBy("this")
    private void applyThresholds() {
        LoggerContext context = (LoggerContext) getContext();

        for (Logger logger : context.getLoggerList()) {
            RootFilterThresholds.setRoot(LoggerName.getInstance(logger.getName()),
                  !logger.isAdditive());
        }

        for (Appender<ILoggingEvent> appender : getAppenders(context)) {
            String appenderName = appender.getName();

            RootFilterThresholds.addAppender(appenderName);
            for (Threshold threshold : _thresholds) {
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
    }

    @Override
    public FilterReply decide(Marker marker, Logger logger, Level level,
          String format, Object[] params, Throwable t) {
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

    private String getOrDiscoverCell() {
        String cellName = getCell();

        if (cellName == null) {
            CDC.discoverAndReset();
            cellName = getCell();
        }

        return cellName;
    }

    private String getCell() {
        return MDC.get(CDC.MDC_CELL);
    }
}
