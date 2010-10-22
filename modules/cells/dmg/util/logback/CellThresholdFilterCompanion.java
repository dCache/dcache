package dmg.util.logback;

import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CDC;

import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * Logback Filter that filters according to the FilterThresholds of
 * the source cell.
 */
public class CellThresholdFilterCompanion extends Filter<ILoggingEvent>
{
    private final String _name;

    public CellThresholdFilterCompanion(String name)
    {
        _name = name;
    }

    @Override
    public FilterReply decide(ILoggingEvent event)
    {
        if (!isStarted()) {
            return FilterReply.NEUTRAL;
        }

        Map<String,String> mdc = event.getMdc();
        String cell = (mdc == null) ? null : mdc.get(CDC.MDC_CELL);
        CellNucleus nucleus = CellNucleus.getLogTargetForCell(cell);
        if (nucleus == null) {
            return FilterReply.NEUTRAL;
        }

        FilterThresholds thresholds = nucleus.getLoggingThresholds();
        if (thresholds == null) {
            return FilterReply.NEUTRAL;
        }

        Level threshold =
            thresholds.getThreshold(LoggerName.getInstance(event.getLoggerName()), _name);
        if (threshold == null) {
            return FilterReply.NEUTRAL;
        }

        if (event.getLevel().isGreaterOrEqual(threshold)) {
            return FilterReply.NEUTRAL;
        } else {
            return FilterReply.DENY;
        }
    }
}