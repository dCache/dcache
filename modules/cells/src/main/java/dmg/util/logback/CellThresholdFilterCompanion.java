package dmg.util.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

import java.util.Map;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellNucleus;

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

        Map<String,String> mdc = event.getMDCPropertyMap();
        String cell = (mdc == null) ? null : mdc.get(CDC.MDC_CELL);
        CellNucleus nucleus = CellNucleus.getLogTargetForCell(cell);
        if (nucleus == null) {
            return FilterReply.NEUTRAL;
        }

        FilterThresholdSet thresholds = nucleus.getLoggingThresholds();
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
