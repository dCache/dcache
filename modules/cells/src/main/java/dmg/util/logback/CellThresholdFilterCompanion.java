package dmg.util.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellNucleus;
import java.util.Map;

/**
 * Logback Filter that filters according to the FilterThresholds of the source cell.
 */
public class CellThresholdFilterCompanion extends Filter<ILoggingEvent> {

    private final String _name;

    public CellThresholdFilterCompanion(String name) {
        _name = name;
    }

    @Override
    public FilterReply decide(ILoggingEvent event) {
        if (!isStarted()) {
            return FilterReply.NEUTRAL;
        }

        Map<String, String> mdc = event.getMDCPropertyMap();
        String cell = (mdc == null) ? null : mdc.get(CDC.MDC_CELL);
        CellNucleus nucleus = CellNucleus.getLogTargetForCell(cell);
        if (nucleus == null) {
            // No specific cell nucleus of even SystemCell exists. We are in bootstrap phase, e.g. on our own ...
            Level level = RootFilterThresholds.getInstance()
                  .getThreshold(event.getLoggerName(), _name);
            if (event.getLevel().isGreaterOrEqual(level)) {
                return FilterReply.NEUTRAL;
            } else {
                return FilterReply.DENY;
            }
        }

        FilterThresholdSet thresholds = nucleus.getLoggingThresholds();
        if (thresholds == null) {
            return FilterReply.NEUTRAL;
        }

        Level threshold =
              thresholds.getThreshold(event.getLoggerName(), _name);
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
