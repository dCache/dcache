package dmg.util;

import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CDC;

import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.AppenderSkeleton;

/**
 * Log4j appender which can send messages to a pinboard. The Log4j MDC
 * must have a property with the key 'cell', which contains the cell
 * name. The cell name is used to identify the correct pinboard to
 * which to log the message.
 *
 * @see Pinboard
 */
public class PinboardAppender extends AppenderSkeleton
{
    protected void append(LoggingEvent event)
    {
        String cell = (String) event.getMDC(CDC.MDC_CELL);
        if (cell != null) {
            CellNucleus nucleus = CellNucleus.getLogTargetForCell(cell);
            if (nucleus != null) {
                Pinboard pinboard = nucleus.getPinboard();
                if (pinboard != null) {
                    pinboard.pin(layout.format(event));
                }
            }
        }
    }

    public boolean requiresLayout()
    {
        return true;
    }

    public void close()
    {
    }
}