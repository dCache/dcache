package dmg.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CDC;

import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.AppenderSkeleton;

/**
 * Log4j appender which can send messages to a pinboard. The Log4j MDC
 * must have a property with the key 'cell', which contains the cell
 * name. The cell name is used to identify the correct pinboard to
 * which to log the message.
 *
 * CellAdapters register a pinboard through a call to the addPinboard
 * method.
 *
 * @see Pinboard
 */
public class PinboardAppender extends AppenderSkeleton
{
    static private Map<String,Pinboard> _pinboards = new ConcurrentHashMap();

    public static void addPinboard(String cellName, Pinboard pinboard)
    {
        _pinboards.put(cellName, pinboard);
    }

    public static void removePinboard(String cellName)
    {
        _pinboards.remove(cellName);
    }

    protected void append(LoggingEvent event)
    {
        Object cell = event.getMDC(CDC.MDC_CELL);
        if (cell != null) {
            Pinboard pinboard = _pinboards.get(cell);
            if (pinboard != null) {
                pinboard.pin(layout.format(event));
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