package dmg.util;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.lang.ref.WeakReference;

import dmg.cells.nucleus.CellAdapter;

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
    static private Map<String,WeakReference<Pinboard>> _pinboards =
        Collections.synchronizedMap(new HashMap());

    public static void addPinboard(String cellName, Pinboard pinboard)
    {
        _pinboards.put(cellName, new WeakReference(pinboard));
    }

    protected void append(LoggingEvent event)
    {
        WeakReference<Pinboard> ref = _pinboards.get(event.getMDC("cell"));
        if (ref != null) {
            Pinboard pinboard = ref.get();
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