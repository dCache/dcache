package dmg.util;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.layout.EchoLayout;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellNucleus;

/**
 * Logback appender which can send messages to a pinboard. The MDC
 * must have an entry with the key 'cells.cell', which contains the
 * cell name. The cell name is used to identify the correct pinboard
 * to which to log the message.
 *
 * @see Pinboard
 */
public class PinboardAppender extends AppenderBase<ILoggingEvent>
{
    private static Layout<ILoggingEvent> _layout =
        new EchoLayout<>();

    public void setLayout(Layout<ILoggingEvent> layout)
    {
        if (layout == null) {
            throw new IllegalArgumentException("Null value is not allowed");
        }
        _layout = layout;
    }

    public Layout<ILoggingEvent> getLayout()
    {
        return _layout;
    }

    @Override
    protected void append(ILoggingEvent event)
    {
        String cell = event.getMDCPropertyMap().get(CDC.MDC_CELL);
        CellNucleus nucleus = CellNucleus.getLogTargetForCell(cell);
        if (nucleus != null) {
            Pinboard pinboard = nucleus.getPinboard();
            if (pinboard != null) {
                pinboard.pin(_layout.doLayout(event));
            }
        }
    }
}
