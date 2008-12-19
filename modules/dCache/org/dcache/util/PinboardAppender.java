package org.dcache.util;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.lang.ref.WeakReference;

import dmg.cells.nucleus.CellAdapter;

import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.AppenderSkeleton;

public class PinboardAppender extends AppenderSkeleton
{
    static private Map<String,WeakReference<CellAdapter>> _cells =
        Collections.synchronizedMap(new HashMap());

    public static void addCell(CellAdapter cell)
    {
        _cells.put(cell.getCellName(), new WeakReference(cell));
    }

    public static void removeCell(CellAdapter cell)
    {
        _cells.remove(cell.getCellName());
    }

    protected void append(LoggingEvent event)
    {
        WeakReference<CellAdapter> ref = _cells.get(event.getMDC("cell"));
        if (ref != null) {
            CellAdapter adapter = ref.get();
            if (adapter != null) {
                adapter.pin(layout.format(event));
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