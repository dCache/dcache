package org.dcache.services.billing;

import java.util.Map;
import java.util.HashMap;

import java.io.PrintWriter;

import dmg.cells.nucleus.CellInfo;
import dmg.util.Formats;
import dmg.util.Args;

import org.dcache.cells.CellInfoProvider;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;

import diskCacheV111.vehicles.InfoMessage;

public class BillingInfo
    implements CellInfoProvider,
               CellCommandListener,
               CellMessageReceiver
{
    private int _requests = 0;
    private int _failed = 0;
    private final Map<String,int[]> _map = new HashMap<String,int[]>();

    public BillingInfo()
    {
    }

    public synchronized void getInfo(PrintWriter pw)
    {
        pw.print(Formats.field("Requests", 20, Formats.RIGHT));
        pw.print(" : ");
        pw.print(Formats.field("" + _requests, 6, Formats.RIGHT));
        pw.print(" / ");
        pw.println(Formats.field("" + _failed, 6, Formats.LEFT));
        for (Map.Entry<String, int[]> entry : _map.entrySet()) {
            int[] values = entry.getValue();
            pw.print(Formats.field(entry.getKey(), 20, Formats.RIGHT));
            pw.print(" : ");
            pw.print(Formats.field("" + values[0], 6, Formats.RIGHT));
            pw.print(" / ");
            pw.println(Formats.field("" + values[1], 6, Formats.LEFT));
        }
    }

    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    public synchronized Object ac_get_billing_info(Args args)
    {
        int i = 0;
        Object[][] result = new Object[_map.size()][];
        for (Map.Entry<String,int[]> entry : _map.entrySet()) {
            int [] values = (int [])entry.getValue();
            result[i] = new Object[2];
            result[i][0] = entry.getKey();
            result[i][1] = new int[2];
            ((int [])(result[i][1]))[0] = values[0];
            ((int [])(result[i][1]))[1] = values[1];
            i++;
        }
        return result;
    }

    public synchronized void messageArrived(InfoMessage message)
    {
        if (message.getMessageType().equals("check"))
            return;

        String key = message.getMessageType() + ":" + message.getCellType();
        int[] values = _map.get(key);

        if (values == null) {
            values = new int[2];
            _map.put(key, values);
        }

        values[0]++;
        _requests++;
        if (message.getResultCode() != 0) {
            _failed++;
            values[1]++;
        }
    }
}