package org.dcache.services.billing;

import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;

import diskCacheV111.vehicles.PnfsFileInfoMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;
import diskCacheV111.vehicles.InfoMessage;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;

import dmg.util.Args;

public class PoolStatistics
    extends BillingComponent
    implements CellCommandListener,
               CellMessageReceiver
{
    private final Map<String, long[]> _poolStatistics =
        new HashMap<String, long[]>();
    private final Map<String, Map<String, long[]>> _poolStorageMap =
        new HashMap<String, Map<String, long[]>>();

    public synchronized void messageArrived(InfoMessage info)
    {
        if (!info.getCellType().equals("pool"))
            return;

        if (info instanceof WarningPnfsFileInfoMessage)
            return;

        String cellName = info.getCellName();
        int pos = cellName.indexOf("@");
        cellName = pos < 1 ? cellName : cellName.substring(0, pos);
        String transactionType = info.getMessageType();

        long [] counters = _poolStatistics.get(cellName);
        if (counters == null)
            _poolStatistics.put(cellName, counters = new long[4]);

        if (info.getResultCode() != 0) {
            counters[3]++;
        } else if (transactionType.equals("transfer")) {
            counters[0]++;
        } else if (transactionType.equals("restore")) {
            counters[1]++;
        } else if (transactionType.equals("store")) {
            counters[2]++;
        }

        if (info instanceof PnfsFileInfoMessage) {
            PnfsFileInfoMessage pnfsInfo = (PnfsFileInfoMessage)info;
            StorageInfo sinfo = (pnfsInfo).getStorageInfo();
            if (sinfo != null) {
                Map<String, long[]> map = _poolStorageMap.get(cellName);
                if (map == null) {
                    map = new HashMap<String, long[]>();
                    _poolStorageMap.put(cellName, map);
                }

                String key = sinfo.getStorageClass() + "@" + sinfo.getHsm();

                counters = map.get(key);

                if (counters == null) {
                    counters = new long[8];
                    map.put(key, counters);
                }

                if (info.getResultCode() != 0) {
                    counters[3]++;
                } else if (transactionType.equals("transfer")) {
                    counters[0]++;
                    MoverInfoMessage mim = (MoverInfoMessage)info;
                    counters[mim.isFileCreated() ? 4 : 5]
                        += mim.getDataTransferred();
                } else if (transactionType.equals("restore")) {
                    counters[1]++;
                    counters[6] += pnfsInfo.getFileSize();
                } else if (transactionType.equals("store")) {
                    counters[2]++;
                    counters[7] += pnfsInfo.getFileSize();
                }
            }
        }
    }

    private synchronized void dumpPoolStatistics(String name)
        throws IOException
    {
        name = name == null ?
            ( "poolFlow-" + fileNameFormat.format(new Date())):
            name;

        File report = new File(getDirectory(), name);
        PrintWriter pw =
            new PrintWriter(new BufferedWriter(new FileWriter(report)));
        try {
            Set<Map.Entry<String, Map<String, long[]>>> pools =
                _poolStorageMap.entrySet();

            for (Map.Entry<String, Map<String, long[]>> poolEntry : pools) {
                String poolName = poolEntry.getKey();
                Map<String, long[]> map = poolEntry.getValue();

                for (Map.Entry<String, long[]> statiEntry : map.entrySet()) {
                    String className = statiEntry.getKey();
                    long[] counter = statiEntry.getValue();
                    pw.print(poolName);
                    pw.print("  ");
                    pw.print(className);
                    for (int i = 0; i < counter.length; i++) {
                        pw.print("  " + counter[i]);
                    }
                    pw.println("");
                }
            }
            pw.close();
            pw = null;
        } finally {
            if (pw != null) {
                pw.close();
                report.delete();
            }
        }
    }

    public String hh_get_pool_statistics = "[<poolName>]";
    public synchronized Object ac_get_pool_statistics_$_0_1(Args args)
    {
        if (args.argc() == 0)
            return _poolStatistics;
        Map map = _poolStorageMap.get(args.argv(0));
        return map == null ? new HashMap() : map;
    }

    public String hh_clear_pool_statistics = "";
    public synchronized Object ac_clear_pool_statistics(Args args)
    {
        _poolStatistics.clear();
        _poolStorageMap.clear();
        return "";
    }

    public String hh_dump_pool_statistics = "";
    public String ac_dump_pool_statistics_$_0_1(Args args)
        throws IOException
    {
        dumpPoolStatistics(args.argc() == 0 ? null : args.argv(0));
        return "" ;
    }
}