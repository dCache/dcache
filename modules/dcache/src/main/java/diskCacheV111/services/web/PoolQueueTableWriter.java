// $Id: PrintPoolCellHelper.java,v 1.1 2006-06-05 08:51:28 patrick Exp $Cg
package diskCacheV111.services.web;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.NamedPoolQueueInfo;
import diskCacheV111.pools.PoolCostInfo.PoolQueueInfo;
import diskCacheV111.util.HTMLWriter;

class PoolCostEntry
{
    final String  _cellName;
    final String  _domainName;
    final int[][] _row;
    final Map<String, NamedPoolQueueInfo> _movers;

    PoolCostEntry(String name, String domain, int[][] row)
    {
        _cellName   = name;
        _domainName = domain;
        _row        = row;
        _movers     = null;
    }

    PoolCostEntry(String name, String domain, int[][] row,
                  Map<String, NamedPoolQueueInfo> movers)
    {
        _cellName   = name;
        _domainName = domain;
        _row        = row;
        _movers     = movers;
    }
}

class ActionHeaderExtension
{
    private TreeMap<String,int[]> _map;

    ActionHeaderExtension(TreeMap<String,int[]> map)
    {
        _map = map;
    }

    public String toString()
    {
        return _map.toString();
    }

    int[][] getSortedMovers(Map<String,NamedPoolQueueInfo> moverMap)
    {
        int[][] rows = new int[_map.size()][];
        if (moverMap == null) {
            for (int i = 0; i < _map.size(); i++) {
                rows[i] = new int[]{-1, -1, -1};
            }
        } else {
            int i = 0;
            for (String key : _map.keySet()) {
                NamedPoolQueueInfo mover = moverMap.get(key);
                if (mover == null ) {
                    rows[i] = new int[] { -1, -1, -1 };
                } else {
                    rows[i] = new int[] {
                        mover.getActive(),
                        mover.getMaxActive(),
                        mover.getQueued()
                    };
                }
                i++;
            }
        }
        return rows;
    }

    public Set<String> getSet()
    {
        return _map.keySet();
    }

    public Map<String,int[]> getTotals()
    {
        return _map;
    }
}

public class PoolQueueTableWriter
{
    private static final int HEADER_TOP    = 0;
    private static final int HEADER_MIDDLE = 1;
    private static final int HEADER_BOTTOM = 2;

    private final int _repeatHeader = 30;
    private final HTMLWriter _html;

    public PoolQueueTableWriter(HTMLWriter html)
    {
        _html = html;
    }

    private void printPoolActionTableTotals(ActionHeaderExtension extension,
                                            int[][] total)
    {
        _html.beginRow("total");
        _html.th(2, null, "Total");

        for (int[] row : total) {
            _html.td("active", row[0]);
            if (row[1] >= 0) {
                _html.td("max", row[1]);
            } else {
                _html.td("max");
            }
            _html.td("queued", row[2]);
        }

        Map<String,int[]> map =
            extension == null ? null : extension.getTotals();
        if (map != null) {
            for (int[] row : map.values()) {
                _html.td("active", row[0]);
                _html.td("max", row[1]);
                _html.td("queued", row[2]);
            }
        }
        _html.endRow();
    }

    private void printPoolActionRow(PoolCostEntry info,
                                    ActionHeaderExtension ext)
    {
        try {
            _html.beginRow(null, "odd");
            _html.td("cell",   info._cellName);
            _html.td("domain", info._domainName);

            for (int[] row : info._row) {
                if (row == null) {
                    _html.td(3, "integrated", "Integrated");
                } else {
                    _html.td("active", row[0]);
                    if (row[1] >= 0) {
                        _html.td("max", row[1]);
                    } else {
                        _html.td("max");
                    }
                    if (row[2] > 0) {
                        _html.td("queued", row[2]);
                    } else {
                        _html.td("idle", 0);
                    }
                }
            }

            if (ext != null) {
                for (int[] row : ext.getSortedMovers(info._movers)) {
                    _html.td("active", row[0]);
                    _html.td("max", row[1]);
                    if (row[2] > 0) {
                        _html.td("queued", row[2]);
                    } else {
                        _html.td("idle", 0);
                    }
                }
            }
            _html.endRow();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void printPoolActionTableHeader(ActionHeaderExtension ext,
                                            int position)
    {
        assert HEADER_TOP    == 0;
        assert HEADER_MIDDLE == 1;
        assert HEADER_BOTTOM == 2;

        int[][] program = {
            { 0, 1, 2, 3 },
            { 0, 3, 2, 1, 2, 3 },
            { 0, 3, 2, 1 }
        };

        Set<String> moverSet = ext != null ? ext.getSet() : null;
        int diff = moverSet == null ? 0 : moverSet.size();

        for (int i : program[position]) {
            switch (i) {
            case 0:
                int rowspan = program[position].length / 2;
                _html.beginRow();
                _html.th(rowspan, 1, "cell",   "CellName");
                _html.th(rowspan, 1, "domain", "DomainName");
                break;

            case 1:
                _html.th(3, null, "Movers");
                _html.th(3, null, "Restores");
                _html.th(3, null, "Stores");
                _html.th(3, null, "P2P-Server");
                _html.th(3, null, "P2P-Client");

                if (moverSet != null) {
                    for (String s : moverSet) {
                        _html.th(3, null, s);
                    }
                }
                _html.endRow();
                break;

            case 2:
                _html.beginRow();
                break;

            case 3:
                for (int h = 0, n = 5 + diff; h < n; h++) {
                    _html.th("active", "Active");
                    _html.th("max",    "Max");
                    _html.th("queued", "Queued");
                }
                _html.endRow();
                break;
            }
        }
    }

    /**
     * Converts the pool cost info (xgetcellinfo) into the int[][]
     * array.
     */
    private int[][] decodePoolCostInfo(PoolCostInfo costInfo)
    {
        try {
            PoolQueueInfo mover     = costInfo.getMoverQueue();
            PoolQueueInfo restore   = costInfo.getRestoreQueue();
            PoolQueueInfo store     = costInfo.getStoreQueue();
            PoolQueueInfo p2pServer = costInfo.getP2pQueue();
            PoolQueueInfo p2pClient = costInfo.getP2pClientQueue();

            int[][] rows = new int[5][];

            rows[0] = new int[3];
            rows[0][0] = mover.getActive();
            rows[0][1] = mover.getMaxActive();
            rows[0][2] = mover.getQueued();

            rows[1] = new int[3];
            rows[1][0] = restore.getActive();
            rows[1][1] = -1;
            rows[1][2] = restore.getQueued();

            rows[2] = new int[3];
            rows[2][0] = store.getActive();
            rows[2][1] = -1;
            rows[2][2] = store.getQueued();

            if (p2pServer == null) {
                rows[3] = null;
            } else {
                rows[3] = new int[3];
                rows[3][0] = p2pServer.getActive();
                rows[3][1] = p2pServer.getMaxActive();
                rows[3][2] = p2pServer.getQueued();
            }

            rows[4] = new int[3];
            rows[4][0] = p2pClient.getActive();
            rows[4][1] = p2pClient.getMaxActive();
            rows[4][2] = p2pClient.getQueued();

            return rows;
        } catch (Exception e) {
            return null;
        }
    }

    public void print(Collection<PoolCellQueryInfo> itemSet)
    {
        //
        // get the translated list
        //
        List<PoolCostEntry> list = preparePoolCostTable(itemSet);

        //
        // calculate the totals ...
        //
        TreeMap<String, int[]> moverMap = new TreeMap<>();
        int[][] total = new int[5][3];

        for (PoolCostEntry e : list) {
            if (e._movers != null) {
                for (Map.Entry<String,NamedPoolQueueInfo> entry :
                         e._movers.entrySet()) {
                    String queueName = entry.getKey();
                    int[] t = moverMap.get(queueName);
                    if (t == null) {
                        moverMap.put(queueName, t = new int[3]);
                    }
                    NamedPoolQueueInfo mover = entry.getValue();

                    t[0] += mover.getActive();
                    t[1] += mover.getMaxActive();
                    t[2] += mover.getQueued();
                }
            }
            int[][] status = e._row;
            for (int j = 0; j < total.length; j++) {
                for (int l = 0; l < total[j].length; l++) {
                    if (status[j] != null) {
                        total[j][l] += status[j][l];
                    }
                }
            }
        }

        ActionHeaderExtension extension = new ActionHeaderExtension(moverMap);

        _html.beginTable(null);
        printPoolActionTableHeader(extension, HEADER_TOP);
        printPoolActionTableTotals(extension, total);

        int i = 0;
        for (PoolCostEntry e : list) {
            i++;
            printPoolActionRow(e, extension);
            if ((_repeatHeader != 0) && (i % _repeatHeader) == 0) {
                printPoolActionTableHeader(extension, HEADER_MIDDLE);
            }
        }
        printPoolActionTableTotals(extension, total);
        printPoolActionTableHeader(extension, HEADER_BOTTOM);
        _html.endTable();
    }

    private List<PoolCostEntry>
        preparePoolCostTable(Collection<PoolCellQueryInfo> itemSet)
    {
        List<PoolCostEntry> list = new ArrayList<>();

        for (PoolCellQueryInfo info : itemSet) {
            try {
                PoolCellInfo cellInfo = info.getPoolCellInfo();
                if (info.isOk() && (cellInfo instanceof PoolCellInfo)) {
                    PoolCellInfo pci = cellInfo;
                    int [] [] status = decodePoolCostInfo(pci.getPoolCostInfo());

                    if (status != null) {
                        list.add(new PoolCostEntry(pci.getCellName(),
                                pci.getDomainName(),
                                status,
                                pci.getPoolCostInfo().getExtendedMoverHash()));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }
}
