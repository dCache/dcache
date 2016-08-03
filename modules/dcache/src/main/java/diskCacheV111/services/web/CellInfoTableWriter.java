// $Id: PrintPoolCellHelper.java,v 1.1 2006-06-05 08:51:28 patrick Exp $Cg
package diskCacheV111.services.web;

import java.text.SimpleDateFormat;
import java.util.Collection;

import diskCacheV111.util.HTMLWriter;

import dmg.cells.nucleus.CellInfo;

public class CellInfoTableWriter
{
    private final SimpleDateFormat _formatter =
        new SimpleDateFormat ("MM/dd HH:mm:ss");
    private final HTMLWriter _html;

    public CellInfoTableWriter(HTMLWriter html)
    {
        _html = html;
    }

    public void print(Collection<PoolCellQueryInfo> itemSet)
    {
        _html.beginTable("sortable",
                        "cell",    "CellName",
                        "domain",  "DomainName",
                        "rp",      "RP",
                        "th",      "TH",
                        "ping",    "Ping",
                        "time",    "Creation Time",
                        "version", "Version");

        for (Object i : itemSet) {
            try {
                PoolCellQueryInfo info = (PoolCellQueryInfo)i;
                CellInfo cellInfo = info.getPoolCellInfo();
                long     pingTime = info.getPingTime();
                if (info.isOk()) {
                    printCellInfoRow(cellInfo, pingTime);
                } else {
                    printOfflineCellInfoRow(cellInfo.getCellName(),
                                            cellInfo.getDomainName().isEmpty()
                                            ? "&lt;unknown&gt"
                                            : cellInfo.getDomainName());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        _html.endTable();
    }

    private void printOfflineCellInfoRow(String name, String domain)
    {
        _html.beginRow(null, "odd");
        _html.td("cell",   name);
        _html.td("domain", domain);
        _html.td(5, "offline", "OFFLINE");
        _html.endRow();
    }

    private void printCellInfoRow(CellInfo info, long ping)
    {
        _html.beginRow(null, "odd");
        _html.td("cell",   info.getCellName());
        _html.td("domain", info.getDomainName());
        _html.td("rp",     info.getEventQueueSize());
        _html.td("th",     info.getThreadCount());
        _html.td("ping",   ping + " msec");
        _html.td("time",   _formatter.format(info.getCreationTime()));
        try {
            _html.td("version", info.getCellVersion());
        } catch (NoSuchMethodError e) {
            _html.td("version", "not-implemented");
        }
        _html.endRow();
    }
}
