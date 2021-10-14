// $Id: PrintPoolCellHelper.java,v 1.1 2006-06-05 08:51:28 patrick Exp $Cg
package diskCacheV111.services.web;

import static org.dcache.util.ByteUnit.BYTES;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.HTMLWriter;
import dmg.cells.nucleus.CellInfo;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;

public class PoolGroupInfoTableWriter {

    private final HTMLWriter _html;

    public PoolGroupInfoTableWriter(HTMLWriter html) {
        _html = html;
    }

    private static double round(double value) {
        return Math.floor(value * 10) / 10.0;
    }

    private void printRow(String group,
          long total, long freespace, long precious, long removable,
          String... classes) {
        double red = round(100 * precious / (float) total);
        double green = round(100 * removable / (float) total);
        double yellow = round(100 * freespace / (float) total);
        double magenta = Math.max(0, 100 - red - green - yellow);

        _html.beginRow(classes);
        _html.td("group", group);
        _html.td("total", BYTES.toMiB(total));
        _html.td("free", BYTES.toMiB(freespace));
        _html.td("precious", BYTES.toMiB(precious));
        _html.td("layout",
              "<div>",
              "<div class=\"layout_precious\" style=\"width: ",
              String.format(Locale.US, "%.1f", red), "%\"></div>",
              "<div class=\"layout_sticky\" style=\"width: ",
              String.format(Locale.US, "%.1f", magenta), "%\"></div>",
              "<div class=\"layout_cached\" style=\"width: ",
              String.format(Locale.US, "%.1f", green), "%\"></div>",
              "<div class=\"layout_free\" style=\"width: ",
              String.format(Locale.US, "%.1f", yellow), "%\"></div>",
              "</div>");
        _html.endRow();
    }

    public void print(String base,
          SortedMap<String, Collection<Object>> info) {
        _html.beginTable("sortable",
              "group", "PoolGroup",
              "total", "Total Space/MB",
              "free", "Free Space/MB",
              "precious", "Precious Space/MB",
              "layout", "<span>Layout   " +
                    "(<span class=\"layout_precious\">precious/</span>" +
                    "<span class=\"layout_sticky\">sticky/</span>" +
                    "<span class=\"layout_cached\">cached/</span>" +
                    "<span class=\"layout_free\">free</span>)</span>");

        for (Map.Entry<String, Collection<Object>> entry : info.entrySet()) {
            String link = String.format("<a href=\"%s/%s/spaces\">%s</a>",
                  base, entry.getKey(), entry.getKey());
            long[] spaces = sumUpSpaces(entry.getValue());
            printRow(link,
                  spaces[0], spaces[1], spaces[2], spaces[3],
                  null, "odd");
        }

        _html.endTable();
    }

    private long[] sumUpSpaces(Collection<Object> itemSet) {
        long[] result = new long[4];

        for (Object i : itemSet) {
            try {
                PoolCellQueryInfo info = (PoolCellQueryInfo) i;
                CellInfo cellInfo = info.getPoolCellInfo();
                if (info.isOk() && (cellInfo instanceof PoolCellInfo)) {
                    PoolCostInfo.PoolSpaceInfo spaceInfo =
                          ((PoolCellInfo) cellInfo).getPoolCostInfo().getSpaceInfo();
                    result[0] += spaceInfo.getTotalSpace();
                    result[1] += spaceInfo.getFreeSpace();
                    result[2] += spaceInfo.getPreciousSpace();
                    result[3] += spaceInfo.getRemovableSpace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
