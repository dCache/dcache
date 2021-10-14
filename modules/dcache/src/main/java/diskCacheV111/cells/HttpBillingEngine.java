package diskCacheV111.cells;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.concurrent.TimeUnit.SECONDS;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.HTMLWriter;
import diskCacheV111.util.TimeoutCacheException;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.DomainContextAware;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;
import java.io.OutputStream;
import java.util.Map;
import java.util.TreeMap;
import org.dcache.cells.CellStub;
import org.dcache.util.Args;

public class HttpBillingEngine
      implements HttpResponseEngine, CellMessageSender, DomainContextAware {

    private final String _billingAddress;

    private CellStub _billing;
    private Map<String, Object> _context;

    public HttpBillingEngine(String[] args) {
        Args arguments = new Args(args);
        _billingAddress = arguments.getOption("billing");
        checkArgument(!isNullOrEmpty(_billingAddress), "billing option is required");
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _billing = new CellStub(endpoint, new CellPath(_billingAddress), 5, SECONDS);
    }

    @Override
    public void setDomainContext(Map<String, Object> context) {
        _context = context;
    }

    private void printTotalStatistics(HTMLWriter out, Object[][] x) {
        out.println("<h2>Total Request Overview</h2>");
        out.beginTable("sortable",
              "action", "Action",
              "count", "Total Request Count",
              "failures", "Request Failed");

        for (Object[] y : x) {
            String key = (String) y[0];
            int[] z = (int[]) y[1];

            out.beginRow(null, "odd");
            out.th("action", key);
            out.td("count", z[0]);
            out.td("failures", z[1]);
            out.endRow();
        }
        out.endTable();
    }

    private void printPoolStatistics(HTMLWriter out, Map<String, long[]> map, String pool) {
        boolean perPool = pool != null;

        out.print("<h2>Pool Statistics");
        if (perPool) {
            out.println(" of " + pool);
        }
        out.println("</h2>");

        if (perPool) {
            out.beginTable("sortable",
                  "pool_storageclass", "StorageClass",
                  "pool_transfers", "Mover Transfers",
                  "pool_restores", "Restores from HSM",
                  "pool_stores", "Stores to HSM",
                  "pool_errors", "Total Errors");
        } else {
            out.beginTable("sortable",
                  "pool_pool", "Pool",
                  "pool_transfers", "Mover Transfers",
                  "pool_restores", "Restores from HSM",
                  "pool_stores", "Stores to HSM",
                  "pool_errors", "Total Errors");
        }

        long[] total = new long[4];
        for (Map.Entry<String, long[]> entry : new TreeMap<>(map).entrySet()) {
            String s = entry.getKey();
            long[] counters = entry.getValue();

            out.beginRow(null, "odd");
            if (perPool) {
                out.th("pool_storageclass", s);
            } else {
                out.th("pool_pool",
                      "<a href=\"pool/" + s + "\">" + s + "</a>");
            }
            out.td("pool_transfers", counters[0]);
            out.td("pool_restores", counters[1]);
            out.td("pool_stores", counters[2]);
            out.td("pool_errors", counters[3]);
            total[0] += counters[0];
            total[1] += counters[1];
            total[2] += counters[2];
            total[3] += counters[3];
            out.endRow();
        }

        //
        // total count
        //
        out.beginRow("total");
        out.th("total", "Total");
        out.td("pool_transfers", total[0]);
        out.td("pool_restores", total[1]);
        out.td("pool_stores", total[2]);
        out.td("pool_errors", total[3]);
        out.endRow();
        out.endTable();
    }

    private void printPerPoolStatisticsPage(OutputStream out, String pool) {
        HTMLWriter html = new HTMLWriter(out, _context);
        try {
            html.addHeader("/styles/billing.css", "dCache Billing");
            Map<String, long[]> map = _billing.sendAndWait("get pool statistics " + pool,
                  Map.class);
            printPoolStatistics(html, map, pool);
        } catch (NoRouteToCellException e) {
            html.print("<blockquote><pre>No connection to billing</pre></blockquote>");
        } catch (TimeoutCacheException e) {
            html.print("<blockquote><pre>Request Timed Out</pre></blockquote>");
        } catch (InterruptedException | CacheException e) {
            html.print("<blockquote><pre>" + e + "</pre></blockquote>");
        } finally {
            html.addFooter(getClass().getName());
        }
    }

    private void printMainStatisticsPage(OutputStream out)
          throws HttpException {
        HTMLWriter html = new HTMLWriter(out, _context);
        try {
            Object[][] x = _billing.sendAndWait("get billing info", Object[][].class);
            html.addHeader("/styles/billing.css", "dCache Billing");
            printTotalStatistics(html, x);
            try {
                Map<String, long[]> map = _billing.sendAndWait("get pool statistics", Map.class);
                printPoolStatistics(html, map, null);
            } catch (InterruptedException | TimeoutCacheException e) {
                throw e;
            } catch (CacheException e) {
                html.print(
                      "<p class=\"error\">This 'billingCell' doesn't support: 'get pool statistics':");
                html.print("<blockquote><pre>" + e + "</pre></blockquote>");
            }
        } catch (NoRouteToCellException e) {
            throw new HttpException(500, "No connection to billing");
        } catch (TimeoutCacheException e) {
            throw new HttpException(500, "Request Timed Out");
        } catch (InterruptedException | CacheException e) {
            throw new HttpException(500, "Problem : " + e.getMessage());
        } finally {
            html.addFooter(getClass().getName());
        }
    }

    @Override
    public void queryUrl(HttpRequest request)
          throws HttpException {
        OutputStream out = request.getOutputStream();
        String[] urlItems = request.getRequestTokens();
        int offset = request.getRequestTokenOffset();

        // System.out.println( "UrlItem (offset) "+offset ) ;
        // for( int i = 0 ; i < urlItems.length ; i++ )
        //   System.out.println("UrlItem : "+i+" "+urlItems[i] ) ;

        request.printHttpHeader(0);

        int argc = urlItems.length - offset;
        if (argc > 0) {
            if (urlItems[offset].equals("pool") && (argc > 1)) {
                printPerPoolStatisticsPage(out, urlItems[offset + 1]);
            }
        } else {
            printMainStatisticsPage(out);
        }
    }
}
