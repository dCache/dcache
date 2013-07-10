// $Id: HttpBillingEngine.java,v 1.12 2007-08-16 20:20:54 behrmann Exp $
package diskCacheV111.cells ;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import diskCacheV111.util.HTMLWriter;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;

public class HttpBillingEngine implements HttpResponseEngine
{
    private final CellEndpoint _endpoint;

    public HttpBillingEngine(CellEndpoint endpoint, String [] args)
    {
        _endpoint = endpoint;
    }

    @Override
    public void startup()
    {
        // No background activity to start
    }

    @Override
    public void shutdown()
    {
        // No background activity to shutdown
    }

    private void printTotalStatistics(HTMLWriter out, Object [][] x)
    {
        out.println("<h2>Total Request Overview</h2>");
        out.beginTable("sortable",
                       "action", "Action",
                       "count", "Total Request Count",
                       "failures", "Request Failed");

        for (Object[] y : x) {
            String    key = (String)y[0];
            int    [] z   = (int [])y[1];

            out.beginRow(null,  "odd");
            out.th("action", key);
            out.td("count", z[0]);
            out.td("failures", z[1]);
            out.endRow();
        }
        out.endTable();
    }

    private void printPoolStatistics(HTMLWriter out, HashMap<String, long[]> map, String pool)
    {
        boolean perPool = pool != null ;

        out.print("<h2>Pool Statistics");
        if (perPool) {
            out.println(" of " + pool);
        }
        out.println("</h2>");

        if (perPool) {
            out.beginTable("sortable",
                           "pool_storageclass", "StorageClass",
                           "pool_transfers",    "Mover Transfers",
                           "pool_restores",     "Restores from HSM",
                           "pool_stores",       "Stores to HSM",
                           "pool_errors",       "Total Errors");
        } else {
            out.beginTable("sortable",
                           "pool_pool",         "Pool",
                           "pool_transfers",    "Mover Transfers",
                           "pool_restores",     "Restores from HSM",
                           "pool_stores",       "Stores to HSM",
                           "pool_errors",       "Total Errors");
        }

        long []  total = new long[4] ;
        for (Map.Entry<String, long[]> entry : new TreeMap<>(map).entrySet()) {
            String    s = entry.getKey();
            long []   counters = entry.getValue();

            out.beginRow(null,  "odd");
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

    private void printPerPoolStatisticsPage(OutputStream out, String pool)
    {
        HTMLWriter html = new HTMLWriter(out, _endpoint.getDomainContext());
        try {
            html.addHeader("/styles/billing.css", "dCache Billing");

            CellMessage result =
                _endpoint.sendAndWait(new CellMessage(new CellPath("billing"),
                                                     "get pool statistics "
                                                     + pool),
                                     5000);
            if (result == null) {
                throw new HttpException(500, "Request Timed Out");
            }

            HashMap<String,long[]> map = (HashMap<String,long[]>) result.getMessageObject();

            printPoolStatistics(html, map, pool);
        } catch (Exception e) {
            html.println("<p class=\"error\">This 'billingCell' doesn't support:  'get pool statistics &lt;poolName&gt;'</p>");
            html.print("<blockquote><pre>" + e + "</pre></blockquote>");
        } finally {
            html.addFooter(getClass().getName() + " [$Revision: 1.12 $]");
        }
    }

    private void printMainStatisticsPage(OutputStream out)
        throws HttpException
    {
        CellMessage result;
        try {
            result =
                _endpoint.sendAndWait(new CellMessage(new CellPath("billing"),
                                                     "get billing info"),
                                     5000);
        } catch (Exception e) {
            throw new
                HttpException(500, "Problem : " + e.getMessage());
        }

        if (result == null) {
            throw new HttpException(500, "Request Timed Out");
        }

        HTMLWriter html = new HTMLWriter(out, _endpoint.getDomainContext());
        try {
            Object [][] x = (Object [][])result.getMessageObject() ;

            html.addHeader("/styles/billing.css", "dCache Billing");
            printTotalStatistics(html, x);

            try {
                result =
                    _endpoint.sendAndWait(new CellMessage(new CellPath("billing"),
                                                         "get pool statistics"),
                                         5000);
                if (result == null) {
                    throw new HttpException(500, "Request Timed Out");
                }

                HashMap<String,long[]> map = (HashMap<String,long[]>) result.getMessageObject();
                printPoolStatistics(html, map, null);
            } catch (Exception e) {
                html.print("<p class=\"error\">This 'billingCell' doesn't support: 'get pool statistics':");
                html.print("<blockquote><pre>" + e + "</pre></blockquote>");
            }
        } catch (Exception e) {
            throw new HttpException(500, "Problem: " + e.getMessage());
        } finally {
            html.addFooter(getClass().getName() + " [$Revision: 1.12 $]");
        }
    }

    @Override
    public void queryUrl(HttpRequest request)
        throws HttpException
    {
        OutputStream out   = request.getOutputStream();
        String [] urlItems = request.getRequestTokens();
        int       offset   = request.getRequestTokenOffset();

        // System.out.println( "UrlItem (offset) "+offset ) ;
        // for( int i = 0 ; i < urlItems.length ; i++ )
        //   System.out.println("UrlItem : "+i+" "+urlItems[i] ) ;

        request.printHttpHeader(0);

        int argc = urlItems.length - offset ;
        if (argc > 0) {
            if (urlItems[offset].equals("pool") && (argc > 1)) {
                printPerPoolStatisticsPage(out, urlItems[offset + 1]);
            }
        } else {
            printMainStatisticsPage(out);
        }
    }
}
