package org.dcache.services.billing.html;

import java.util.List;

import org.dcache.services.billing.plots.BillingHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.HTMLWriter;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;

/**
 * Servlet stand-in {@link org.dcache.services.httpd.HttpServiceCell}. <br><br>
 * Constructs dynamic html page with table of plots.
 *
 * @see BillingHistory
 * @author arossi
 */
public final class HttpBillingHistoryEngine extends BillingHistory implements
HttpResponseEngine {

    private static final Logger _log = LoggerFactory.getLogger(HttpBillingHistoryEngine.class);
    private static final String fileSep = System.getProperty("file.separator");

    private static final String THUMBNAIL_W = "120";
    private static final String THUMBNAIL_H = "60";

    private CellNucleus nucleus;
    private final Thread _billingHistory;

    /**
     * @param nucleus
     * @param args
     */
    public HttpBillingHistoryEngine(CellNucleus nucleus, String[] args) {
        super(new Args(args));
        this.nucleus = nucleus;
        _billingHistory = nucleus.newThread(this, "billing-history");
    }

    @Override
    public void startup()
    {
        _billingHistory.start();
    }


    @Override
    public void shutdown()
    {
        setRunning(false);
        _billingHistory.interrupt();
        try {
            _billingHistory.join();
        } catch (InterruptedException e) {
            _log.warn("Interrupted while waiting for BillingHistory thread to terminate");
        }
        close();
    }

    /*
     * (non-Javadoc)
     *
     * @see dmg.util.HttpResponseEngine#queryUrl(dmg.util.HttpRequest)
     */
    @Override
    public void queryUrl(HttpRequest request) throws HttpException {
        request.printHttpHeader(0);
        emit(new HTMLWriter(request.getOutputStream(),
                        this.nucleus.getDomainContext()));
    }

    /**
     * Constructs the html page.
     *
     * @param contents
     */
    public void emit(HTMLWriter contents) {
        contents.addHeader("/styles/common.css", "Billing History Plots");
        contents.println("<table align='center'>");
        contents.println("<tr>");
        contents.println("<th>Plot Type</th>");
        contents.println("<th>24-Hour</th>");
        contents.println("<th>Weekly</th>");
        contents.println("<th>Monthly</th>");
        contents.println("<th>Yearly</th>");
        contents.println("</tr>");
        List<String> type =  BillingHistory.getTYPE();
        for (int t = 0; t < type.size(); t++) {
            printRow(contents, BillingHistory.getTITLE(t), type.get(t));
        }
        contents.println("</table>");
        contents.addFooter(this.getClass().getName());
        contents.flush();
    }

    /**
     * Generates row in the table containing link to the image file for plot.
     *
     * @param contents
     * @param title
     * @param type
     */
    private void printRow(HTMLWriter contents, String title, String type) {
        contents.println("<tr>");
        contents.println("<td align='right'><i>" + title + "</i></td>");
        String prefix =  subDir + fileSep + type;
        for (String tag : BillingHistory.getEXT()) {
            String file = prefix + tag + imgExt;
            contents.println("<td><a href='" + file + "'> <img src='" + file
                            + "' width=" + THUMBNAIL_W + " height="
                            + THUMBNAIL_H
                            + "> </a></td>");
        }
        contents.println("</tr>");
    }
}
