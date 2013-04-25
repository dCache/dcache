package org.dcache.services.billing.html;

import org.dcache.services.billing.plots.BillingHistory;

import diskCacheV111.util.HTMLWriter;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet stand-in {@link org.dcache.services.httpd.HttpServiceCell}. <br>
 * <br>
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
    private static final String GENERATE_PLOTS = "generatePlots";

    private CellNucleus nucleus;
    private final boolean generatePlots;
    private Thread _billingHistory;

    public HttpBillingHistoryEngine(CellNucleus nucleus, String[] args) {
        super(new Args(args));
        this.nucleus = nucleus;
        generatePlots = Boolean.valueOf(getArgs().getOpt(GENERATE_PLOTS));
        if (generatePlots) {
            _billingHistory = nucleus.newThread(this, "billing-history");
        }
    }

    @Override
    public void startup() {
        if (generatePlots) {
            _billingHistory.start();
        }
    }

    @Override
    public void shutdown() {
        if (generatePlots) {
            setRunning(false);
            _billingHistory.interrupt();
            try {
                _billingHistory.join();
            } catch (InterruptedException e) {
                _log.warn("Interrupted while waiting for BillingHistory thread to terminate");
            }
        }
        close();
    }

    @Override
    public void queryUrl(HttpRequest request) throws HttpException {
        request.printHttpHeader(0);
        emit(new HTMLWriter(request.getOutputStream(),
                        this.nucleus.getDomainContext()));
    }

    public void emit(HTMLWriter contents) {
        contents.addHeader("/styles/common.css", "Billing History Plots");
        if (!generatePlots) {
            printWarning(contents);
        } else {
            contents.println("<table align='center'>");
            contents.println("<tr>");
            contents.println("<th>Plot Type</th>");
            contents.println("<th>24-Hour</th>");
            contents.println("<th>Weekly</th>");
            contents.println("<th>Monthly</th>");
            contents.println("<th>Yearly</th>");
            contents.println("</tr>");
            for (int t = 0; t < BillingHistory.TYPE.length; t++) {
                printRow(contents, BillingHistory.TITLE[t],
                                BillingHistory.TYPE[t]);
            }
            contents.println("</table>");
        }
        contents.addFooter(this.getClass().getName());
        contents.flush();
    }

    /**
     * Generates warning message that plots are turned off.
     */
    private void printWarning(HTMLWriter contents) {
        contents.println("<table align='center'>");
        contents.println("<tr><td>");
        contents.println("<h2>Billing Plots Have been Disabled</h2>");
        contents.println("</td></tr>");
        contents.println("</table>");
    }

    /**
     * Generates row in the table containing link to the image file for plot.
     */
    private void printRow(HTMLWriter contents, String title, String type) {
        contents.println("<tr>");
        contents.println("<td align='right'><i>" + title + "</i></td>");
        String prefix = subDir + fileSep + type;
        for (String tag : BillingHistory.EXT) {
            String file = prefix + tag + imgExt;
            contents.println("<td><a href='" + file + "'> <img src='" + file
                            + "' width=" + THUMBNAIL_W + " height="
                            + THUMBNAIL_H + "> </a></td>");
        }
        contents.println("</tr>");
    }
}
