/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.commons.stats.rrd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.dcache.commons.stats.RequestExecutionTimeGauge;
import org.dcache.commons.stats.RequestExecutionTimeGauges;

/**
 *  a utility class that creates and manages rrd databases (using RRDRequestExecutionTimeGauge)
 *  and plots graphs for RequestExecutionTimeGauges collection of request counters
 * @param <T> a type of key inherited from RequestExecutionTimeGauges
 * @author timur
 */
public class RrdRequestExecutionTimeGauges<T> {
    private static final Logger logger = LoggerFactory.getLogger(RrdRequestExecutionTimeGauges.class);
    private final Map<T,RRDRequestExecutionTimeGauge> rrdgauges =
            new HashMap<>();
    private final RequestExecutionTimeGauges<T> gauges;
    private static final Timer rrdTimer = new Timer("RrdRequestGauges",true);
    private final File rrdDir;
    private TimerTask updateRrd ;
    private TimerTask updateRrdGraphs;
    private long updatePeriodSecs = 60L; //one minute by default
    private long graphPeriodSecs = 5*60L; //five minutes by default

    /**
     * creates new instance of RrdRequestGauges
     *  you will need to call startRrdUpdates to begin collecting data
     *  and startRrdGraphPlots to being automatically updating images
     *  intead it is possible to plot on demand by calling
     * plotGraphs
     * @param requestCounters from which counters will be logged and plotted
     * @param rrdDir place where to put rrd database files and images
     */
    public RrdRequestExecutionTimeGauges(RequestExecutionTimeGauges<T> requestGauges,
        File rrdDir)  throws IOException {
        this(requestGauges,rrdDir,60L,5*60L);
    }

    /**
     * creates new instance of RrdRequestGauges
     *  you will need to call startRrdUpdates to begin collecting data
     *  and startRrdGraphPlots to being automatically updating images
     *  intead it is possible to plot on demand by calling
     * plotGraphs
     * @param requestCounters  from which counters will be logged and plotted
     * @param rrdDir  place where to put rrd database files and images
     * @param updatePeriodSecs specifies how often to read counters
     * @param graphPeriodSecs specifies how often to plot images
     */
    public RrdRequestExecutionTimeGauges(RequestExecutionTimeGauges<T> requestGauges,
            File rrdDir,long updatePeriodSecs,
            long graphPeriodSecs) throws IOException {
        logger.debug("RrdRequestGauges("+requestGauges+", "+rrdDir);
        this.gauges = requestGauges;
        this.rrdDir = rrdDir;
        if(!rrdDir.exists()) {
            rrdDir.mkdir() ;
        }
        if(!rrdDir.exists() || !rrdDir.canRead() ||
           !rrdDir.canWrite() || !rrdDir.canExecute()) {
            throw new AccessControlException("directory "+
                    rrdDir  + " does not exists or is not accessable");
        }
        this.updatePeriodSecs = updatePeriodSecs;
        this.graphPeriodSecs = graphPeriodSecs;
        updateIndex();

    }

    /**
     * starts data collection
     */
    public synchronized void startRrdUpdates() {
        if(updateRrd != null) {
            throw new IllegalStateException("RRD Updates are started");
        }
        updateRrd = new TimerTask() {
            @Override
            public void run() {
                logger.debug("RrdRequestGauges updateRrd running updateRrds()");
                try {
                    updateRrds() ;
                    logger.debug("RrdRequestGauges updateRrd updateRrds() is done");
                } catch (IOException ioe) {
                    logger.error("updateRrds io exception : ",ioe);
                }
            }
        };
        rrdTimer.schedule(updateRrd, updatePeriodSecs*1000L,updatePeriodSecs*1000L);
    }

    private void updateRrds() throws IOException {
        boolean gaugesAdded = false;
        logger.debug("updateRrds() for "+gauges);
        synchronized (gauges) {
            for(T key:gauges.keySet()) {
                logger.debug("updatePrds(): key is "+key);
                if(!rrdgauges.containsKey(key)) {
                    RequestExecutionTimeGauge requestGauges = gauges.getGauge(key);
                    logger.debug("updatePrds(): creating RRDRequestExecutionTimeGauge for "+requestGauges);
                    RRDRequestExecutionTimeGauge rrdRequestGauges =
                            new RRDRequestExecutionTimeGauge(rrdDir,requestGauges,updatePeriodSecs);
                    rrdgauges.put(key, rrdRequestGauges);
                    gaugesAdded = true;
                }
            }
        }

        for(RRDRequestExecutionTimeGauge gauge : rrdgauges.values() ) {
            logger.debug("updateRrds(): calling gauge.update()");
            gauge.update();
        }
        logger.debug("updateRrds(): calling totalRequestCounter.update()");
        if(gaugesAdded)  {
            updateIndex();
        }
    }

    /**
     * plots graphs
     * @throws IOException
     */
    public void plotGraphs() throws IOException {
        for(RRDRequestExecutionTimeGauge gauge : rrdgauges.values()) {
            gauge.graph();
        }
    }

    /**
     * starts automatic graphs updates
     */
    public synchronized void startRrdGraphPlots() {
        if(updateRrdGraphs != null) {
            throw new IllegalStateException("RRD Graph Updates are started");
        }
        updateRrdGraphs = new TimerTask() {
            @Override
            public void run() {
                try {
                    logger.debug("RrdRequestGauges updateRrd running plotGraphs()");
                    plotGraphs() ;
                } catch(IOException ioe) {
                    logger.error("plotGraphs io exception : ", ioe);
                }
            }
        };
        rrdTimer.schedule(updateRrdGraphs, updatePeriodSecs*1000L,graphPeriodSecs*1000L);
    }

    /**
     * stops data collection
     */
    public synchronized void stopRrdUpdates() {
        if(updateRrd != null) {
            updateRrd.cancel();
            updateRrd = null;
        }
    }

    /**
     * stop  automatic graphs updates
     */
    public synchronized void stopRrdGraphPlots() {
        if(updateRrdGraphs != null) {
            updateRrdGraphs.cancel();
            updateRrdGraphs = null;
        }

    }

    private void updateIndex() throws IOException {
        File index = new File(rrdDir,"index.html");
        String indexHtml = getIndexHtml();
        try (FileWriter fw = new FileWriter(index)) {
            fw.write(indexHtml);
        }


    }
    private String getIndexHtml() {
        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">\n");
        sb.append("<html>\n");
        sb.append("<head>\n");
        sb.append("  <meta content=\"text/html; charset=ISO-8859-1\"\n");
        sb.append(" http-equiv=\"content-type\">\n");
        sb.append("  <title>Request Execution Time Graphs Index for ");
        sb.append(gauges.getName());
        sb.append(" </title>\n");
        sb.append("</head>\n");
        sb.append("<body>\n");
        sb.append("  <h1>Request Execution Time Graphs Index for ");
        sb.append(gauges.getName());
        sb.append(" </h1>\n");
        sb.append("  <table> \n");
        sb.append("  <tr> <td> \n");
        for(RRDRequestExecutionTimeGauge gauge : rrdgauges.values()) {
            sb.append("  <tr> <td> \n");
            String nextHtml = gauge.getRrdGraphicsHtmlFileName();
            sb.append("<a href=\"");
            sb.append(nextHtml);
            sb.append("\">");
            sb.append(nextHtml);
            sb.append("</a>\n");
            sb.append("  </td> </tr> \n");
        }
        sb.append("</body>");
        sb.append("</html>");
        return sb.toString();

    }
    /**
     *  runs tests of the rrdRequestCouters
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{

        String  dirname = args[0];
        RequestExecutionTimeGauges<String> retg = new RequestExecutionTimeGauges<>(dirname);
        Random r = new Random();
        retg.update("gauge1",r.nextInt(100));
        retg.update("gauge2",r.nextInt(100));
        File dir = new File(dirname);
        RrdRequestExecutionTimeGauges<String> rrdrcs =
                new RrdRequestExecutionTimeGauges<>(retg,dir,10,20);
        rrdrcs.startRrdUpdates();
        rrdrcs.startRrdGraphPlots();
        while(true) {
                Thread.sleep(10000);
                retg.update("gauge1",r.nextInt(300));
                retg.update("gauge2",r.nextInt(300));
                System.out.println("updated gauges:\n"+retg);

        }
    }
}
