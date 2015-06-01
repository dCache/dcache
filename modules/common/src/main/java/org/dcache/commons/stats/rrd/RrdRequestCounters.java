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
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.dcache.commons.stats.RequestCounter;
import org.dcache.commons.stats.RequestCounterImpl;
import org.dcache.commons.stats.RequestCounters;

/**
 *  a utility class that creates and manages rrd databases (using RRDRequestCounter)
 *  and plots graphs for RequestCounters collection of request counters
 * @param <T> a type of key inherited from RequestCounters
 * @author timur
 */
public class RrdRequestCounters<T> {
    private static final Logger logger = LoggerFactory.getLogger(RrdRequestCounters.class);
    private final Map<T,RRDRequestCounter> rrdcounters =
            new HashMap<>();
    private RRDRequestCounter totalRequestCounter;
    private final RequestCounters<T> requestCounters;
    private static final Timer rrdTimer = new Timer("RrdRequestCounters",true);
    private final String rrdDir;
    private TimerTask updateRrd ;
    private TimerTask updateRrdGraphs;
    private long updatePeriodSecs = 60L; //one minute by default
    private long graphPeriodSecs = 5*60L; //five minutes by default

    /**
     * creates new instance of RrdRequestCounters
     *  you will need to call startRrdUpdates to begin collecting data
     *  and startRrdGraphPlots to being automatically updating images
     *  intead it is possible to plot on demand by calling
     * plotGraphs
     * @param requestCounters from which counters will be logged and plotted
     * @param rrdDir place where to put rrd database files and images
     */
    public RrdRequestCounters(RequestCounters<T> requestCounters,
        String rrdDir)  throws IOException {
        this(requestCounters,rrdDir,60L,5*60L);
    }

    /**
     * creates new instance of RrdRequestCounters
     *  you will need to call startRrdUpdates to begin collecting data
     *  and startRrdGraphPlots to being automatically updating images
     *  intead it is possible to plot on demand by calling
     * plotGraphs
     * @param requestCounters  from which counters will be logged and plotted
     * @param rrdDir  place where to put rrd database files and images
     * @param updatePeriodSecs specifies how often to read counters
     * @param graphPeriodSecs specifies how often to plot images
     */
    public RrdRequestCounters(RequestCounters<T> requestCounters,
            String rrdDir,long updatePeriodSecs,
            long graphPeriodSecs) throws IOException {
        logger.debug("RrdRequestCounters("+requestCounters+", "+rrdDir);
        this.requestCounters = requestCounters;
        this.rrdDir = rrdDir;
        File dir = new File(rrdDir);
        if(!dir.exists()) {
            dir.mkdir() ;
        }
        if(!dir.exists() || !dir.canRead() ||
           !dir.canWrite() || !dir.canExecute()) {
            throw new AccessControlException("directory "+
                    dir  + " does not exists or is not accessable");
        }
        this.updatePeriodSecs = updatePeriodSecs;
        this.graphPeriodSecs = graphPeriodSecs;
        totalRequestCounter =
                new RRDRequestCounter(rrdDir,
                requestCounters.getTotalRequestCounter(),
                updatePeriodSecs);
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
                logger.debug("RrdRequestCounters updateRrd running updateRrds()");
                try {
                    updateRrds() ;
                    logger.debug("RrdRequestCounters updateRrd updateRrds() is done");
                } catch (Throwable t) {
                    logger.error("updateRrds",t);
                }
            }
        };
        rrdTimer.schedule(updateRrd, updatePeriodSecs*1000L,updatePeriodSecs*1000L);
    }

    private void updateRrds() throws IOException {
        boolean countersAdded = false;
        logger.debug("updateRrds() for "+requestCounters);
        synchronized (requestCounters) {
            for(T key:requestCounters.keySet()) {
                logger.debug("updatePrds(): key is "+key);
                if(!rrdcounters.containsKey(key)) {
                    RequestCounter requestCounter = requestCounters.getCounter(key);
                    logger.debug("updatePrds(): creating RRDRequestCounter for "+requestCounter);
                    RRDRequestCounter rrdRequestCounter =
                            new RRDRequestCounter(rrdDir,requestCounter,updatePeriodSecs);
                    rrdcounters.put(key, rrdRequestCounter);
                    countersAdded = true;
                }
            }
        }

        for(RRDRequestCounter rrdRequestCounter:rrdcounters.values() ) {
            logger.debug("updateRrds(): calling rrdRequestCounter.update()");
            rrdRequestCounter.update();
        }
        logger.debug("updateRrds(): calling totalRequestCounter.update()");
        totalRequestCounter.update();
        if(countersAdded)  {
            updateIndex();
        }
    }

    /**
     *
     * @return keyset
     */
    public Set<T> keySet() {
        return rrdcounters.keySet();
    }

        /**
     *
     * @param counterKey a key corresponding to a counter
     * @return a RequestCounter associated with counterKey
     * @throws  NoSuchElementException if counter for counterKey is not defined
     */
    public RRDRequestCounter getRrdCounter(T key) {
        synchronized(this) {
            if(rrdcounters.containsKey(key)) {
                return rrdcounters.get(key);
            } else {
                throw new NoSuchElementException("counter with name "+
                        key+" is not defined in rrdcounters" );
            }
        }
    }

    /**
     * plots graphs
     * @throws IOException
     */
    public void plotGraphs() throws IOException {
        for(T key:rrdcounters.keySet()) {
            RRDRequestCounter rrdRequestCounter = rrdcounters.get(key);
            logger.debug("plotGraphs(): calling rrdRequestCounter.graph()");
            rrdRequestCounter.graph();
            logger.debug("plotGraphs(): rrdRequestCounter.graph() returned");
        }
        logger.debug("plotGraphs(): calling totalRequestCounter.graph()");
        totalRequestCounter.graph();
        logger.debug("plotGraphs(): totalRequestCounter.graph() returned");

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
                    logger.debug("RrdRequestCounters updateRrd running plotGraphs()");
                    plotGraphs() ;
                } catch(Throwable t) {
                    logger.error("plotGraphs", t);
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
        FileWriter fw = new FileWriter(index);
        fw.write(indexHtml);
        fw.close();

    }
    private String getIndexHtml() {
        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">\n");
        sb.append("<html>\n");
        sb.append("<head>\n");
        sb.append("  <meta content=\"text/html; charset=ISO-8859-1\"\n");
        sb.append(" http-equiv=\"content-type\">\n");
        sb.append("  <title>Request Rate Graphs Index for ");
        sb.append(requestCounters.getName());
        sb.append(" </title>\n");
        sb.append("</head>\n");
        sb.append("<body>\n");
        sb.append("  <h1>Request Rate Graphs Index for ");
        sb.append(requestCounters.getName());
        sb.append(" </h1>\n");
        sb.append("  <table> \n");
        sb.append("  <tr> <td> \n");
        for(T key:rrdcounters.keySet()) {
            sb.append("  <tr> <td> \n");
            String nextHtml = rrdcounters.get(key).getRrdGraphicsHtmlFileName();
            sb.append("<a href=\"");
            sb.append(nextHtml);
            sb.append("\">");
            sb.append(nextHtml);
            sb.append("</a>\n");
            sb.append("  </td> </tr> \n");
        }
        sb.append("  <tr> <td> \n");
        String nextHtml = totalRequestCounter.getRrdGraphicsHtmlFileName();
        sb.append("<a href=\"");
        sb.append(nextHtml);
        sb.append("\">");
        sb.append(nextHtml);
        sb.append("</a>\n");
        sb.append("  </td> </tr> \n");

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

        String dir = args[0];
        RequestCounters<String> rcs = new RequestCounters<>(dir);
        Random r = new Random();
        rcs.incrementRequests("counter1",r.nextInt(100));
        rcs.incrementRequests("counter2",r.nextInt(100));
        RrdRequestCounters<String> rrdrcs =
                new RrdRequestCounters<>(rcs,dir,10,20);
        rrdrcs.startRrdUpdates();
        rrdrcs.startRrdGraphPlots();
        while(true) {
                Thread.sleep(10000);
                rcs.incrementRequests("counter1",r.nextInt(300));
                rcs.incrementFailed("counter1",r.nextInt(200));
                rcs.incrementRequests("counter2",r.nextInt(300));
                rcs.incrementFailed("counter2",r.nextInt(200));
                System.out.println("updated counters:\n"+rcs);

        }
    }

    public  RRDRequestCounter getTotalRequestCounter() {
        return totalRequestCounter;
    }
}
