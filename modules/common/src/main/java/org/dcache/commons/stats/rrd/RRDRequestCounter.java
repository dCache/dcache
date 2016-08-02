/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.commons.stats.rrd;

import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.FetchData;
import org.rrd4j.core.FetchRequest;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.Sample;
import org.rrd4j.core.Util;
import org.rrd4j.graph.RrdGraph;
import org.rrd4j.graph.RrdGraphDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.concurrent.TimeUnit;

import org.dcache.commons.stats.RequestCounter;


/**
 * utility class for logging the request counters into rrd
 *  and plotting graphs
 * @author timur
 */
public class RRDRequestCounter {
    private static final long fivemin = TimeUnit.MINUTES.toSeconds( 5);
    private static final long tenmin = TimeUnit.MINUTES.toSeconds( 5);
    private static final long hour = TimeUnit.HOURS.toSeconds(1);
    private static final long day =  TimeUnit.DAYS.toSeconds(1);
    private static final long month = TimeUnit.DAYS.toSeconds(31);
    private static final long year = TimeUnit.DAYS.toSeconds(365);
    private static final int DEFAULT_IMAGE_WIDTH=491;
    private static final int DEFAULT_IMAGE_HEIGHT=167;


    private static final Logger logger = LoggerFactory.getLogger(RRDRequestCounter.class);
    private final RequestCounter counter;

    private final String rrdFileName;
    private final String rrdFiveminImage;
    private final String rrdHourlyImage;
    private final String rrdDaylyImage;
    private final String rrdMounthlyImage;
    private final String rrdYearlyImage;
    private final String rrdGraphicsHtmlFileName;

    private int imageWidth = DEFAULT_IMAGE_WIDTH;
    private int imageHeight = DEFAULT_IMAGE_HEIGHT;


    /**
     *
     * @param rrdDirectory dir where rdd dbs and and images will be created
     * @param counter the counter logged and plotted by this RRDRequestCounter
     * @param updatePeriodSecs how often counters will be updated
     *        note that RRDRequestCounter does not call update
     *        it relies on external service to call it periodically
     * @param imageWidth width of the images generated
     * @param imageHeight height of the images generated
     * @throws IOException
     */
    public RRDRequestCounter(String rrdDirectory, RequestCounter counter,
            long updatePeriodSecs ) throws
            IOException
    {
        this(rrdDirectory,counter,updatePeriodSecs,DEFAULT_IMAGE_WIDTH,DEFAULT_IMAGE_HEIGHT);
    }

    /**
     *
     * @param rrdDirectory dir where rdd dbs and and images will be created
     * @param counter the counter logged and plotted by this RRDRequestCounter
     * @param updatePeriodSecs  updatePeriodSecs how often counters will be updated
     *        note that RRDRequestCounter does not call update
     *        it relies on external service to call it periodically
     * @throws IOException
     */
    public RRDRequestCounter(String rrdDirectory, RequestCounter counter,
            long updatePeriodSecs,int imageWidth, int imageHeight) throws
            IOException
    {
        this.imageWidth = imageWidth;
        this.imageHeight = imageHeight;
        if(updatePeriodSecs <=0 || updatePeriodSecs >=fivemin) {
            throw new IllegalArgumentException("updatePeriodSecs="+updatePeriodSecs+
                    ", should be greater than 0 and less than "+fivemin+" secs");
        }
        logger.debug("RRDRequestCounter(" + rrdDirectory + ", " + counter + ',' + updatePeriodSecs + ')');
        File dir = new File(rrdDirectory);
        if(!dir.exists() || !dir.isDirectory() || !dir.canWrite() ) {
            throw new AccessControlException("directory "+
                    rrdDirectory + " does not exists or is not accessable");
        }
        File imagesDir = new File(dir,"images");
        if(!imagesDir.exists()) {
            imagesDir.mkdir() ;
        }
        if(!imagesDir.exists() || !imagesDir.isDirectory() ||
           !imagesDir.canWrite() ) {
            throw new AccessControlException("directory "+
                    imagesDir + " does not exists or is not accessable");
        }
        String rrdImageDir = imagesDir.getCanonicalPath();
        String counterName = counter.getName();
        rrdFiveminImage = rrdImageDir+File.separatorChar+counterName+".5min.png";
        rrdHourlyImage = rrdImageDir+File.separatorChar+counterName+".hour.png";
        rrdDaylyImage = rrdImageDir+File.separatorChar+counterName+".day.png";
        rrdMounthlyImage = rrdImageDir+File.separatorChar+counterName+".month.png";
        rrdYearlyImage = rrdImageDir+File.separatorChar+counterName+".year.png";
        File f = new File(dir,counterName+".rrd4j");
        this.rrdFileName = f.getCanonicalPath();
        if(!f.exists()) {
            RrdDef rrdDef = new RrdDef(this.rrdFileName);
            rrdDef.setStartTime( Util.getTime()-1);
            rrdDef.setStep(updatePeriodSecs); // step is 60 seconds or one minute
            // use derive to eliminate the false jumps in values due to
            // counter resets (restarts)
            rrdDef.addDatasource("requests",DsType.DERIVE,updatePeriodSecs*2,0, Double.NaN);
            rrdDef.addDatasource("failed",  DsType.DERIVE,updatePeriodSecs*2,0, Double.NaN);

            //one sample for every period of  updatePeriodSecs
            // for one hour
            int samplesPerOur =(int) (hour / updatePeriodSecs );
            rrdDef.addArchive(ConsolFun.AVERAGE, 0.5d, 1, samplesPerOur);
            rrdDef.addArchive(ConsolFun.MIN,     0.5d, 1, samplesPerOur);
            rrdDef.addArchive(ConsolFun.MAX,     0.5d, 1, samplesPerOur);

            //sample for every 10 min period of  updatePeriodSecs
            // for 100 hours
            int samplesPerTenMin = (int) (tenmin / updatePeriodSecs);
            int tenMinSamplesPer100Hours =(int)(hour*100/samplesPerTenMin);
            rrdDef.addArchive(ConsolFun.AVERAGE, 0.5d, samplesPerTenMin, tenMinSamplesPer100Hours);
            rrdDef.addArchive(ConsolFun.MIN,     0.5d, samplesPerTenMin, tenMinSamplesPer100Hours);
            rrdDef.addArchive(ConsolFun.MAX,     0.5d, samplesPerTenMin, tenMinSamplesPer100Hours);

            //sample for every 1 hour period of  updatePeriodSecs
            // for one month
            int hourSamplesPerMonth =(int)(month/samplesPerOur);
            rrdDef.addArchive(ConsolFun.AVERAGE, 0.5d, samplesPerOur, hourSamplesPerMonth);
            rrdDef.addArchive(ConsolFun.MIN, 0.5d, samplesPerOur, hourSamplesPerMonth);
            rrdDef.addArchive(ConsolFun.MAX, 0.5d, samplesPerOur, hourSamplesPerMonth);

            //one sample for every one day period of  updatePeriodSecs
            // for two years
            int samplesPerDay =(int) (24*hour / updatePeriodSecs );
            int daySamplesPer2Years =(int)(2*year/samplesPerDay);
            rrdDef.addArchive(ConsolFun.AVERAGE, 0.5d, samplesPerDay, daySamplesPer2Years);
            rrdDef.addArchive(ConsolFun.MIN, 0.5d, samplesPerDay, daySamplesPer2Years);
            rrdDef.addArchive(ConsolFun.MAX, 0.5d, samplesPerDay, daySamplesPer2Years);

            RrdDb rrdDb = new RrdDb(rrdDef);
            rrdDb.close();
        }

        RrdDb rrdDb = new RrdDb(this.rrdFileName);// check that we can read it

        // we could use rrdDb.getRrdDef() to make sure that this is a correct rrd
        rrdDb.close();
        this.counter = counter;
        File html = new File(rrdDirectory,counterName+".html");
        if(!html.exists()) {
            String graphicsHtml = getGraphicsHtml(counterName,imageWidth,imageHeight);
            FileWriter fw = new FileWriter(html);
            fw.write(graphicsHtml);
            fw.close();
        }
        rrdGraphicsHtmlFileName =counterName+".html";
    }
    Long dumpstart;
    /**
     *
     * @throws IOException
     */
    public void update() throws IOException {

        logger.debug("RRDRequestCounter.update() rrdFileName is "+rrdFileName);
        RrdDb rrdDb = new RrdDb(rrdFileName);
        Sample sample = rrdDb.createSample();
        long currentTimeSecs =   Util.getTime();
        StringBuilder sb = new StringBuilder();
        sb.append(currentTimeSecs).append(':');
        sb.append(counter.getTotalRequests()).append(':');
        sb.append(counter.getFailed());
        sample.setAndUpdate(sb.toString());
        logger.debug("RRDRequestCounter.update() updated with : " + sb);
        rrdDb.close();
        logger.debug("RRDRequestCounter.update() succeeded");
        logger.debug("RRDRequestCounter.update() let us try to fetch data");
        rrdDb = new RrdDb(rrdFileName);
        if(dumpstart == null) {
                  dumpstart = currentTimeSecs - 100;
        }
        FetchRequest fetchRequest = rrdDb.createFetchRequest(ConsolFun.AVERAGE, dumpstart, currentTimeSecs+1);
        FetchData fetchData = fetchRequest.fetchData();
        logger.debug("RRDRequestCounter.update() dump is: "+fetchData.dump());
        rrdDb.close();
    }


    /**
     *
     * @throws IOException
     */
    public void graph() throws IOException {

        long currentTime = Util.getTime();
        logger.debug("RRDRequestCounter.graph()");
        RrdDb rrdDb = new RrdDb(rrdFileName);

        RrdGraphDef graphDef = new RrdGraphDef();
        graphDef.setVerticalLabel("requests/sec");
        graphDef.setUnit("r");
        graphDef.datasource("requests_avg", rrdFileName, "requests", ConsolFun.AVERAGE);
        graphDef.datasource("failed_avg", rrdFileName, "failed", ConsolFun.AVERAGE);

        graphDef.line("requests_avg", new Color(0xBB, 0, 0), "requests_avg", 4);
        graphDef.line("failed_avg", new Color(0, 0xBB, 0), "failed_avg", 4);

        graphDef.datasource("requests_max", rrdFileName, "requests", ConsolFun.MAX);
        graphDef.datasource("failed_max", rrdFileName, "failed", ConsolFun.MAX);

        graphDef.line("requests_max", new Color(0xFF, 0, 0), "requests_max", 3);
        graphDef.line("failed_max", new Color(0, 0xFF, 0), "failed_max", 3);

        graphDef.datasource("requests_min", rrdFileName, "requests", ConsolFun.MIN);
        graphDef.datasource("failed_min", rrdFileName, "failed", ConsolFun.MIN);

        graphDef.line("requests_min", new Color(0x90, 0x20, 0x20), "requests_min", 2);
        graphDef.line("failed_min", new Color(0x20, 0x90, 0x20), "failed_min", 2);
        //hour
        //graphDef.setStartTime(-hour);
        //#graphDef.setStartTime(-hour);

        graphDef.setTimeSpan(currentTime-fivemin, currentTime);
        graphDef.setFilename(rrdFiveminImage);
        RrdGraph graph = new RrdGraph(graphDef);
        BufferedImage bi = new BufferedImage(imageWidth,imageHeight,BufferedImage.TYPE_INT_RGB);
        graph.render(bi.getGraphics());
        logger.debug("RRDRequestCounter.graph() wrote "+rrdHourlyImage);


        graphDef.setTimeSpan(currentTime-hour, currentTime);
        graphDef.setFilename(rrdHourlyImage);
        graph = new RrdGraph(graphDef);
        bi = new BufferedImage(imageWidth,imageHeight,BufferedImage.TYPE_INT_RGB);
        graph.render(bi.getGraphics());
        logger.debug("RRDRequestCounter.graph() wrote "+rrdHourlyImage);

        //day
        //graphDef.setStartTime(-day);
        graphDef.setTimeSpan(currentTime-day, currentTime);
        graphDef.setFilename(rrdDaylyImage);
        graph = new RrdGraph(graphDef);
        bi = new BufferedImage(imageWidth,imageHeight,BufferedImage.TYPE_INT_RGB);
        graph.render(bi.getGraphics());

        //month
        //graphDef.setStartTime(-month);
        graphDef.setTimeSpan(currentTime-month, currentTime);
        graphDef.setFilename(rrdMounthlyImage);
        graph = new RrdGraph(graphDef);
        bi = new BufferedImage(imageWidth,imageHeight,BufferedImage.TYPE_INT_RGB);
        graph.render(bi.getGraphics());

        //year
        //graphDef.setStartTime(-year);
        graphDef.setTimeSpan(currentTime-year, currentTime);
        graphDef.setFilename(rrdYearlyImage);
        graph = new RrdGraph(graphDef);
        bi = new BufferedImage(imageWidth,imageHeight,BufferedImage.TYPE_INT_RGB);
        graph.render(bi.getGraphics());
    }

    /**
     * @return the imageWidth
     */
    public int getImageWidth() {
        return imageWidth;
    }

    /**
     * @param imageWidth the imageWidth to set
     */
    public void setImageWidth(int imageWidth) {
        this.imageWidth = imageWidth;
    }

    /**
     * @return the imageHight
     */
    public int getImageHight() {
        return imageHeight;
    }

    /**
     * @param imageHight the imageHight to set
     */
    public void setImageHight(int imageHight) {
        this.imageHeight = imageHight;
    }


private static String getGraphicsHtml( String counterName, int width, int height) {

        StringBuilder sb = new StringBuilder("\"width: ");
        sb.append(width);
        sb.append("px; height: ");
        sb.append(height);
        sb.append("px;\"");
        String style= sb.toString();
        sb = new StringBuilder();
        sb.append("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">\n");
        sb.append("<html>\n");
        sb.append("<head>\n");
        sb.append("  <meta content=\"text/html; charset=ISO-8859-1\"\n");
        sb.append(" http-equiv=\"content-type\">\n");
        sb.append(" <title>Request Rate Graphics for ");
        sb.append(counterName);
        sb.append("</title>\n");
        sb.append("</head>\n");
        sb.append("<body>\n");
        sb.append("<h1> Request Rate Graphics for ");
        sb.append(counterName);
        sb.append("</h1> \n");
        sb.append("5 Minutes<br>\n");
        sb.append("<img style=" );
        sb.append(style);
        sb.append(" alt=\"5 Minutes\"\n");
        sb.append(" src=\"images/");
        sb.append(counterName);
        sb.append(".5min.png\"><br>\n");
        sb.append("Hour<br>\n");
        sb.append("<img style=" );
        sb.append(style);
        sb.append(" alt=\"Hour\"\n");
        sb.append(" src=\"images/");
        sb.append(counterName);
        sb.append(".hour.png\"><br>\n");
        sb.append("Day<br>\n");
        sb.append("<img style=" );
        sb.append(style);
        sb.append(" alt=\"Day\"\n");
        sb.append(" src=\"images/");
        sb.append(counterName);
        sb.append(".day.png\"><br>\n");
        sb.append("Month<br>\n");
        sb.append("<img style=" );
        sb.append(style);
        sb.append(" alt=\"Month\"\n");
        sb.append(" src=\"images/");
        sb.append(counterName);
        sb.append(".month.png\"><br>\n");
        sb.append("Year<br>\n");
        sb.append("<img style=" );
        sb.append(style);
        sb.append(" alt=\"Year\"\n");
        sb.append(" src=\"images/");
        sb.append(counterName);
        sb.append(".year.png\"><br>\n");
        sb.append("</body>\n");
        sb.append("</html>");
        return sb.toString();

}

    /**
     * @return the rrdGraphicsHtmlFileName
     */
    public String getRrdGraphicsHtmlFileName() {
        return rrdGraphicsHtmlFileName;
    }

    public String getName() {
        return counter.getName();
    }

}
