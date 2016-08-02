/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.commons.stats.rrd;

import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
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

import org.dcache.commons.stats.RequestExecutionTimeGauge;


/**
 * utility class for logging the request gauges into rrd
 *  and plotting graphs
 * @author timur
 */
public class RRDRequestExecutionTimeGauge {
    private static final long FIVEMIN = TimeUnit.MINUTES.toSeconds( 5);
    private static final long TENMIN = TimeUnit.MINUTES.toSeconds( 5);
    private static final long HOUR = TimeUnit.HOURS.toSeconds(1);
    private static final long DAY =  TimeUnit.DAYS.toSeconds(1);
    private static final long MONTH = TimeUnit.DAYS.toSeconds(31);
    private static final long YEAR = TimeUnit.DAYS.toSeconds(365);
    private static final int DEFAULT_IMAGE_WIDTH=491;
    private static final int DEFAULT_IMAGE_HEIGHT=167;


    private static final Logger logger = LoggerFactory.getLogger(RRDRequestExecutionTimeGauge.class);
    private final RequestExecutionTimeGauge gauge;

    private final String rrdFileName;
    private final String rrdFiveminImage;
    private final String rrdHourlyImage;
    private final String rrdDaylyImage;
    private final String rrdMounthlyImage;
    private final String rrdYearlyImage;
    private final String rrdGraphicsHtmlFileName;

    private int imageWidth = DEFAULT_IMAGE_WIDTH;
    private int imageHeight = DEFAULT_IMAGE_HEIGHT;

    private Long dumpstart;

    /**
     *
     * @param rrdDirectory dir where rdd dbs and and images will be created
     * @param gauge the gauge logged and plotted by this RRDRequestExecutionTimeGauge
     * @param updatePeriodSecs how often gauges will be updated
     *        note that RRDRequestExecutionTimeGauge does not call update
     *        it relies on external service to call it periodically
     * @param imageWidth width of the images generated
     * @param imageHeight height of the images generated
     * @throws IOException
     */
    public RRDRequestExecutionTimeGauge(File rrdDirectory, RequestExecutionTimeGauge gauge,
            long updatePeriodSecs ) throws
            IOException
    {
        this(rrdDirectory,gauge,updatePeriodSecs,DEFAULT_IMAGE_WIDTH,DEFAULT_IMAGE_HEIGHT);
    }

    /**
     *
     * @param rrdDirectory dir where rdd dbs and and images will be created
     * @param gauge the gauge logged and plotted by this RRDRequestExecutionTimeGauge
     * @param updatePeriodSecs  updatePeriodSecs how often gauges will be updated
     *        note that RRDRequestExecutionTimeGauge does not call update
     *        it relies on external service to call it periodically
     * @throws IOException
     */
    public RRDRequestExecutionTimeGauge(File rrdDirectory, RequestExecutionTimeGauge gauge,
            long updatePeriodSecs,int imageWidth, int imageHeight) throws
            IOException
    {
        this.imageWidth = imageWidth;
        this.imageHeight = imageHeight;
        if(updatePeriodSecs <=0 || updatePeriodSecs >=FIVEMIN) {
            throw new IllegalArgumentException("updatePeriodSecs="+updatePeriodSecs+
                    ", should be greater than 0 and less than "+FIVEMIN+" secs");
        }
        logger.debug("RRDRequestExecutionTimeGauge(" + rrdDirectory + ", " + gauge + ',' + updatePeriodSecs + ')');
        if(!rrdDirectory.exists() || !rrdDirectory.isDirectory() || !rrdDirectory.canWrite() ) {
            throw new AccessControlException("directory "+
                    rrdDirectory + " does not exists or is not accessable");
        }
        File imagesDir = new File(rrdDirectory,"images");
        if(!imagesDir.exists()) {
            imagesDir.mkdir() ;
        }
        if(!imagesDir.exists() || !imagesDir.isDirectory() ||
           !imagesDir.canWrite() ) {
            throw new AccessControlException("directory "+
                    imagesDir + " does not exists or is not accessable");
        }
        String rrdImageDir = imagesDir.getCanonicalPath();
        String gaugeName = gauge.getName();
        rrdFiveminImage = rrdImageDir+File.separatorChar+gaugeName+".5min.png";
        rrdHourlyImage = rrdImageDir+File.separatorChar+gaugeName+".hour.png";
        rrdDaylyImage = rrdImageDir+File.separatorChar+gaugeName+".day.png";
        rrdMounthlyImage = rrdImageDir+File.separatorChar+gaugeName+".month.png";
        rrdYearlyImage = rrdImageDir+File.separatorChar+gaugeName+".year.png";
        File f = new File(rrdDirectory,gaugeName+".rrd4j");
        this.rrdFileName = f.getCanonicalPath();
        if(!f.exists()) {
            RrdDef rrdDef = new RrdDef(this.rrdFileName);
            rrdDef.setStartTime( Util.getTime()-1);
            rrdDef.setStep(updatePeriodSecs); // step is 60 seconds or one minute
            // use derive to eliminate the false jumps in values due to
            // gauge resets (restarts)
            rrdDef.addDatasource("exectime",DsType.GAUGE,updatePeriodSecs*2,0, Double.NaN);

            //one sample for every period of  updatePeriodSecs
            // for one hour
            int samplesPerOur =(int) (HOUR / updatePeriodSecs );
            rrdDef.addArchive(ConsolFun.AVERAGE, 0.5d, 1, samplesPerOur);
            rrdDef.addArchive(ConsolFun.MIN,     0.5d, 1, samplesPerOur);
            rrdDef.addArchive(ConsolFun.MAX,     0.5d, 1, samplesPerOur);

            //sample for every 10 min period of  updatePeriodSecs
            // for 100 hours
            int samplesPerTenMin = (int) (TENMIN / updatePeriodSecs);
            int tenMinSamplesPer100Hours =(int)(HOUR*100/samplesPerTenMin);
            rrdDef.addArchive(ConsolFun.AVERAGE, 0.5d, samplesPerTenMin, tenMinSamplesPer100Hours);
            rrdDef.addArchive(ConsolFun.MIN,     0.5d, samplesPerTenMin, tenMinSamplesPer100Hours);
            rrdDef.addArchive(ConsolFun.MAX,     0.5d, samplesPerTenMin, tenMinSamplesPer100Hours);

            //sample for every 1 hour period of  updatePeriodSecs
            // for one month
            int hourSamplesPerMonth =(int)(MONTH/samplesPerOur);
            rrdDef.addArchive(ConsolFun.AVERAGE, 0.5d, samplesPerOur, hourSamplesPerMonth);
            rrdDef.addArchive(ConsolFun.MIN, 0.5d, samplesPerOur, hourSamplesPerMonth);
            rrdDef.addArchive(ConsolFun.MAX, 0.5d, samplesPerOur, hourSamplesPerMonth);

            //one sample for every one day period of  updatePeriodSecs
            // for two years
            int samplesPerDay =(int) (24*HOUR / updatePeriodSecs );
            int daySamplesPer2Years =(int)(2*YEAR/samplesPerDay);
            rrdDef.addArchive(ConsolFun.AVERAGE, 0.5d, samplesPerDay, daySamplesPer2Years);
            rrdDef.addArchive(ConsolFun.MIN, 0.5d, samplesPerDay, daySamplesPer2Years);
            rrdDef.addArchive(ConsolFun.MAX, 0.5d, samplesPerDay, daySamplesPer2Years);

            RrdDb rrdDb = new RrdDb(rrdDef);
            rrdDb.close();
        }

        RrdDb rrdDb = new RrdDb(this.rrdFileName);// check that we can read it

        // we could use rrdDb.getRrdDef() to make sure that this is a correct rrd
        rrdDb.close();
        this.gauge = gauge;
        File html = new File(rrdDirectory,gaugeName+".html");
        if(!html.exists()) {
            String graphicsHtml = getGraphicsHtml(gaugeName,imageWidth,imageHeight);
            FileWriter fw = new FileWriter(html);
            fw.write(graphicsHtml);
            fw.close();
        }
        rrdGraphicsHtmlFileName =gaugeName+".html";
    }
    /**
     * Performs update of the rrd with the current value of the gauge
     * @throws IOException
     */
    public void update() throws IOException {

        logger.debug("RRDRequestExecutionTimeGauge.update() rrdFileName is "+rrdFileName);
        RrdDb rrdDb = new RrdDb(rrdFileName);
        try {
            Sample sample = rrdDb.createSample();
            long currentTimeSecs =   Util.getTime();
            String update = Long.toString(currentTimeSecs) +':'+
                            (long) gauge.resetAndGetAverageExecutionTime()+':';
            sample.setAndUpdate(update);
            logger.debug("RRDRequestExecutionTimeGauge.update() updated with : "+update);

        } finally {
            rrdDb.close();
            logger.debug("RRDRequestExecutionTimeGauge.update() succeeded");
        }
    }

    private void plotGraph(RrdGraphDef graphDef,
            long beginningtime,
            long endTime,
            String filename ) throws IOException {
        graphDef.setTimeSpan(beginningtime, endTime);
        graphDef.setFilename(filename);
        RrdGraph graph = new RrdGraph(graphDef);
        BufferedImage bi = new BufferedImage(imageWidth,imageHeight,BufferedImage.TYPE_INT_RGB);
        graph.render(bi.getGraphics());
        logger.debug("RRDRequestExecutionTimeGauge.graph() wrote "+filename);
    }

    /**
     * Plots up to date graphs of the gauge
     * @throws IOException
     */
    public void graph() throws IOException {

        long currentTime = Util.getTime();
        logger.debug("RRDRequestExecutionTimeGauge.graph()");
        RrdDb rrdDb = new RrdDb(rrdFileName);

        RrdGraphDef graphDef = new RrdGraphDef();
        graphDef.setVerticalLabel("exectime(ms)");
        graphDef.setUnit("ms");
        graphDef.datasource("exectime_avg", rrdFileName, "exectime", ConsolFun.AVERAGE);

        graphDef.line("exectime_avg", new Color(0xBB, 0, 0), "exectime_avg", 4);

        graphDef.datasource("exectime_max", rrdFileName, "exectime", ConsolFun.MAX);

        graphDef.line("exectime_max", new Color(0xFF, 0, 0), "exectime_max", 3);

        graphDef.datasource("exectime_min", rrdFileName, "exectime", ConsolFun.MIN);

        graphDef.line("exectime_min", new Color(0x90, 0x20, 0x20), "exectime_min", 2);
        //hour
        //graphDef.setStartTime(-hour);
        //#graphDef.setStartTime(-hour);
        plotGraph(graphDef,currentTime-FIVEMIN,currentTime,rrdFiveminImage);

        plotGraph(graphDef,currentTime-HOUR,currentTime,rrdHourlyImage);

        plotGraph(graphDef,currentTime-DAY,currentTime,rrdDaylyImage);

        plotGraph(graphDef,currentTime-MONTH,currentTime,rrdMounthlyImage);

        plotGraph(graphDef,currentTime-YEAR,currentTime,rrdYearlyImage);
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


private static String getGraphicsHtml( String gaugeName, int width, int height) {

        StringBuilder style = new StringBuilder("\"width: ");
        style.append(width);
        style.append("px; height: ");
        style.append(height);
        style.append("px;\"");

    String sb = "<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">\n" +
                "<html>\n" +
                "<head>\n" +
                "  <meta content=\"text/html; charset=ISO-8859-1\"\n" +
                " http-equiv=\"content-type\">\n" +
                " <title>Request Execution Time Graphics for " +
                gaugeName +
                "</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "<h1> Request Execution Time Graphics for " +
                gaugeName +
                "</h1> \n" +
                "5 Minutes<br>\n" +
                "<img style=" +
                style +
                " alt=\"5 Minutes\"\n" +
                " src=\"images/" +
                gaugeName +
                ".5min.png\"><br>\n" +
                "Hour<br>\n" +
                "<img style=" +
                style +
                " alt=\"Hour\"\n" +
                " src=\"images/" +
                gaugeName +
                ".hour.png\"><br>\n" +
                "Day<br>\n" +
                "<img style=" +
                style +
                " alt=\"Day\"\n" +
                " src=\"images/" +
                gaugeName +
                ".day.png\"><br>\n" +
                "Month<br>\n" +
                "<img style=" +
                style +
                " alt=\"Month\"\n" +
                " src=\"images/" +
                gaugeName +
                ".month.png\"><br>\n" +
                "Year<br>\n" +
                "<img style=" +
                style +
                " alt=\"Year\"\n" +
                " src=\"images/" +
                gaugeName +
                ".year.png\"><br>\n" +
                "</body>\n" +
                "</html>";
    return sb;

}

    /**
     * @return the rrdGraphicsHtmlFileName
     */
    public String getRrdGraphicsHtmlFileName() {
        return rrdGraphicsHtmlFileName;
    }

}
