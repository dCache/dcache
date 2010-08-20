package org.dcache.services.plots;

import dmg.util.HttpException;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.dcache.commons.plot.ParamBinSize;
import org.dcache.commons.plot.ParamEndDate;
import org.dcache.commons.plot.ParamOutputFileName;
import org.dcache.commons.plot.ParamRendererID;
import org.dcache.commons.plot.ParamStartDate;
import org.dcache.commons.plot.PlotManager;
import org.dcache.commons.plot.PlotReply;
import org.dcache.commons.plot.PlotRequest;
import org.w3c.dom.Element;

/**
 *
 * @author timur and tao
 */
public class DisplayPage extends PlotPage {

    protected Map<String, String> urls = new HashMap<String, String>();
    protected static String imageDir = System.getProperty("org.dcache.services.plots.imagedir", "/tmp/img");
    protected final static String YEAR = "year";
    protected final static String MONTH = "month";
    protected final static String WEEK = "week";
    protected final static String DAY = "day";

    protected final static String outputType = "png";
    /**
     * this map dictates how often plots are updated
     */
    protected Map<String, Integer> plotDuration = new HashMap<String, Integer>();

    public DisplayPage() {
        plotDuration.put(YEAR, Calendar.WEEK_OF_MONTH);
        plotDuration.put(MONTH, Calendar.DATE);
        plotDuration.put(WEEK, Calendar.DATE);
        plotDuration.put(DAY, Calendar.HOUR);
    }


    /**
     * make sure plots are up to date and urls are set
     */
    protected void updatePlots() throws HttpException{
        try {
            for (String timeSpan : plotDuration.keySet()) {
                PlotRequest plotRequest = getPlotRequest(timeSpan);
                ParamOutputFileName paramFileName = plotRequest.getParameter(ParamOutputFileName.class);
                String fileName = paramFileName.getOutputFileName() +"." +this.outputType;
                File file = new File(fileName);

                boolean isUpToDate = false;
                if (file.exists()) {
                    Calendar cal = Calendar.getInstance();
                    cal.add(plotDuration.get(timeSpan), -1);

                    //image file exists and up to date
                    if (file.lastModified() > cal.getTime().getTime()) {
                        String[] tokens = fileName.split(File.separator);
                        isUpToDate = true;
                    }
                }
                //plot
                if (!isUpToDate){
                    PlotReply reply = PlotManager.plot(plotRequest);
                }
                String[] tokens = fileName.split(File.separator);
                urls.put(timeSpan, PlotPage.IMAGE_PAGE + File.separator + tokens[tokens.length - 1]);
            }
        } catch (Exception e) {
            throw new HttpException(501, e.toString());
        }
    }

    @Override
    protected void buildHead() throws HttpException {
        super.buildHead();
        Element script = document.createElement("script");
        head.appendChild(script);

        script.setAttribute("type", "text/javascript");
        String content = "function setSrc(src){var x = document.images;x[0].src = src;}";
        script.setTextContent(content);
    }

    @Override
    protected void buildBody() throws HttpException{
        updatePlots();

        Element table = document.createElement("table");
        body.appendChild(table);

        Element tr = document.createElement("tr");
        table.appendChild(tr);

        Element td, input;

        for (String key : urls.keySet()) {
            td = document.createElement("td");
            tr.appendChild(td);
            input = document.createElement("input");
            td.appendChild(input);
            input.setAttribute("onclick", "setSrc('" + urls.get(key) + "')");
            input.setAttribute("value", key);
            input.setAttribute("type", "button");
        }

        //img
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setAttribute("colspan", Integer.toString(urls.size()));
        Element img = document.createElement("img");
        td.appendChild(img);
        img.setAttribute("src", urls.get(YEAR));
    }

    /**
     * output an html form that contains start/end day fields and bin size
     * @param action which page this form is submitted to
     * @return dom element representing the form
     */
    protected Element createForm(String action) {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        String curDateString = format.format(new Date());

        Element form = document.createElement("form");
        form.setAttribute("action", action);
        Element table = document.createElement("table");
        form.appendChild(table);

        Element tr = null, td = null, input;

        tr = document.createElement("tr");
        table.appendChild(tr);

        //start date
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Start Date");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("input");
        td.appendChild(input);
        input.setAttribute("value", curDateString);
        input.setAttribute("name", "start_date");

        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Format YYYY-MM-DD hh:mm:ss");

        //end date
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("End Date");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("input");
        td.appendChild(input);
        input.setAttribute("name", "end_date");
        input.setAttribute("value", curDateString);

        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Format YYYY-MM-DD hh:mm:ss");

        //bin size
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Bin Size");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("select");
        td.appendChild(input);
        input.setAttribute("name", "bin_size");
        Element option = null;

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "monthly");
        option.setTextContent("Monthly");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "weekly");
        option.setTextContent("Weekly");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "daily");
        option.setTextContent("Daily");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "hourly");
        option.setTextContent("Hourly");

        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Plot bin size");


        //submit
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Submit");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("input");
        td.appendChild(input);
        input.setAttribute("type", "submit");

        return form;
    }

    public PlotRequest getPlotRequest(String timeSpan) {
        PlotRequest plotRequest = new PlotRequest();
        Calendar cal = Calendar.getInstance();
        plotRequest.setParameter(new ParamEndDate(cal.getTime().getTime()));
        if (timeSpan.compareTo(YEAR) == 0) {
            cal.add(Calendar.YEAR, -1);
            plotRequest.setParameter(new ParamStartDate(cal.getTime().getTime()));
            plotRequest.setParameter(new ParamBinSize(Calendar.MONTH));
            plotRequest.setParameter(new ParamRendererID("renderer_year_" + outputType));
            return plotRequest;
        }

        if (timeSpan.compareTo(MONTH) == 0) {
            cal.add(Calendar.MONTH, -1);
            plotRequest.setParameter(new ParamStartDate(cal.getTime().getTime()));
            plotRequest.setParameter(new ParamBinSize(Calendar.DATE));
            plotRequest.setParameter(new ParamRendererID("renderer_month_" + outputType));
            return plotRequest;
        }

        if (timeSpan.compareTo(WEEK) == 0) {
            cal.add(Calendar.WEEK_OF_YEAR, -1);
            plotRequest.setParameter(new ParamStartDate(cal.getTime().getTime()));
            plotRequest.setParameter(new ParamBinSize(Calendar.DATE));
            plotRequest.setParameter(new ParamRendererID("renderer_week_" + outputType));
            return plotRequest;
        }

        if (timeSpan.compareTo(DAY) == 0) {
            cal.add(Calendar.DATE, -1);
            plotRequest.setParameter(new ParamStartDate(cal.getTime().getTime()));
            plotRequest.setParameter(new ParamBinSize(Calendar.HOUR));
            plotRequest.setParameter(new ParamRendererID("renderer_day_" + outputType));
            return plotRequest;
        }
        return plotRequest;
    }
}
