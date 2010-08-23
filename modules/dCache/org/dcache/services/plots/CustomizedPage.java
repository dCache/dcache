package org.dcache.services.plots;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.dcache.commons.plot.ParamBinSize;
import org.dcache.commons.plot.ParamDaoID;
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
public class CustomizedPage extends PlotPage {

    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

    @Override
    protected void buildBody() {
        Element h2 = document.createElement("h2");
        h2.setTextContent("Customized Plots");
        body.appendChild(h2);

        if (queryString != null) {
            PlotRequest request = this.getPlotRequest(PlotPage.getQueryMap(queryString));
            if (request != null) {
                try {
                    PlotReply reply = PlotManager.plot(request);
                    String url = reply.getOutputURL().toString();
                    String[] tokens = url.split(File.separator);
                    url = tokens[tokens.length - 1];
                    Element img = document.createElement("img");
                    img.setAttribute("src", IMAGE_PAGE + File.separator + url);
                    body.appendChild(img);
                } catch (Exception ex) {
                    h2 = document.createElement("h2");
                    h2.setTextContent("Plot Exception:" + ex);
                    body.appendChild(h2);
                }
            }
        }
        Element form = createForm(CUSTOMIZED_PAGE);
        body.appendChild(form);
    }

    /**
     * output an html form that contains start/end day fields and bin size
     * @param action which page this form is submitted to
     * @return dom element representing the form
     */
    protected Element createForm(String action) {
        //display name and DaoIDs
        Map<String, String> plots = new HashMap<String, String>();
        plots.put("hit cached", "hits_cached");
        plots.put("hit uncached", "hits_uncached");
        plots.put("dcache connection time", "dcache_conn_time");
        plots.put("enstore connection time", "enstore_conn_time");
        plots.put("dcache read rate", "dcache_read");
        plots.put("dcache write rate", "dcache_write");
        plots.put("enstore read rate", "dcache_read");
        plots.put("enstore write rate", "dcache_write");

        Element form = document.createElement("form");
        form.setAttribute("action", action);
        Element table = document.createElement("table");
        form.appendChild(table);

        Element tr = null, td = null, input;
        Element option = null;
        //plot1
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Plot 1");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("select");
        td.appendChild(input);
        input.setAttribute("name", "plot1");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "null");
        option.setTextContent("null");
        option.setAttribute("selected", "selected");

        for (String key : plots.keySet()) {
            option = document.createElement("option");
            input.appendChild(option);
            option.setAttribute("value", plots.get(key));
            option.setTextContent(key);
        }

        //plot2
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Plot 2");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("select");
        td.appendChild(input);
        input.setAttribute("name", "plot2");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "null");
        option.setTextContent("null");
        option.setAttribute("selected", "selected");

        for (String key : plots.keySet()) {
            option = document.createElement("option");
            input.appendChild(option);
            option.setAttribute("value", plots.get(key));
            option.setTextContent(key);
        }

        //plot3
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Plot 3");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("select");
        td.appendChild(input);
        input.setAttribute("name", "plot3");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "null");
        option.setTextContent("null");
        option.setAttribute("selected", "selected");

        for (String key : plots.keySet()) {
            option = document.createElement("option");
            input.appendChild(option);
            option.setAttribute("value", plots.get(key));
            option.setTextContent(key);
        }

        //plot3
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Plot 4");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("select");
        td.appendChild(input);
        input.setAttribute("name", "plot4");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "null");
        option.setTextContent("null");
        option.setAttribute("selected", "selected");

        for (String key : plots.keySet()) {
            option = document.createElement("option");
            input.appendChild(option);
            option.setAttribute("value", plots.get(key));
            option.setTextContent(key);
        }


        //start date
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Start Date");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("input");
        td.appendChild(input);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        input.setAttribute("value", dateFormat.format(cal.getTime()));
        input.setAttribute("name", "start_date");

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
        cal.add(Calendar.MONTH, 1);
        input.setAttribute("value", dateFormat.format(cal.getTime()));

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

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "month");
        option.setTextContent("month");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "day");
        option.setTextContent("day");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "hour");
        option.setTextContent("hour");

        //scale
        tr = document.createElement("tr");
        table.appendChild(tr);
        td = document.createElement("td");
        tr.appendChild(td);
        td.setTextContent("Scale");

        td = document.createElement("td");
        tr.appendChild(td);
        input = document.createElement("select");
        td.appendChild(input);
        input.setAttribute("name", "scale");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "log");
        option.setTextContent("log");

        option = document.createElement("option");
        input.appendChild(option);
        option.setAttribute("value", "linear");
        option.setTextContent("linear");

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

    private PlotRequest getPlotRequest(Map<String, String> queryMap) {
        try {
            if (queryMap == null) {
                return null;
            }
            PlotRequest request = new PlotRequest();
            String value;

            value = queryMap.get("start_date");
            value = value.replace('+', ' ');
            Date startDate = dateFormat.parse(value);
            request.setParameter(new ParamStartDate(startDate));

            value = queryMap.get("end_date");
            value = value.replace('+', ' ');
            Date endDate = dateFormat.parse(value);
            request.setParameter(new ParamEndDate(endDate));

            String daos = "";
            String dao = queryMap.get("plot1");
            if (dao != null && dao.compareTo("null") != 0) {
                daos += dao;
            }

            dao = queryMap.get("plot2");
            if (dao != null && dao.compareTo("null") != 0) {
                daos += ":" + dao;
            }

            dao = queryMap.get("plot3");
            if (dao != null && dao.compareTo("null") != 0) {
                daos += ":" + dao;
            }

            dao = queryMap.get("plot4");
            if (dao != null && dao.compareTo("null") != 0) {
                daos += ":" + dao;
            }

            if (daos.startsWith(":")) {
                daos = daos.substring(1);
            }

            if (daos.length() == 0) {
                return null;
            }
            request.setParameter(new ParamDaoID(daos));

            String renderer = null;
            value = queryMap.get("bin_size");
            if (value.compareTo("month") == 0) {
                renderer = "renderer_year_" + imgFormat;
                request.setParameter(new ParamBinSize(Calendar.MONTH));
            }

            if (value.compareTo("day") == 0) {
                renderer = "renderer_month_" + imgFormat;
                request.setParameter(new ParamBinSize(Calendar.DATE));
            }

            if (value.compareTo("hour") == 0) {
                renderer = "renderer_day_" + imgFormat;
                request.setParameter(new ParamBinSize(Calendar.HOUR));
            }

            if (renderer == null) {
                return null;
            }

            value = queryMap.get("scale");
            if (value.compareTo("log") == 0) {
                renderer += "_log";
            }
            request.setParameter(new ParamRendererID(renderer));

            String fileName = imageDir + File.separator + "temp" + (int) (Math.random() * 100);
            request.setParameter(new ParamOutputFileName(fileName));

            return request;
        } catch (Exception e) {
            return null;
        }
    }
}
