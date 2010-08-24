package org.dcache.services.plots;

import dmg.util.HttpException;
import org.apache.xerces.dom.DocumentImpl;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import dmg.util.HttpRequest;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author timur and tao
 */
public class PlotPage {

    protected Element html, head, body;
    protected Document document;
    protected String queryString;
    protected HttpRequest httpRequest;
    protected final String DATE_FORMAT = "yyyy-MM-dd HH:mm";
    protected final static String PLOTDIR = "plots";
    protected final static String TRANSFERRATE_PAGE = "transferrate";
    protected final static String CONNECTIONTIME_PAGE = "connectiontime";
    protected final static String CACHEHITS_PAGE = "cachehits";
    protected final static String IMAGE_PAGE = "images";
    protected final static String NAV_PAGE = "navigation";
    protected final static String CUSTOMIZED_PAGE = "customized";
    protected final static String SHOW_FRAME = "showframe";
    protected static String imageDir = System.getProperty("org.dcache.services.plots.imagedir", "/tmp/img");
    protected final static String imgFormat = "png";

    public void setRequest(HttpRequest request) {
        this.httpRequest = request;
    }

    public void buildHtml() throws HttpException {
        document = new DocumentImpl();
        html = document.createElement("html");
        document.appendChild(html);

        body = document.createElement("body");
        html.appendChild(body);

        buildHead();
        buildBody();
    }

    protected void buildHead() throws HttpException{
        head = document.createElement("head");
        html.appendChild(head);

        Element link = document.createElement("link");
        link.setAttribute("type", "text/css");
        link.setAttribute("rel", "stylesheet");
        link.setAttribute("href", "/styles/common.css");
        head.appendChild(link);

        link = document.createElement("link");
        link.setAttribute("type", "text/css");
        link.setAttribute("rel", "stylesheet");
        link.setAttribute("href", "/styles/billing.css");
        head.appendChild(link);

        Element title = document.createElement("title");
        title.setTextContent("dCache Billing Plots");
        head.appendChild(title);
    }

    protected void buildBody() throws HttpException {
        Element title = document.createElement("h1");
        title.setTextContent("dCache Billing Plots");
        body.appendChild(title);
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public final Document getDocument() {
        return document;
    }

    /**
     * parse query string
     * @param query
     * @return
     */
    protected static Map<String, String> getQueryMap(String query) {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params) {
            String name = param.split("=")[0];
            String value = param.split("=")[1];
            map.put(name, value);
        }
        return map;
    }
}
