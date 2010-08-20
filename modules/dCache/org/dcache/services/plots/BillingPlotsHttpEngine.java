/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.services.plots;

import java.io.IOException;
import java.io.PrintWriter;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.KillEvent;
import dmg.cells.nucleus.MessageEvent;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;

import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.w3c.dom.Document;

/**
 *
 * @author taolong
 */
public class BillingPlotsHttpEngine implements HttpResponseEngine, Cell {

    private final CellNucleus _nucleus;
    private static String httpResponseText = "HTTP/1.0 200 OK\nContent-Type: text/html\n\n";
    private static String httpResponseImage = "HTTP/1.0 200 OK\nContent-Type: image/";
    private static int bufferSize = 1024;
    private static String imageDir = System.getProperty("org.dcache.services.plots.imagedir", "/tmp/img");

    public static String getImageDir() {
        return imageDir;
    }

    public CellNucleus getNucleus() {
        return _nucleus;
    }

    public BillingPlotsHttpEngine(CellNucleus nucleus, String[] args) {
        _nucleus = nucleus;
    }

    @Override
    public final void queryUrl(HttpRequest request) throws HttpException {
        PlotPage page = null;
        String[] tokens = request.getRequestTokens();
        if (tokens.length == 1) {
            page = new HomePage();
        }

        if (tokens.length > 1) {
            String pageName = tokens[1];
            String queryString = null;

            if (tokens[1].contains("?")) {
                String[] parts = tokens[1].split("\\?");
                pageName = parts[0];
                queryString = parts[1];
            }

            if (pageName.compareTo(PlotPage.TRANSFERRATE_PAGE) == 0) {
                page = new TransferRate();
                page.setQueryString(queryString);
            }

            if (pageName.compareTo(PlotPage.CONNECTIONTIME_PAGE) == 0) {
                page = new ConnectionTime();
                page.setQueryString(queryString);
            }

            if (pageName.compareTo(PlotPage.NAV_PAGE) == 0) {
                page = new NavigationPage();
            }

            if (pageName.compareTo(PlotPage.IMAGE_PAGE) == 0) {
                if (tokens.length == 3) {
                    String fileName = tokens[2];
                    serveImage(fileName, request);
                }
            }
        }
        if (page == null) {
            page = new PageNotFound();
        }

        page.setRequest(request);
        page.buildHtml();
        Document document = page.getDocument();

        OutputFormat formatter = new OutputFormat("XML", "ISO-8859-1", true);
        formatter.setOmitXMLDeclaration(true);
        OutputStream out = request.getOutputStream();
        PrintWriter output = new PrintWriter(out);
        output.append(httpResponseText);
        XMLSerializer serializer = new XMLSerializer(output, formatter);

        try {
            serializer.asDOMSerializer();
            serializer.serialize(document.getDocumentElement());
            out.close();
        } catch (Exception ex) {
            throw new HttpException(505, "exception occured: " + ex);
        }
    }

    private void serveImage(String fileName, HttpRequest request)
            throws HttpException {

        if (fileName.startsWith(".")
                || fileName.startsWith(File.separator)) {
            throw new HttpException(400, "Permission denied");
        }

        OutputStream outputStream = request.getOutputStream();
        String extension = fileName.substring(fileName.lastIndexOf(".") + 1).toLowerCase();

        if (extension.compareTo("jpg") != 0
                && extension.compareTo("jpeg") != 0
                && extension.compareTo("png") != 0
                && extension.compareTo("svg") != 0
                && extension.compareTo("tif") != 0
                && extension.compareTo("tiff") != 0) {
            throw new HttpException(400, "Permission denied for extension: " + extension);
        }

        File file = new File(imageDir + File.separator + fileName);
        if (!file.exists()) {
            throw new HttpException(400, "Requested resource not found: " + imageDir + File.separator + fileName);
        }

        try {
            String headerResponse = httpResponseImage + extension + "\n\n";
            outputStream.write(headerResponse.getBytes());
            byte buffer[] = new byte[bufferSize];
            FileInputStream inputStream = new FileInputStream(file);
            int len = 0;
            while ((len = inputStream.read(buffer)) > 0) {
                outputStream.write(buffer, 0, len);
            }
            inputStream.close();
            outputStream.close();
        } catch (IOException ex) {
            throw new HttpException(504, "IO exception occured in server: " + ex);
        }


    }

    @Override
    public String getInfo() {
        return "billing plots";
    }

    @Override
    public void messageArrived(MessageEvent me) {
    }

    @Override
    public void prepareRemoval(KillEvent killEvent) {
    }

    @Override
    public void exceptionArrived(ExceptionEvent ce) {
    }
}
