package org.dcache.commons.plot.renderer.svg;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.dcache.commons.plot.ParamPlotName;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotReply;
import org.dcache.commons.plot.PlotRequest;
import org.dcache.commons.plot.dao.TupleList;
import org.dcache.commons.plot.renderer.Renderer;
import org.w3c.dom.Element;
import org.dcache.commons.plot.ParamOutputFileName;

/**
 *
 * @author tao
 */
public class SVGRenderer<T extends TupleList> implements Renderer<T> {

    protected static String TagBackground = "background";
    protected static String TagFrame = "frame";
    protected static String TagData = "data";
    protected SVGDocument svg = new SVGDocument();
    protected PlotRequest request;
    protected T tupleList;
    protected float width = 640, height = 480, margin = 40;
    protected String outputFileName = "out";
    protected Element elemFrame, elemBackground, elemData;

    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    public T getTupleList() {
        return tupleList;
    }

    public void setTupleList(T tupleList) {
        this.tupleList = tupleList;
    }

    public void writeToFile(String filename) throws PlotException {
        try {
            svg.serialize(filename);
        } catch (FileNotFoundException ex) {
            throw new PlotException("file not found: " + ex, ex);
        } catch (IOException ex) {
            throw new PlotException("io exception: " + ex, ex);
        }
    }

    public PlotRequest getRequest() {
        return request;
    }

    public void setRequest(PlotRequest request) {
        this.request = request;
    }

    public SVGDocument getSvg() {
        return svg;
    }

    public void setSvg(SVGDocument svg) {
        this.svg = svg;
    }

    protected void renderBackground() throws PlotException {
        svg.setFillColor(SVGColor.WHITE);
        elemBackground.appendChild(svg.createRectangle(0, 0, width, height));
    }

    protected void renderFrame() throws PlotException {
        //plot name
        ParamPlotName plotName = request.getParameter(ParamPlotName.class);
        if (plotName != null) {
            svg.setFillColor(SVGColor.BLACK);
            svg.setStroke(SVGColor.BLACK);
            svg.setTextAlignment(SVGDocument.TextAlignment.CENTER);
            elemFrame.appendChild(svg.createText(width / 2, margin, plotName.getName()));
        }
    }

    protected void renderData() throws PlotException {
    }

    @Override
    public PlotReply render(T tupleList, PlotRequest plotRequest) throws PlotException {
        this.tupleList = tupleList;
        this.request = plotRequest;

        ParamOutputFileName fileName = request.getParameter(ParamOutputFileName.class);
        if (fileName != null) {
            outputFileName = fileName.getOutputFileName() + ".svg";
        }

        elemBackground = svg.createGroup(TagBackground);
        svg.getRoot().appendChild(elemBackground);

        elemFrame = svg.createGroup(TagFrame);
        svg.getRoot().appendChild(elemFrame);

        elemData = svg.createGroup(TagData);
        svg.getRoot().appendChild(elemData);

        renderBackground();
        renderFrame();
        renderData();

        try {
            svg.serialize(outputFileName);
            File file = new File(outputFileName);
            PlotReply reply = new PlotReply();
            reply.setOutputURL(file.toURL());
            return reply;
        } catch (FileNotFoundException ex) {
            throw new PlotException("file not found: " + ex, ex);
        } catch (IOException ex) {
            throw new PlotException("IO exception: " + ex, ex);
        }
    }
}
