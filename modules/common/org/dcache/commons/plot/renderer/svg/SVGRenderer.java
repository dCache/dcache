package org.dcache.commons.plot.renderer.svg;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
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
    protected PlotRequest plotRequest;
    protected List<T> tupleLists;
    protected float width = 800, height = 500, margin = 40;
    protected String outputFileName;
    protected Element elemFrame, elemBackground, elemData;

    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
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

    protected void renderBackground() throws PlotException {
        svg.setFillColor(new RGBColor(200, 200, 230));
        SVGGradient color = new SVGGradient(svg.getDocument(), "background_grad");
        color.setLinerGradient(0, 0, new RGBColor(200, 200, 230), 1.0f,
                    100, 100, new RGBColor(200, 200, 200), 1.0f);
        svg.setFillColor(color);
        svg.setStrokeWidth(0);
        svg.setWidth(width);
        svg.setHeight(height);
        elemBackground.appendChild(svg.createRectangle(0, 0, width, height));
    }

    protected void renderFrame() throws PlotException {
        //plot name
        svg.setStrokeWidth(0);
        ParamPlotName plotName = plotRequest.getParameter(ParamPlotName.class);
        if (plotName != null) {
            svg.setFillColor(SVGColor.BLACK);
            svg.setStroke(SVGColor.BLACK);
            svg.setTextSize(svg.getTextSize() * 1.5f);
            svg.setTextAlignment(SVGDocument.TextAlignment.CENTER);
            elemFrame.appendChild(svg.createText(width / 2, height - svg.getTextSize(),
                    plotName.getName()));
            svg.setTextSize(svg.getTextSize() / 1.5f);
            Date curDate = new Date();
            DateFormat format = new SimpleDateFormat("MMM, dd, yyyy HH:mm");
            svg.setTextAlignment(SVGDocument.TextAlignment.LEFT);
            elemFrame.appendChild(svg.createText(0, height - svg.getTextSize(), format.format(curDate)));
        }
    }

    protected void renderData() throws PlotException {
    }

    @Override
    public PlotReply render(List<T> tupleList, PlotRequest plotRequest) throws PlotException {
        this.tupleLists = tupleList;
        this.plotRequest = plotRequest;

        ParamOutputFileName fileName = this.plotRequest.getParameter(ParamOutputFileName.class);
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
