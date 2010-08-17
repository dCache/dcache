/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.commons.plot.renderer.batik;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import org.apache.batik.transcoder.Transcoder;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;

import org.apache.batik.transcoder.image.PNGTranscoder;
import org.apache.batik.transcoder.image.JPEGTranscoder;
import org.apache.batik.transcoder.image.TIFFTranscoder;
import org.dcache.commons.plot.ParamOutputFileName;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotReply;
import org.dcache.commons.plot.PlotRequest;
import org.dcache.commons.plot.dao.TupleList;
import org.dcache.commons.plot.renderer.PlotOutputType;
import org.dcache.commons.plot.renderer.Renderer;
import org.dcache.commons.plot.renderer.svg.RectRenderer;
import org.dcache.commons.plot.renderer.svg.SVGRenderer;

/**
 *
 * @author taolong
 */
public class BatikRenderer implements Renderer {

    private String outputFileName = "out";
    private SVGRenderer svgRenderer = new RectRenderer();
    private String tempDir = "/tmp";

    public String getTempDir() {
        return tempDir;
    }

    public void setTempDir(String tempDir) {
        this.tempDir = tempDir;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    public SVGRenderer getSvgRenderer() {
        return svgRenderer;
    }

    public void setSvgRenderer(SVGRenderer svgRenderer) {
        this.svgRenderer = svgRenderer;
    }

    public PlotReply render(TupleList plotData, PlotRequest request) throws PlotException {
        //perform svg rendering first
        if (svgRenderer == null) {
            throw new PlotException("SVG renderer must be set first");
        }

        ParamOutputFileName fileName = request.getParameter(ParamOutputFileName.class);

        if (fileName != null) {
            outputFileName = fileName.getOutputFileName();
        }

        String extension = ".png";
        PlotOutputType type = request.getParameter(PlotOutputType.class);
        Transcoder transcoder = null;
        switch (type) {
            case PNG:
                extension = ".png";
                transcoder = new PNGTranscoder();
                break;
            case JPEG:
                extension = ".jpg";
                transcoder = new JPEGTranscoder();
                transcoder.addTranscodingHint(JPEGTranscoder.KEY_QUALITY, new Float(1.0));
                break;
            case TIFF:
                extension = ".tiff";
                transcoder = new TIFFTranscoder();
                break;
            default:
                throw new PlotException("PlotOutputType not supported by Batik: " + type);
        }
        svgRenderer.render(plotData, request);

        File svgFile = new File(outputFileName + ".svg");
        if (!svgFile.exists()) {
            throw new PlotException("failure in creating intermediate SVG file");
        }

        try {
            TranscoderInput input = new TranscoderInput(svgFile.toURL().toString());
            OutputStream ostream = new FileOutputStream(outputFileName + extension);
            TranscoderOutput output = new TranscoderOutput(ostream);

            transcoder.transcode(input, output);
            ostream.close();

            PlotReply reply = new PlotReply();
            File file = new File(outputFileName + extension);
            reply.setOutputURL(file.toURL());
            svgFile.delete();
            return reply;
        } catch (Exception e) {
            throw new PlotException("Batik transform failed: " + e.toString(), e);
        }
    }
}
