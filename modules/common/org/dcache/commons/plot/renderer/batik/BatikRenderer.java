/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.commons.plot.renderer.batik;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Random;
import org.apache.batik.transcoder.Transcoder;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;

import org.apache.batik.transcoder.image.PNGTranscoder;
import org.apache.batik.transcoder.image.JPEGTranscoder;
import org.apache.batik.transcoder.image.TIFFTranscoder;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotReply;
import org.dcache.commons.plot.PlotRequest;
import org.dcache.commons.plot.dao.TupleList;
import org.dcache.commons.plot.renderer.Renderer;
import org.dcache.commons.plot.renderer.svg.RectRenderer;
import org.dcache.commons.plot.renderer.svg.SVGRenderer;

/**
 *
 * @author taolong
 */
public class BatikRenderer implements Renderer {

    private String outputFileName = "out.png";
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

        int n = (new Random()).nextInt();
        String intermediateFileName = tempDir + "/temp" + n + ".svg";
        svgRenderer.setOutputFileName(intermediateFileName);
        svgRenderer.render(plotData, request);

        File svgFile = new File(intermediateFileName);
        if (!svgFile.exists()) {
            throw new PlotException("failure in creating intermediate SVG file");
        }

        try {
            TranscoderInput input = new TranscoderInput(svgFile.toURL().toString());
            OutputStream ostream = new FileOutputStream(outputFileName);
            TranscoderOutput output = new TranscoderOutput(ostream);

            Transcoder transcoder = null;
            if (outputFileName.toLowerCase().endsWith("png")) {
                transcoder = new PNGTranscoder();
            } else if (outputFileName.toLowerCase().endsWith("jpg")
                    || outputFileName.toLowerCase().endsWith("jpeg")) {
                transcoder = new JPEGTranscoder();
                transcoder.addTranscodingHint(JPEGTranscoder.KEY_QUALITY, new Float(1.0));
            } else if (outputFileName.toLowerCase().endsWith("tff")
                    || outputFileName.toLowerCase().endsWith("tiff")) {
                transcoder = new TIFFTranscoder();
            }

            if (transcoder == null) {
                throw new PlotException("image format " + outputFileName.substring(outputFileName.lastIndexOf(".")) + " not supported by Batik");
            }

            transcoder.transcode(input, output);
            ostream.close();

            PlotReply reply = new PlotReply();
            File file = new File(outputFileName);
            reply.setOutputURL(file.toURL());
            svgFile.delete();
            return reply;
        } catch (Exception e) {
            throw new PlotException("Batik transform failed: " + e.toString(), e);
        }
    }
}
