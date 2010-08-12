package org.dcache.commons.plot;

import java.io.Serializable;
import java.net.URL;

/**
 * This class represents the output of the plotting process to external packages.
 * It contains a set of PlotData which contains PlotData used to generate the plot
 * and the URL of the output file
 * @author timur and tao
 */
public class PlotReply implements Serializable {

    private static final long serialVersionUID = 7932166056998290196L;
    private URL outputURL;

    public URL getOutputURL() {
        return outputURL;
    }

    public void setOutputURL(URL plotURL) {
        this.outputURL = plotURL;
    }
}
