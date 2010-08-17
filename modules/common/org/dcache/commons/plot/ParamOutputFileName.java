package org.dcache.commons.plot;

/**
 * specifies the output file name, not including extension
 * @author timur and tao
 */
public class ParamOutputFileName implements PlotParameter {

    private String outputFileName;

    public ParamOutputFileName(String name) {
        outputFileName = name;
    }

    public String getOutputFileName() {
        return outputFileName;
    }
}
