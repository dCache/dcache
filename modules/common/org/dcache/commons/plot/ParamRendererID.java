package org.dcache.commons.plot;

/**
 *
 * @author timur and tao
 */
public class ParamRendererID implements PlotParameter {

    private String rendererID;

    public ParamRendererID(String id) {
        rendererID = id;
    }

    public String getRendererID() {
        return rendererID;
    }
}
