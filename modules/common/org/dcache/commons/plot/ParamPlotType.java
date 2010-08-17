package org.dcache.commons.plot;

/**
 * plot type is a unique id that identifies a particular plot, it is not
 * used for final rendering
 * @author taolong
 */
public class ParamPlotType implements PlotParameter {

    private static final long serialVersionUID = -8621814463919867407L;
    protected String type;

    public ParamPlotType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
