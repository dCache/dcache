package org.dcache.commons.plot;

/**
 * Plot name is the title of the plot in the final rendering
 * @author timur and tao
 */
public class ParamPlotName implements PlotParameter {

    private String name;

    public ParamPlotName(){}

    public ParamPlotName(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
