package org.dcache.commons.plot;

/**
 *
 * @author timur and tao
 */
public class ParamBinSize<T extends Number> implements PlotParameter{
    private T binSize;

    public ParamBinSize(T value){
        binSize = value;
    }

    public Number getBinSize() {
        return binSize;
    }

    public void setBinSize(Number binSize) {
        this.binSize = (T) binSize;
    }
}
