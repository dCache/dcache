package org.dcache.commons.plot;

import java.math.BigDecimal;

/**
 *
 * @author timur and tao
 */
public class ParamBinSize extends BigDecimal implements PlotParameter{

    public ParamBinSize(double value){
        super(value);
    }

    public ParamBinSize(Number value){
        super(value.toString());
    }
}
