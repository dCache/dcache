package org.dcache.commons.plot;

import java.util.Date;

/**
 *
 * @author timur and tao
 */
public class ParamEndDate extends Date implements PlotParameter {
    public ParamEndDate(){
        super();
    }

    public ParamEndDate(long time){
        super(time);
    }

    public ParamEndDate(int year, int month, int date, int hour, int minute, int second){
        super(hour, month, date, hour, minute, second);
    }
}
