package org.dcache.commons.plot;

import java.util.Date;

/**
 *
 * @author timur and tao
 */
public class ParamStartDate extends Date implements PlotParameter {

    public ParamStartDate() {
        super();
    }

    public ParamStartDate(long time) {
        super(time);
    }

    public ParamStartDate(int year, int month, int date, int hour, int minute, int second) {
        super(hour, month, date, hour, minute, second);
    }
}
