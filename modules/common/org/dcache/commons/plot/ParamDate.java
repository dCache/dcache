package org.dcache.commons.plot;

import java.util.Date;

/**
 *
 * @author timur and tao
 */
public class ParamDate extends Date implements PlotParameter {

    public ParamDate() {
    }

    /**
     *  constructor
     * @param date
     */
    public ParamDate(Date date) {
        super(date.getTime());
    }

    /**
     * constructor
     * @param time time in milliseconds
     */
    public ParamDate(long time) {
        super(time);
    }
}
