package org.dcache.commons.plot;

import java.util.Date;

/**
 *
 * @author timur and tao
 */
public class ParamStartDate extends ParamDate {

    public ParamStartDate() {
        super();
    }

    public ParamStartDate(Date date){
        super(date.getTime());
    }

    public ParamStartDate(long time) {
        super(time);
    }
}
