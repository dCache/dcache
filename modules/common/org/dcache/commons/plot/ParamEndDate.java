package org.dcache.commons.plot;

import java.util.Date;

/**
 *
 * @author timur and tao
 */
public class ParamEndDate extends ParamDate {

    public ParamEndDate() {
        super();
    }

    public ParamEndDate(Date date) {
        super(date.getTime());
    }

    public ParamEndDate(long time) {
        super(time);
    }
}
