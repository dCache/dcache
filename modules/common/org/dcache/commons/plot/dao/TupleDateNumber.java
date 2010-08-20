package org.dcache.commons.plot.dao;

import java.util.Date;

/**
 * this class is a an extension of Tuple in specifying x and y values
 *
 * @author timur and tao
 */
public class TupleDateNumber extends Tuple<Date, Number> {

    public TupleDateNumber(Date date, Number value) {
        super(date, value);
    }
}
