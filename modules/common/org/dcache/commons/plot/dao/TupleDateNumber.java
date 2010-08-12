package org.dcache.commons.plot.dao;

import java.util.Date;
import java.util.List;

/**
 * this class is a an extension of Tuple in specifying x and y values
 *
 * @author timur and tao
 */
public class TupleDateNumber extends Tuple<Date, List<Number>> {

    public TupleDateNumber(Date date, List<Number> value) {
        super(date, value);
    }
}
