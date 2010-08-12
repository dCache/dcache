package org.dcache.commons.plot.dao;

import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotRequest;

/**
 *
 * @author timur and tao
 */
public interface PlotDao {

    public TupleList getData(PlotRequest request) throws PlotException;
}
