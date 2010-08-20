package org.dcache.commons.plot.renderer;

import java.util.List;
import org.dcache.commons.plot.PlotException;
import org.dcache.commons.plot.PlotReply;
import org.dcache.commons.plot.PlotRequest;
import org.dcache.commons.plot.dao.TupleList;

/**
 * transform/render PlotData into specific output image
 * @author
 */
public interface Renderer<T extends TupleList> {
    public PlotReply render(List<T> tupleLists, PlotRequest plotRequest) throws PlotException;
}
