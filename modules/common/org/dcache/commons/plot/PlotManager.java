package org.dcache.commons.plot;

import org.dcache.commons.plot.dao.PlotDao;
import org.dcache.commons.plot.dao.PlotDaoFactory;
import org.dcache.commons.plot.dao.TupleList;
import org.dcache.commons.plot.renderer.PlotOutputType;
import org.dcache.commons.plot.renderer.PlotRendererFactory;
import org.dcache.commons.plot.renderer.Renderer;

/**
 * This class is the main interface to external packages.
 * A static method Plot takes in a PlotRequest as input and plot requested data
 * a PlotReply is returned when plotting process is finished successfully.
 *
 * @author timur and tao
 */
public class PlotManager {

    /**
     * Main interface method, plot based on a plot request
     * Internally, it converts PlotRequest to DataSourceRequest and
     * TransformRequest and perform plotting. Result is returned as
     * PlotRequest.
     *
     * @param request input PlotRequest
     * @return PlotReply if success
     * @throws PlotException if failed, either in DataSource class or Transform
     * Class
     */
    public static PlotReply plot(PlotRequest request) throws PlotException {
        ParamPlotType plotType = request.getParameter(ParamPlotType.class);
        if (plotType == null) {
            throw new PlotException("plot type is not specified in the request");
        }

        PlotDaoFactory plotDaoFactory = PlotDaoFactory.getInstance();
        PlotDao plotDao = plotDaoFactory.getPlotDao(plotType);

        TupleList table = plotDao.getData(request);

        PlotOutputType plotOutputType =
                request.getParameter(PlotOutputType.class);

        PlotRendererFactory rendererFactory =
                PlotRendererFactory.getInstance();
        Renderer renderer = rendererFactory.getPlotRenderer(plotOutputType);

        return renderer.render(table, request);
    }
}
