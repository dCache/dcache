package org.dcache.commons.plot;

import java.util.ArrayList;
import java.util.List;
import org.dcache.commons.plot.dao.PlotDao;
import org.dcache.commons.plot.dao.PlotDaoFactory;
import org.dcache.commons.plot.dao.TupleList;
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

        PlotDaoFactory plotDaoFactory = PlotDaoFactory.getInstance();
        ParamDaoID daoID = request.getParameter(ParamDaoID.class);
        String[] ids = daoID.getDaoID().split(":");
        List<TupleList> tupleLists = new ArrayList<TupleList>();
        for (int i = 0; i < ids.length; i++) {
            PlotDao plotDao = plotDaoFactory.getPlotDao(new ParamPlotType(ids[i]));
            TupleList tupleList = plotDao.getData(request);
            tupleLists.add(tupleList);
        }

        PlotRendererFactory rendererFactory =
                PlotRendererFactory.getInstance();
        ParamRendererID rendererID = request.getParameter(ParamRendererID.class);
        Renderer renderer =
                rendererFactory.getPlotRenderer(rendererID.getRendererID());

        return renderer.render(tupleLists, request);
    }
}
