package org.dcache.services.plots;

import org.dcache.commons.plot.ParamDaoID;
import org.dcache.commons.plot.ParamOutputFileName;
import org.dcache.commons.plot.ParamPlotName;
import org.dcache.commons.plot.ParamRendererID;
import org.dcache.commons.plot.PlotRequest;

/**
 *
 * @author timur and tao
 */
public class ConnectionTime extends DisplayPage {
    @Override
    public PlotRequest getPlotRequest(String timeSpan) {
        PlotRequest plotRequest = super.getPlotRequest(timeSpan);

        //set using log scale
        ParamRendererID id = plotRequest.getParameter(ParamRendererID.class);
        plotRequest.setParameter(new ParamRendererID(id.getRendererID()+ "_log"));
        plotRequest.setParameter(new ParamOutputFileName(imageDir + "/conn_time_log_" + timeSpan));
        plotRequest.setParameter(new ParamDaoID("dcache_conn_time:enstore_conn_time"));
        plotRequest.setParameter(new ParamPlotName("Connection Time (milliseconds)"));
        return plotRequest;
    }
}