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
public class CacheHits extends DisplayPage {
    @Override
    public PlotRequest getPlotRequest(String timeSpan) {
        PlotRequest plotRequest = super.getPlotRequest(timeSpan);
        ParamRendererID id = plotRequest.getParameter(ParamRendererID.class);
        plotRequest.setParameter(new ParamRendererID(id.getRendererID()+ "_log"));
        plotRequest.setParameter(new ParamOutputFileName(imageDir + "/cache_hits_" + timeSpan));
        plotRequest.setParameter(new ParamDaoID("hits_cached:hits_uncached"));
        plotRequest.setParameter(new ParamPlotName("Cache Hits (counts)"));
        return plotRequest;
    }
}