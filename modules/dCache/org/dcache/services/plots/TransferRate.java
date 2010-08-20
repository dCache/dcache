package org.dcache.services.plots;

import org.dcache.commons.plot.ParamDaoID;
import org.dcache.commons.plot.ParamOutputFileName;
import org.dcache.commons.plot.ParamPlotName;
import org.dcache.commons.plot.PlotRequest;

/**
 *
 * @author timur and tao
 */
public class TransferRate extends DisplayPage {
    @Override
    public PlotRequest getPlotRequest(String timeSpan) {
        PlotRequest plotRequest = super.getPlotRequest(timeSpan);
        plotRequest.setParameter(new ParamOutputFileName(imageDir + "/transfer_rate_" + timeSpan));
        plotRequest.setParameter(new ParamDaoID("dcache_read:dcache_write:enstore_read:enstore_write"));
        plotRequest.setParameter(new ParamPlotName("Transfer Rates (bytes)"));
        return plotRequest;
    }
}
