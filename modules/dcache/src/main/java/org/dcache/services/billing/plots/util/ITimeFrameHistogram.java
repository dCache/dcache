package org.dcache.services.billing.plots.util;

import java.util.Collection;

import org.dcache.services.billing.db.data.IPlotData;

/**
 * Defines 1-D time histogram abstraction based on TimeFrame definition.
 *
 * @see TimeFrame
 * @author arossi
 */
public interface ITimeFrameHistogram {

    /**
     * Processes the data points onto the historgram. @see IPlotData.
     *
     * @param data
     *            set of data points to plot.
     * @param field
     *            the Y-axis data to be plotted.
     * @param dfactor
     *            (optional) scaling factor for Y-axis data.
     * @throws Throwable
     */
    void setData(Collection<IPlotData> data, String field, Double dfactor);

    TimeFrame getTimeframe();

    String getTitle();

    void setTitle(String title);

    String getXLabel();

    void setXLabel(String xLabel);

    String getYLabel();

    void setYLabel(String yLabel);

    String getColor();

    void setColor(String color);

    String getStyle();

    void setStyle(String style);

    String getScaling();

    void setScaling(String scaling);
}
