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
    void setData(Collection<IPlotData> data, String field, Double dfactor)
                    throws Throwable;

    /**
     * @return the TimeFrame
     */
    TimeFrame getTimeframe();

    /**
     * @return the Title
     */
    String getTitle();

    /**
     * @param title
     */
    void setTitle(String title);

    /**
     * @return the xLabel
     */
    String getXLabel();

    /**
     * @param xLabel
     *            the xLabel to set
     */
    void setXLabel(String xLabel);

    /**
     * @return the yLabel
     */
    String getYLabel();

    /**
     * @param yLabel
     *            the yLabel to set
     */
    void setYLabel(String yLabel);

    /**
     * @return the color
     */
    String getColor();

    /**
     * @param color
     *            the color to set
     */
    void setColor(String color);

    /**
     * @return the style
     */
    String getStyle();

    /**
     * @param style
     *            the style to set
     */
    void setStyle(String style);

    /**
     * @return the scaling
     */
    String getScaling();

    /**
     * @param scaling
     *            the scaling to set
     */
    void setScaling(String scaling);
}
