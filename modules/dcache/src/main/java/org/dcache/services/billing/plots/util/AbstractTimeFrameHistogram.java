package org.dcache.services.billing.plots.util;

/**
 * Base class for ITimeFrameHistogram implementations.
 *
 * @see ITimeFrameHistogram
 * @author arossi
 */
public abstract class AbstractTimeFrameHistogram implements ITimeFrameHistogram {

    protected final TimeFrame timeframe;

    protected String XLabel;
    protected String YLabel;
    protected String color;
    protected String style;
    protected String scaling;
    protected String title;

    /**
     * @param timeframe
     * @param title
     */
    protected AbstractTimeFrameHistogram(TimeFrame timeframe, String title) {
        this.timeframe = timeframe;
        this.title = title;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.statistics.util.ITimeFrameHistogram#getTitle()
     */
    public String getTitle() {
        return title;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogram#setTitle(java.
     * lang.String)
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogram#getTimeframe()
     */
    public TimeFrame getTimeframe() {
        return timeframe;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.statistics.util.ITimeFrameHistogram#getXLabel()
     */
    public String getXLabel() {
        return XLabel;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogram#setXLabel(java
     * .lang.String)
     */
    public void setXLabel(String xLabel) {
        XLabel = xLabel;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.statistics.util.ITimeFrameHistogram#getYLabel()
     */
    public String getYLabel() {
        return YLabel;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogram#setYLabel(java
     * .lang.String)
     */
    public void setYLabel(String yLabel) {
        YLabel = yLabel;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.statistics.util.ITimeFrameHistogram#getColor()
     */
    public String getColor() {
        return color;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogram#setColor(java.
     * lang.String)
     */
    public void setColor(String color) {
        this.color = color;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.statistics.util.ITimeFrameHistogram#getStyle()
     */
    public String getStyle() {
        return style;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogram#setStyle(java.
     * lang.String)
     */
    public void setStyle(String style) {
        this.style = style;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.statistics.util.ITimeFrameHistogram#getScaling()
     */
    public String getScaling() {
        return scaling;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.dcache.billing.statistics.util.ITimeFrameHistogram#setScaling(java
     * .lang.String)
     */
    public void setScaling(String scaling) {
        this.scaling = scaling;
    }
}
