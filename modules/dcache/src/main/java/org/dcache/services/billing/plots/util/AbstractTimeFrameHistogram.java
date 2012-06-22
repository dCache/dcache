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

    protected AbstractTimeFrameHistogram(TimeFrame timeframe, String title) {
        this.timeframe = timeframe;
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public TimeFrame getTimeframe() {
        return timeframe;
    }

    public String getXLabel() {
        return XLabel;
    }

    public void setXLabel(String xLabel) {
        XLabel = xLabel;
    }

    public String getYLabel() {
        return YLabel;
    }

    public void setYLabel(String yLabel) {
        YLabel = yLabel;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getStyle() {
        return style;
    }

    public void setStyle(String style) {
        this.style = style;
    }

    public String getScaling() {
        return scaling;
    }

    public void setScaling(String scaling) {
        this.scaling = scaling;
    }
}
