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

    @Override
    public String getTitle() {
        return title;
    }

    @Override
    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public TimeFrame getTimeframe() {
        return timeframe;
    }

    @Override
    public String getXLabel() {
        return XLabel;
    }

    @Override
    public void setXLabel(String xLabel) {
        XLabel = xLabel;
    }

    @Override
    public String getYLabel() {
        return YLabel;
    }

    @Override
    public void setYLabel(String yLabel) {
        YLabel = yLabel;
    }

    @Override
    public String getColor() {
        return color;
    }

    @Override
    public void setColor(String color) {
        this.color = color;
    }

    @Override
    public String getStyle() {
        return style;
    }

    @Override
    public void setStyle(String style) {
        this.style = style;
    }

    @Override
    public String getScaling() {
        return scaling;
    }

    @Override
    public void setScaling(String scaling) {
        this.scaling = scaling;
    }
}
