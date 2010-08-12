package org.dcache.commons.plot.renderer;

import java.text.Format;
import java.util.Set;

/**
 *
 * @author timur and tao
 */
public class AxisInfo {

    private Location location = Location.BOTTOM;
    private String axisName;
    private Number binSize = new Integer(1);
    private int maxNumTicks = 12;
    private Format format;
    private float textAngle = 0, tickLength = 6;
    private Set<Integer> rangeReference;
    private Scale scale = Scale.LINEAR;
    private Range range;

    public Number getBinSize() {
        return binSize;
    }

    public void setBinSize(Number binSize) {
        this.binSize = binSize;
    }

    public Range getRange() {
        return range;
    }

    public void setRange(Range range) {
        this.range = range;
    }

    public Scale getScale() {
        return scale;
    }

    public void setScale(Scale scale) {
        this.scale = scale;
    }

    public Set<Integer> getRangeReference() {
        return rangeReference;
    }

    public void setRangeReference(Set<Integer> rangeReference) {
        this.rangeReference = rangeReference;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public float getTextAngle() {
        return textAngle;
    }

    public void setTextAngle(float textAngle) {
        this.textAngle = textAngle;
    }

    public float getTickLength() {
        return tickLength;
    }

    public void setTickLength(float tickLength) {
        this.tickLength = tickLength;
    }

    public Format getFormat() {
        return format;
    }

    public void setTickLength(int tickLength) {
        this.tickLength = tickLength;
    }

    public int getMaxNumTicks() {
        return maxNumTicks;
    }

    public void setMaxNumTicks(int maxNumTicks) {
        this.maxNumTicks = maxNumTicks;
    }

    public String getAxisName() {
        return axisName;
    }

    public void setAxisName(String axisName) {
        this.axisName = axisName;
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }
}
