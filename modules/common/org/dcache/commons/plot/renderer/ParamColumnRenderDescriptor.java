package org.dcache.commons.plot.renderer;

import org.dcache.commons.plot.PlotParameter;
import org.dcache.commons.plot.renderer.svg.RGBColor;
import org.dcache.commons.plot.renderer.svg.SVGColor;

/**
 * specification for data in each column
 * Index, starting from 0 to (numColumns - 1), specifies which column in the yValue to be plotted
 * This specification is optional and the default is set to
 * use the first column and
 * @author timur and tao
 */
public class ParamColumnRenderDescriptor implements PlotParameter {

    private int index = 0;
    private String name;
    private SVGColor color;
    private RenderStyle renderStyle = RenderStyle.LINES;
    private Location rangeReference = Location.LEFT;

    public Location getRangeReference() {
        return rangeReference;
    }

    public void setRangeReference(Location rangeReference) {
        this.rangeReference = rangeReference;
    }

    public RenderStyle getRenderStyle() {
        return renderStyle;
    }

    public void setRenderStyle(RenderStyle renderStyle) {
        this.renderStyle = renderStyle;
    }

    public SVGColor getColor() {
        return color;
    }

    public void setColor(SVGColor color) {
        this.color = color;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
