package org.dcache.commons.plot.renderer.svg;


/**
 *
 * @author timur and tao
 */
public interface SVGColor {
    @Override
    public String toString();

    public static RGBColor BLACK = new RGBColor(0, 0, 0);
    public static RGBColor WHITE = new RGBColor(255, 255, 255);
    public static RGBColor RED = new RGBColor(255, 0, 0);
    public static RGBColor BLUE = new RGBColor(0, 0, 255);
    public static RGBColor GREEN = new RGBColor(0, 255, 0);
    public static RGBColor MAGENTA = new RGBColor(255, 0, 255);
}
