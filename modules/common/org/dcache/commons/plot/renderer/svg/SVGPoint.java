package org.dcache.commons.plot.renderer.svg;

/**
 *
 * @author timur and tao
 */
public class SVGPoint {

    float x, y;

    public SVGPoint() {
    }

    public SVGPoint(float x, float y) {
        this.x = x;
        this.y = y;
    }

    public float getX() {
        return x;
    }

    public void setX(float x) {
        this.x = x;
    }

    public float getY() {
        return y;
    }

    public void setY(float y) {
        this.y = y;
    }
}
