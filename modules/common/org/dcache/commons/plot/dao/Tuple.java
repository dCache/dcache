package org.dcache.commons.plot.dao;

/**
 * a tuple represent and x, y value pair(e.g. data vs number, or number vs number, etc)
 * @author timur and tao
 */
public class Tuple<X, Y> {

    private X xValue;
    private Y yValue;

    public Tuple(X xValue, Y yValue) {
        this.xValue = xValue;
        this.yValue = yValue;
    }

    public X getXValue() {
        return xValue;
    }

    public Y getYValue() {
        return yValue;
    }

    public void setxValue(X xValue) {
        this.xValue = xValue;
    }

    public void setyValue(Y yValue) {
        this.yValue = yValue;
    }
}
