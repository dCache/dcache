package org.dcache.commons.plot;

/**
 * This enumeration represents the how data is aggregated together, for example,
 * one might be interested in plotting the max and/or mean of data transfer size
 * at each hour of certain day
 *
 * @author timur and tao
 */
public enum ParamAggregationType implements PlotParameter {

    NONE,
    SUM,
    AVERAGE,
    MAXIMUM,
    MINIMUM;
}
