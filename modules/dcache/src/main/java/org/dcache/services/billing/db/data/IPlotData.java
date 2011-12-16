package org.dcache.services.billing.db.data;

import java.util.Map;

/**
 * Implemented by all beans storing data to be used in 1-D Histograms.
 *
 * @author arossi
 */
public interface IPlotData extends ITimestamped {

    /**
     * The contract is that the data which can be plotted on the Y-axis of a 1-D
     * time histogram will be made available as a Map of name:value pairs.
     *
     * @return the data map
     */
    Map<String, Double> data();
}
