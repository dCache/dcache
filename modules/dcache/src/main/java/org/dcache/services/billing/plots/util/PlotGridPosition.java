package org.dcache.services.billing.plots.util;

/**
 * Abstraction for 2D grid-based plots. <br><br>
 *
 * @author arossi
 */
public class PlotGridPosition {
    private final int row;
    private final int col;

    /**
     * @param row count
     * @param col count
     */
    public PlotGridPosition(int row, int col) {
        this.row = row;
        this.col = col;
    }

    /**
     * @param key of form "rows:cols"
     */
    public PlotGridPosition(String key) {
        String[] coord = key.split(":");
        if (coord.length > 0)
            row = Integer.parseInt(coord[0]);
        else
            row = 1;
        if (coord.length > 1)
            col = Integer.parseInt(coord[1]);
        else
            col = 1;
    }

    /**
     * @return the row
     */
    public int getRow() {
        return row;
    }

    /**
     * @return the col
     */
    public int getCol() {
        return col;
    }

    /**
     * for hashing
     */
    public String getKey() {
        return row + ":" + col;
    }
}
