package org.dcache.services.billing.db.data;


/**
 * @author arossi
 *
 */
public abstract class TransferDaily extends SizeDaily implements IPlotData {
    protected Long transferred = 0L;

    /**
     * @return the transferred
     */
    public Long getTransferred() {
        return transferred;
    }

    /**
     * @param transferred the transferred to set
     */
    public void setTransferred(Long transferred) {
        this.transferred = transferred;
    }
}
