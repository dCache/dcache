package org.dcache.services.billing.db.data;

/**
 * @author arossi
 */
public final class DcacheReadsHourly extends TransferDaily {
    public String toString() {
        return "(" + dateString() + "," + count + "," + size + ","
                        + transferred + ")";
    }
}
