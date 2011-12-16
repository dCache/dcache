package org.dcache.services.billing.db.data;

/**
 * @author arossi
 */
public final class DcacheReadsDaily extends TransferDaily {
    public String toString() {
        return "(" + dateString() + "," + count + "," + size + ","
                        + transferred + ")";
    }
}
