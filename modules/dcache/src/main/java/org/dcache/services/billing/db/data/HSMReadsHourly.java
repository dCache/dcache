package org.dcache.services.billing.db.data;

/**
 * @author arossi
 *
 */
public final class HSMReadsHourly extends SizeDaily {
    public String toString() {
        return "(" + dateString() + "," + count + "," + size + ")";
    }
}
