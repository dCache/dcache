package org.dcache.services.billing.db.data;

/**
 * @author arossi
 *
 */
public final class HSMReadsDaily extends SizeDaily {
    public String toString() {
        return "(" + dateString() + "," + count + "," + size + ")";
    }
}
