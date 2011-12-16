package org.dcache.services.billing.db.data;

import java.util.Date;

/**
 * @author arossi
 */
public interface ITimestamped {
    /**
     * @return the timestamp
     */
    Date timestamp();
}
