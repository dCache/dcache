package org.dcache.services.billing.db.data;

import java.util.Map;

/**
 * @author arossi
 *
 */
public final class HitsDaily extends BaseDaily {
    public static final String NOT_CACHED = "notcached";
    public static final String CACHED = "cached";

    private Long notcached= 0L;
    private Long cached= 0L;

    /**
     * @return the notcached
     */
    public Long getNotcached() {
        return notcached;
    }

    /**
     * @param notcached
     *            the notcached to set
     */
    public void setNotcached(Long notcached) {
        this.notcached = notcached;
    }

    /**
     * @return the cached
     */
    public Long getCached() {
        return cached;
    }

    /**
     * @param cached
     *            the cached to set
     */
    public void setCached(Long cached) {
        this.cached = cached;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestampedData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(NOT_CACHED, notcached.doubleValue());
        dataMap.put(CACHED, cached.doubleValue());
        return dataMap;
    }

    public String toString() {
        return "(" + dateString() + "," + count + "," + notcached + ","
                        + cached + ")";
    }
}
