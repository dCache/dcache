package org.dcache.services.billing.db.data;

import java.util.Map;


/**
 * @author arossi
 *
 */
public abstract class SizeDaily extends BaseDaily {

    public static final String SIZE = "size";

    protected Long size = 0L;

    /**
     * @return the size
     */
    public Long getSize() {
        return size;
    }

    /**
     * @param size
     *            the size to set
     */
    public void setSize(Long size) {
        this.size = size;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestampedData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(SIZE, size.doubleValue());
        return dataMap;
    }
}
