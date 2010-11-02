package org.dcache.util;


/**
 * An interface for requests to be sortable according priority and creation time.
 * @since 1.9.11
 */
public interface IoPrioritizable {

    /**
     * Get {@link IoPriority} of the request.
     * @return priority.
     */
    IoPriority getPriority();

    /**
     * Get request creation time. This method can only be used to compute a difference
     * between two requests and is not related to any other notion of system or wall-clock time.
     *
     * @return creation time.
     */
    long getCreateTime();
}
