package org.dcache.cells;

/**
 * Classes implementing this method receive Cell life cycle
 * notifications.
 */
public interface CellLifeCycleAware
{
    /**
     * Called just after the cell has been started.
     */
    void afterStart();

    /**
     * Called just before the cell is killed.
     */
    void beforeStop();
}