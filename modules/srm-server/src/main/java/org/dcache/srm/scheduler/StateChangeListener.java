package org.dcache.srm.scheduler;

import org.dcache.srm.request.Job;

/**
 * Listener for SRM job state changes.
 */
public interface StateChangeListener
{
    void stateChanged(Job job, State oldState, State newState);
}
