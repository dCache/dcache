/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.srm.request;

import org.dcache.srm.scheduler.State;

/**
 * Obtain notification about a job changing its state.  It is guaranteed that
 * the method is called for all state changes of subscribed jobs and that the
 * method is called in the order the job experiences the corresponding state
 * transitions.
 * <p>
 * Any further changes to the job are blocked until all subscribed
 * JobStateChangeAware methods return.  Therefore, a method call will not see
 * a job change any of its internal state for the duration of the call.
 * <p>
 * The method will not be called concurrently regarding the same job; however,
 * it may be called concurrently from different jobs.  Therefore the method
 * must be thread-safe.
 * <p>
 * There are no guarantees in which order different JobStateChangeAware
 * classes are called.  Any interactions between different JobStateChangeAware
 * classes must not make any assumptions in which order they are called
 * and not assume that their methods are called sequentially.
 * <p>
 * Obtaining the job's read-lock from within the the call is guaranteed not to
 * block.  Therefore, it's guaranteed safe to call any of the job's methods
 * protected only by this job's read lock (e.g., getter methods).
 * <p>
 * Methods MUST NOT call any method that attempts to acquire the Job's write
 * lock; for example, by attempting to modify the Job in any way.
 */
@FunctionalInterface
public interface JobStateChangeAware
{
    /**
     * This method is called after a Job object has changed state.
     * Concrete implementations of this method are not expected to block and
     * MUST NOT obtain the Job's write lock (e.g., updating the job).
     * @param job The job that has just changed state.
     * @param oldState The state this job had before acquiring its current state.
     * @param description A short description on what triggered the change.
     */
    void jobStateChanged(Job job, State oldState, String description);
}
