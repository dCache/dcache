/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.srm.scheduler.strategy;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.StateChangeListener;
import org.dcache.srm.scheduler.spi.TransferStrategy;

/**
 * Provides a fair share transfer strategy.
 *
 * Allows TURLs to be handed out according to a fair share of the ready slots. A client is entitled to
 * its share by having requests in READY or RQUEUED. This strategy may leave transfer slots unused as
 * a result of one client having requests in RQUEUED without readying them. In other words, if a client
 * has requests in RQUEUED, this strategy will try to keep the client's fair share available.
 */
public class FairShareTransferStrategy extends ForwardingJobDiscriminator implements TransferStrategy, StateChangeListener
{
    private final Multiset<String> rqueued = ConcurrentHashMultiset.create();
    private final Multiset<String> ready = ConcurrentHashMultiset.create();
    private final Scheduler scheduler;

    public FairShareTransferStrategy(Scheduler scheduler, String discriminator)
    {
        super(discriminator);
        this.scheduler = scheduler;
        scheduler.addStateChangeListener(this);
    }

    @Override
    public void stateChanged(Job job, State oldState, State newState)
    {
        if (oldState == State.RQUEUED && newState != State.RQUEUED) {
            rqueued.remove(getDiscriminatingValue(job));
        } else if (oldState != State.RQUEUED && newState == State.RQUEUED) {
            rqueued.add(getDiscriminatingValue(job));
        }
        if (oldState == State.READY && newState != State.READY) {
            ready.remove(getDiscriminatingValue(job));
        } else if (oldState != State.READY && newState == State.READY) {
            ready.add(getDiscriminatingValue(job));
        }
    }

    @Override
    public boolean canTransfer(Job job)
    {
        int readySize = ready.size();
        int queuedSize = rqueued.size();
        int max = scheduler.getMaxReadyJobs();

        /* Common cases that are cheap to check for.
         */
        if (readySize + queuedSize <= max) {
            return true;
        }
        if (readySize >= max) {
            return false;
        }

        /* The following isn't accurate in the presence of concurrent state change
         * notifications, but we trade an approximate result for lock contention.
         */

        /* We count how many other requests would need to be readied before job would
         * be allowed to start. If the current number of ready requests plus the number
         * of requests ahead of job is below the max, then we allow job to proceed.
         */
        int count = ready.count(getDiscriminatingValue(job));
        int aheadOfJob = ready.entrySet().stream()
                .mapToInt(e -> Math.min(rqueued.count(e.getElement()), Math.max(0, count - e.getCount())))
                .sum();

        return readySize + aheadOfJob < max;
    }
}
