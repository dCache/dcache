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
import com.google.common.collect.Ordering;

import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.StateChangeListener;
import org.dcache.srm.scheduler.spi.SchedulingStrategy;

import static org.dcache.srm.scheduler.State.*;

public class InProgressFairShareSchedulingStrategy extends DiscriminatingSchedulingStrategy implements SchedulingStrategy, StateChangeListener
{
    private static final EnumSet<State> RUNNING_STATES =
            EnumSet.of(INPROGRESS, RQUEUED, READY, TRANSFERRING);

    private final Map<String,Queue<Long>> jobs = new HashMap<>();
    private final Multiset<String> counters = ConcurrentHashMultiset.create();
    private int size;

    private final Ordering<Map.Entry<String, Queue<Long>>> byCount =
            Ordering.natural().onResultOf(entry -> counters.count(entry.getKey()));

    public InProgressFairShareSchedulingStrategy(Scheduler scheduler, String discriminator)
    {
        super(discriminator);
        scheduler.addStateChangeListener(this);
    }

    @Override
    public void stateChanged(Job job, State oldState, State newState)
    {
        if (RUNNING_STATES.contains(oldState) && !RUNNING_STATES.contains(newState)) {
            counters.remove(getDiscriminatingValue(job));
        } else if (!RUNNING_STATES.contains(oldState) && RUNNING_STATES.contains(newState)) {
            counters.add(getDiscriminatingValue(job));
        }
    }

    @Override
    public synchronized Long remove()
    {
        if (size == 0) {
            return null;
        }

        Map.Entry<String,Queue<Long>> entry = byCount.min(jobs.entrySet());
        Queue<Long> queue = entry.getValue();
        Long job = queue.remove();
        if (queue.isEmpty()) {
            jobs.remove(entry.getKey());
        }
        size--;
        return job;
    }

    @Override
    public synchronized int size()
    {
        return size;
    }

    @Override
    protected synchronized void add(String key, Job job)
    {
        jobs.computeIfAbsent(key, k -> new ArrayDeque<>()).add(job.getId());
        size++;
    }
}
