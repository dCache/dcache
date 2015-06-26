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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.spi.SchedulingStrategy;

public class ThroughputFairShareSchedulingStrategy extends DiscriminatingSchedulingStrategy implements SchedulingStrategy
{
    private final Map<String,Queue<Long>> jobs = new HashMap<>();
    private List<String> keys = new ArrayList<>();
    private int position;
    private int size;

    public ThroughputFairShareSchedulingStrategy(String discriminator)
    {
        super(discriminator);
    }

    @Override
    protected synchronized void add(String key, Job job)
    {
        Queue<Long> queue =
                jobs.computeIfAbsent(key, k -> {
                    keys.add(k);
                    return new ArrayDeque<>();
                });
        queue.add(job.getId());
        size++;
    }

    @Override
    public synchronized Long remove()
    {
        if (size == 0) {
            return null;
        }
        Queue<Long> queue;
        do {
            queue = jobs.get(keys.get(position));
            position = position + 1;
            if (position >= keys.size()) {
                compact();
                position = 0;
            }
        } while (queue.isEmpty());
        size--;
        return queue.remove();
    }

    @Override
    public synchronized int size()
    {
        return size;
    }

    private void compact()
    {
        ArrayList<String> newKeys = new ArrayList<>(keys.size());
        for (String key : keys) {
            if (jobs.get(key).isEmpty()) {
                jobs.remove(key);
            } else {
                newKeys.add(key);
            }
        }
        keys = newKeys;
    }
}
