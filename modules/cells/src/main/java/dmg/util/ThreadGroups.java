/*
 * dCache - http://www.dcache.org/
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
package dmg.util;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Utility methods for handling ThreadGroups.
 */
public class ThreadGroups
{
    private ThreadGroups()
    {
        // Utility class should not be instantiated.
    }

    public static List<Thread> threadsInGroup(ThreadGroup threadgroup)
    {
        /*
         * We must allocate an array with at least one more elements than the
         * number of actual threads present since we only know we have all
         * threads if {@link ThreadGroup#enumerate} returns a number strictly
         * less than the array size.
         *
         * Allocating more than just one extra element allows for thread
         * creation between the activeCount estimate and reality.  10 is a
         * somewhat arbitrary choice.
         */
        int arraySize = threadgroup.activeCount() + 1 + 10;
        Thread[] threads = new Thread[arraySize];

        int threadCount = threadgroup.enumerate(threads);

        while (threadCount == arraySize) {
            arraySize += 10;
            threads = new Thread[arraySize];
            threadCount = threadgroup.enumerate(threads);
        }

        return asList(threads).subList(0, threadCount);
    }
}
