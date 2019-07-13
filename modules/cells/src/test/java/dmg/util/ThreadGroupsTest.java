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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import static org.mockito.Mockito.mock;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;

public class ThreadGroupsTest
{
    @Test
    public void shouldFindNoThreadsInThreadGroupWithNoThreads()
    {
        ThreadGroup threadgroup = mock(ThreadGroup.class);
        given(threadgroup.activeCount()).willReturn(0);
        given(threadgroup.enumerate(any(Thread[].class))).willReturn(0);

        List<Thread> threads = ThreadGroups.threadsInGroup(threadgroup);

        assertThat(threads, is(empty()));
    }

    @Test
    public void shouldFindSingleThreadInThreadGroupWithSingleThread()
    {
        Thread thread = mock(Thread.class);
        ThreadGroup threadgroup = mock(ThreadGroup.class);
        given(threadgroup.activeCount()).willReturn(1);
        given(threadgroup.enumerate(any(Thread[].class))).willAnswer(a -> {
                    Thread[] storage = a.getArgumentAt(0, Thread[].class);
                    storage[0] = thread;
                    return 1;
                });

        List<Thread> threads = ThreadGroups.threadsInGroup(threadgroup);

        assertThat(threads, contains(thread));
    }

    @Test
    public void shouldFindAllThreadInThreadGroupWithLargeActiveCount()
    {
        List<Thread> actualThreads = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            actualThreads.add(mock(Thread.class));
        }

        ThreadGroup threadgroup = mock(ThreadGroup.class);
        given(threadgroup.activeCount()).willReturn(20);
        given(threadgroup.enumerate(any(Thread[].class))).willAnswer(a -> {
                    Thread[] storage = a.getArgumentAt(0, Thread[].class);
                    int count = Math.min(storage.length, actualThreads.size());
                    for(int i = 0; i < count; i++) {
                        storage[i] = actualThreads.get(i);
                    }
                    return count;
                });

        List<Thread> threads = ThreadGroups.threadsInGroup(threadgroup);

        assertThat(threads, is(equalTo(actualThreads)));
    }

    @Test
    public void shouldFindAllThreadInThreadGroupWithSmallActiveCount()
    {
        List<Thread> actualThreads = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            actualThreads.add(mock(Thread.class));
        }

        ThreadGroup threadgroup = mock(ThreadGroup.class);
        given(threadgroup.activeCount()).willReturn(1);
        given(threadgroup.enumerate(any(Thread[].class))).willAnswer(a -> {
                    Thread[] storage = a.getArgumentAt(0, Thread[].class);
                    int count = Math.min(storage.length, actualThreads.size());
                    for(int i = 0; i < count; i++) {
                        storage[i] = actualThreads.get(i);
                    }
                    return count;
                });

        List<Thread> threads = ThreadGroups.threadsInGroup(threadgroup);

        assertThat(threads, is(equalTo(actualThreads)));
    }

    @Test
    public void shouldFindAllThreadInThreadGroupWithVerySmallActiveCount()
    {
        List<Thread> actualThreads = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            actualThreads.add(mock(Thread.class));
        }

        ThreadGroup threadgroup = mock(ThreadGroup.class);
        given(threadgroup.activeCount()).willReturn(1);
        given(threadgroup.enumerate(any(Thread[].class))).willAnswer(a -> {
                    Thread[] storage = a.getArgumentAt(0, Thread[].class);
                    int count = Math.min(storage.length, actualThreads.size());
                    for(int i = 0; i < count; i++) {
                        storage[i] = actualThreads.get(i);
                    }
                    return count;
                });

        List<Thread> threads = ThreadGroups.threadsInGroup(threadgroup);

        assertThat(threads, is(equalTo(actualThreads)));
    }
}