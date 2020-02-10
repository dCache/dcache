/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class RepeatableTaskRunnerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RepeatableTaskRunnerTest.class);

    private RepeatableTaskRunner runner;
    private Executor executor;
    private Runnable task;

    @Test(expected=NullPointerException.class)
    public void shouldRejectNullExecutor()
    {
        given(aTask());
        given(anExecutor().thatUsesSameThread());
        given(aRepeatableTaskRunner().withNullExecutor());
    }

    @Test(expected=NullPointerException.class)
    public void shouldRejectNullTask()
    {
        given(aTask());
        given(anExecutor().thatUsesSameThread());
        given(aRepeatableTaskRunner().withNullTask());
    }

    @Test
    public void shouldRunTask()
    {
        given(aTask());
        given(anExecutor().thatUsesSameThread());
        given(aRepeatableTaskRunner());

        runner.start();

        verify(executor).execute(any(Runnable.class));
        verify(task).run();
    }

    @Test
    public void shouldRunTaskThatReschedulesItself()
    {
        given(aTask().that(this::callRunnerStart));
        given(anExecutor().thatUsesSameThread());
        given(aRepeatableTaskRunner());

        runner.start();

        verify(executor).execute(any(Runnable.class));
        verify(task, times(2)).run();
    }

    @Test
    public void shouldRunTaskTwiceWhenStartCalledWithRunningTask() throws Exception
    {
        CountDownLatch firstCallHasStarted = new CountDownLatch(1);
        CountDownLatch firstCallMayExit = new CountDownLatch(1);
        CountDownLatch secondCallHasStarted = new CountDownLatch(1);
        given(aTask().that(() -> {
                    firstCallHasStarted.countDown();

                    try {
                        firstCallMayExit.await();
                    } catch (InterruptedException e) {
                        LOGGER.error("Interrupted while waiting for start method to be called", e);
                    }
                }).that(() -> {
                    secondCallHasStarted.countDown();
                }));
        given(anExecutor().thatUsesSeparateThread());
        given(aRepeatableTaskRunner());

        runner.start();

        firstCallHasStarted.await();

        runner.start();

        firstCallMayExit.countDown();

        secondCallHasStarted.await();

        verify(executor).execute(any(Runnable.class));
        verify(task, times(2)).run();
    }

    @Test
    public void shouldRunTaskOnlyTwiceWhenStartCalledMultipleTimesWithRunningTask() throws Exception
    {
        CountDownLatch firstCallHasStarted = new CountDownLatch(1);
        CountDownLatch firstCallMayExit = new CountDownLatch(1);
        CountDownLatch secondCallHasStarted = new CountDownLatch(1);
        given(aTask().that(() -> {
                    firstCallHasStarted.countDown();

                    try {
                        firstCallMayExit.await();
                    } catch (InterruptedException e) {
                        LOGGER.error("Interrupted while waiting for start method to be called", e);
                    }
                }).that(() -> {
                    secondCallHasStarted.countDown();
                }));
        given(anExecutor().thatUsesSeparateThread());
        given(aRepeatableTaskRunner());

        runner.start();

        firstCallHasStarted.await();

        runner.start();
        runner.start(); // start is idempotent, here.  This call has no effect.

        firstCallMayExit.countDown();

        secondCallHasStarted.await();

        verify(executor).execute(any(Runnable.class));
        verify(task, times(2)).run();
    }

    @Test
    public void shouldRunTaskTwiceWhenStartCalledAfterTaskRun() throws Exception
    {
        given(aTask());
        given(anExecutor().thatUsesSameThread());
        given(aRepeatableTaskRunner());

        runner.start();
        runner.start();

        verify(executor, times(2)).execute(any(Runnable.class));
        verify(task, times(2)).run();
    }

    private void callRunnerStart()
    {
        runner.start();
    }

    private void given(RepeatableTaskRunnerBuilder builder)
    {
        runner = builder.build();
    }

    private void given(TaskBuilder builder)
    {
        task = builder.build();
    }

    private void given(ExecutorBuilder builder)
    {
        executor = builder.build();
    }

    private RepeatableTaskRunnerBuilder aRepeatableTaskRunner()
    {
        return new RepeatableTaskRunnerBuilder();
    }

    private class RepeatableTaskRunnerBuilder
    {
        Executor executor = requireNonNull(RepeatableTaskRunnerTest.this.executor);
        Runnable task = requireNonNull(RepeatableTaskRunnerTest.this.task);

        private RepeatableTaskRunnerBuilder withNullExecutor()
        {
            executor = null;
            return this;
        }

        private RepeatableTaskRunnerBuilder withNullTask()
        {
            task = null;
            return this;
        }

        private RepeatableTaskRunner build()
        {
            return new RepeatableTaskRunner(executor, task);
        }
    }

    private TaskBuilder aTask()
    {
        return new TaskBuilder();
    }

    private class TaskBuilder
    {
        private final Runnable task = mock(Runnable.class);
        private final Queue<Runnable> innerTasks = new ArrayDeque<>();

        private TaskBuilder()
        {
            doAnswer(ignored -> {
                        Runnable inner = innerTasks.poll();
                        if (inner != null) {
                            inner.run();
                        }
                        return null;
                    }).when(task).run();
        }

        private TaskBuilder that(Runnable inner)
        {
            innerTasks.offer(inner);
            return this;
        }

        private Runnable build()
        {
            return task;
        }
    }

    private ExecutorBuilder anExecutor()
    {
        return new ExecutorBuilder();
    }

    private class ExecutorBuilder
    {
        private final Executor executor = mock(Executor.class);
        private Executor handler;
        private Runnable postTask = () -> {};

        private ExecutorBuilder thatUsesSameThread()
        {
            handler = Runnable::run;
            return this;
        }

        private ExecutorBuilder thatUsesSeparateThread()
        {
            handler = r -> {
                        Thread thread = new Thread(r);
                        thread.setDaemon(true);
                        thread.start();
                    };
            return this;
        }

        private Executor build()
        {
            doAnswer(invocation ->  {
                        Runnable task = (Runnable)invocation.getArgument(0);
                        handler.execute(task);
                        return null;
                    }).when(executor).execute(any(Runnable.class));
            return executor;
        }
    }

}
