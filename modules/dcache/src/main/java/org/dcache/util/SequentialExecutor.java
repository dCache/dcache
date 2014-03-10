package org.dcache.util;

import com.google.common.util.concurrent.Monitor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An executor which executes its tasks sequentially.
 *
 * Differs from Executors#newSingleThreadExecutor by sourcing a thread
 * from a shared executor when needed.
 */
public class SequentialExecutor extends AbstractExecutorService
{
    private final Queue<Runnable> tasks = new ArrayDeque<>();

    private final Executor executor;

    private final Monitor monitor = new Monitor();
    private final Monitor.Guard isTerminated = new Monitor.Guard(monitor) {
        @Override
        public boolean isSatisfied()
        {
            return isShutdown && !isRunning;
        }
    };

    private final Worker worker = new Worker();
    private boolean isShutdown;
    private boolean isRunning;

    public SequentialExecutor(Executor executor)
    {
        this.executor = executor;
    }

    @Override
    public void shutdown()
    {
        monitor.enter();
        try {
            isShutdown = true;
        } finally {
            monitor.leave();
        }
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        monitor.enter();
        try {
            isShutdown = true;
            List<Runnable> unexecutedTasks = new ArrayList<>();
            unexecutedTasks.addAll(tasks);
            tasks.clear();

            // Kill runnable

            return unexecutedTasks;
        } finally {
            monitor.leave();
        }
    }

    @Override
    public boolean isShutdown()
    {
        monitor.enter();
        try {
            return isShutdown;
        } finally {
            monitor.leave();
        }
    }

    @Override
    public boolean isTerminated()
    {
        monitor.enter();
        try {
            return isShutdown && !isRunning;
        } finally {
            monitor.leave();
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        monitor.enter();
        try {
            return monitor.waitFor(isTerminated, timeout, unit);
        } finally {
            monitor.leave();
        }
    }

    public void awaitTermination() throws InterruptedException
    {
        monitor.enter();
        try {
            monitor.waitFor(isTerminated);
        } finally {
            monitor.leave();
        }
    }

    @Override
    public void execute(Runnable task)
    {
        monitor.enter();
        try {
            if (isShutdown) {
                throw new RejectedExecutionException("Executor has been shut down.");
            }
            tasks.add(task);
            if (!isRunning) {
                executor.execute(worker);
                isRunning = true;
            }
        } finally {
            monitor.leave();
        }
    }

    private Runnable getTask()
    {
        monitor.enter();
        try {
            if (tasks.isEmpty()) {
                isRunning = false;
                return null;
            } else {
                return tasks.remove();
            }
        } finally {
            monitor.leave();
        }
    }

    private class Worker implements Runnable
    {
        @Override
        public void run()
        {
            Runnable task = getTask();
            while (task != null) {
                try {
                    task.run();
                } catch (Throwable t) {
                    Thread thread = Thread.currentThread();
                    thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                }
                task = getTask();
            }
        }
    }
}
