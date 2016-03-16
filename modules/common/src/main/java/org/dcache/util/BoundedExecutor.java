package org.dcache.util;

import com.google.common.util.concurrent.AbstractListeningExecutorService;
import com.google.common.util.concurrent.Monitor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An executor that places a bound on the number of concurrent tasks.
 *
 * Differs from Executors#newFixedThreadPool by not holding on to idle
 * threads and by sourcing threads from another Executor rather than
 * creating them itself.
 *
 * Combined with Executors#newCachedThreadPool, the achieved semantics is
 * that of an unlimited task queue that scales the number of threads to
 * a certain limit, while also allowing those threads to time out. These
 * semantics cannot be achieved with ThreadPoolExecutor, as that class
 * will always create new threads up to the core size, even if allowing
 * core threads to time out.
 */
public class BoundedExecutor extends AbstractListeningExecutorService
{
    private final Queue<Runnable> workQueue = new ArrayDeque<>();
    private final Executor executor;
    private final List<Thread> workers;
    private int maxThreads;
    private int maxQueued;
    private int threads;

    private final Monitor monitor = new Monitor();
    private final Monitor.Guard isTerminated = new Monitor.Guard(monitor) {
        @Override
        public boolean isSatisfied()
        {
            return isShutdown && threads == 0;
        }
    };

    private final Worker worker = new Worker();
    private boolean isShutdown;

    public BoundedExecutor(Executor executor, int maxThreads)
    {
        this(executor,  maxThreads,  Integer.MAX_VALUE);
    }

    public BoundedExecutor(Executor executor, int maxThreads, int maxQueued)
    {
        this.executor = executor;
        this.maxThreads = maxThreads;
        this.maxQueued = maxQueued;
        this.workers = new ArrayList<>(maxThreads);
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
            unexecutedTasks.addAll(workQueue);
            workQueue.clear();
            for (Thread thread : workers) {
                thread.interrupt();
            }
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
            return isShutdown && threads == 0;
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
            if (workQueue.size() - maxThreads + threads >= maxQueued) {
                throw new RejectedExecutionException("Executor queue limit has been reached.");
            }
            if (threads < maxThreads) {
                threads++;
                executor.execute(worker);
            }
            workQueue.add(task);
        } finally {
            monitor.leave();
        }
    }

    public void setMaximumPoolSize(int size)
    {
        monitor.enter();
        try {
            maxThreads = size;

            int threadsToStart = Math.min(maxThreads - threads, workQueue.size());
            for (int i = 0; i < threadsToStart; i++) {
                threads++;
                executor.execute(worker);
            }
        } finally {
            monitor.leave();
        }
    }

    public int getMaximumPoolSize()
    {
        monitor.enter();
        try {
            return maxThreads;
        } finally {
            monitor.leave();
        }
    }

    public void setMaximumQueueSize(int size)
    {
        monitor.enter();
        try {
            maxQueued = size;
        } finally {
            monitor.leave();
        }
    }

    public int getMaximumQueueSize()
    {
        monitor.enter();
        try {
            return maxQueued;
        } finally {
            monitor.leave();
        }
    }

    private class Worker implements Runnable
    {
        private Runnable getFirstTask()
        {
            monitor.enter();
            try {
                if (workQueue.isEmpty()) {
                    threads--;
                    return null;
                } else {
                    workers.add(Thread.currentThread());
                    return workQueue.remove();
                }
            } finally {
                monitor.leave();
            }
        }

        private Runnable getNextTask()
        {
            monitor.enter();
            try {
                if (workQueue.isEmpty() || threads > maxThreads) {
                    threads--;
                    workers.remove(Thread.currentThread());
                    return null;
                } else {
                    return workQueue.remove();
                }
            } finally {
                monitor.leave();
            }
        }

        @Override
        public void run()
        {
            Runnable task = getFirstTask();
            while (task != null) {
                try {
                    task.run();
                } catch (Throwable t) {
                    Thread thread = Thread.currentThread();
                    thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                }
                task = getNextTask();
            }
        }
    }
}
