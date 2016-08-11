package org.dcache.services.billing.text;

import com.google.common.base.Throwables;
import com.google.common.io.LineProcessor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;

import static com.google.common.base.Preconditions.checkState;

/**
 * Decorates a LineProcessor to parallelize invocation of the decorator processor.
 *
 * Invoking {@link #processLine(String)} may or may not block and the
 * decorator processor's {@link LineProcessor#processLine(String)} method
 * will be called from one of a number of worker threads.
 *
 * Lines starting with a hash ('#') however act as barriers and are always passed
 * through sequentially.
 *
 * The decorator must be closed through {@link #close()} before retrieving the result
 * with {@link #getResult()}.
 */
public class ParallelizingLineProcessor<T> implements LineProcessor<T>, AutoCloseable
{
    private final ExecutorService service = Executors.newCachedThreadPool();
    private final List<Future<T>> readers = new ArrayList<>();
    private final BlockingQueue<String> queue;
    private final int threads;
    private final LineProcessor<T> callback;
    private final Phaser phaser;
    private boolean areWorkersRunning;

    public ParallelizingLineProcessor(int threads, LineProcessor<T> callback)
    {
        this.threads = threads;
        this.callback = callback;
        phaser = new Phaser(threads + 1);
        queue = new ArrayBlockingQueue<>(threads * 512);
        for (int i = 0; i < threads; i++) {
            readers.add(service.submit(new LineReader<>(queue, callback, phaser)));
        }
    }

    @Override
    public void close() throws IOException
    {
        try {
            if (areWorkersRunning) {
                stopWorkers();
            }
            phaser.forceTermination();
            phaser.arriveAndDeregister();
            service.shutdown();
            for (Future<T> reader : readers) {
                reader.get();
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.propagateIfPossible(cause);
            throw new IOException(cause.getMessage(), cause);
        }
    }

    private void startWorkers()
    {
        checkState(!areWorkersRunning);
        phaser.arriveAndAwaitAdvance();
        areWorkersRunning = true;
    }

    private void stopWorkers() throws InterruptedException
    {
        checkState(areWorkersRunning);
        for (int i = 0; i < threads; i++) {
            queue.put("");
        }
        phaser.arriveAndAwaitAdvance();
        areWorkersRunning = false;
    }

    @Override
    public boolean processLine(String line) throws IOException
    {
        if (!line.isEmpty()) {
            try {
                if (line.charAt(0) != '#') {
                    if (!areWorkersRunning) {
                        startWorkers();
                    }
                    queue.put(line);
                } else {
                    if (areWorkersRunning) {
                        stopWorkers();
                    }
                    callback.processLine(line);
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException(e.getMessage());
            }
        }
        return true;
    }

    @Override
    public T getResult()
    {
        return callback.getResult();
    }

    private static class LineReader<T> implements Callable<T>
    {
        private final BlockingQueue<String> queue;
        private final LineProcessor<T> processor;
        private final Phaser phaser;

        private LineReader(BlockingQueue<String> queue, LineProcessor<T> processor, Phaser phaser)
        {
            this.queue = queue;
            this.processor = processor;
            this.phaser = phaser;
        }

        @Override
        public T call() throws InterruptedException
        {
            while (phaser.arriveAndAwaitAdvance() >= 0) {
                String line;
                while (!(line = queue.take()).isEmpty()) {
                    try {
                        processor.processLine(line);
                    } catch (Throwable t) {
                        Thread thread = Thread.currentThread();
                        thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                    }
                }
                phaser.arriveAndAwaitAdvance();
            }
            return processor.getResult();
        }
    }
}
