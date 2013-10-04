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

public class ParallelizingLineProcessor<T> implements LineProcessor<T>, AutoCloseable
{
    private final ExecutorService service = Executors.newCachedThreadPool();
    private final List<Future> readers = new ArrayList<>();
    private final BlockingQueue<String> queue;
    private final int threads;
    private final LineProcessor<T> callback;

    public ParallelizingLineProcessor(int threads, LineProcessor<T> callback)
    {
        this.threads = threads;
        this.callback = callback;
        queue = new ArrayBlockingQueue<>(threads * 512);
        for (int i = 0; i < threads; i++) {
            readers.add(service.submit(new LineReader<>(queue, callback)));
        }
    }

    @Override
    public void close() throws IOException
    {
        try {
            // Poison the readers
            for (int i = 0; i < threads; i++) {
                queue.put("");
            }
            service.shutdown();
            for (Future reader : readers) {
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

    @Override
    public boolean processLine(String line) throws IOException
    {
        if (!line.isEmpty()) {
            try {
                queue.put(line);
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

        private LineReader(BlockingQueue<String> queue, LineProcessor<T> processor)
        {
            this.queue = queue;
            this.processor = processor;
        }

        @Override
        public T call() throws InterruptedException
        {
            String line;
            while (!(line = queue.take()).isEmpty()) {
                try {
                    processor.processLine(line);
                } catch (Throwable t) {
                    Thread thread = Thread.currentThread();
                    thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                }
            }
            return processor.getResult();
        }
    }
}
