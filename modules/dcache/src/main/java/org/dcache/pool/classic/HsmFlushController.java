package org.dcache.pool.classic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolFlushDoFlushMessage;
import diskCacheV111.vehicles.PoolFlushGainControlMessage;

import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.Reply;
import dmg.util.Formats;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.util.FireAndForgetTask;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Controls flush to tape.
 *
 * Can interact with a central flush controller to coordinate flush
 * on multiple pools.
 */
public class HsmFlushController
        extends AbstractCellComponent
        implements CellMessageReceiver, CellCommandListener
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(HsmFlushController.class);

    private final ScheduledExecutorService _flushExecutor = createFlushExecutor();
    private final StorageClassContainer _storageQueue;
    private final FireAndForgetTask _flushTask = new FireAndForgetTask(new FlushTask());

    private ScheduledFuture<?> _future;
    private long _holdUntil = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
    private long _flushingInterval = TimeUnit.MINUTES.toMillis(1);
    private long _retryDelayOnError = TimeUnit.MINUTES.toMillis(1);
    private int _maxActive = 1000;

    public HsmFlushController(
            StorageClassContainer storageQueue)
    {
        _storageQueue = storageQueue;
    }

    private ScheduledThreadPoolExecutor createFlushExecutor()
    {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("flush").build();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }

    private synchronized void schedule()
    {
        _future = _flushExecutor.scheduleWithFixedDelay(_flushTask, getHoldDelay(), _flushingInterval, TimeUnit.MILLISECONDS);
    }

    private synchronized void reschedule()
    {
        if (_future != null) {
            _future.cancel(false);
            schedule();
        }
    }

    public synchronized int getMaxActive()
    {
        return _maxActive;
    }

    public synchronized void setMaxActive(int concurrency)
    {
        _maxActive = concurrency;
    }

    public synchronized long getFlushInterval()
    {
        return _flushingInterval;
    }

    public synchronized void setFlushInterval(long delay)
    {
        _flushingInterval = delay;
        reschedule();
    }

    public synchronized long getHoldDelay()
    {
        return Math.max(0, _holdUntil - System.currentTimeMillis());
    }

    public synchronized void setHoldDelay(long delay)
    {
        _holdUntil = System.currentTimeMillis() + delay;
        reschedule();
    }

    public synchronized long getRetryDelayOnError()
    {
        return _retryDelayOnError;
    }

    public synchronized void setRetryDelayOnError(long delay)
    {
        _retryDelayOnError = delay;
    }

    public void start()
    {
        schedule();
    }

    public void stop()
    {
        _flushExecutor.shutdown();
    }

    public synchronized PoolFlushGainControlMessage messageArrived(PoolFlushGainControlMessage gain)
    {
        long holdTimer = gain.getHoldTimer();
        if (holdTimer > 0) {
            _holdUntil = System.currentTimeMillis() + holdTimer;
            reschedule();
        }
        if (gain.getReplyRequired()) {
            gain.setCellInfo((PoolCellInfo) getCellInfo());
            gain.setFlushInfos(_storageQueue.getFlushInfos());
        }
        return gain;
    }

    public synchronized Reply messageArrived(PoolFlushDoFlushMessage msg)
    {
        PrivateFlush flush = new PrivateFlush(msg);
        _flushExecutor.execute(new FireAndForgetTask(flush));
        return flush;
    }

    @Override
    public synchronized void printSetup(PrintWriter pw)
    {
        pw.println("#\n# Flushing Thread setup\n#");
        pw.println("flush set max active " + _maxActive);
        pw.println("flush set interval " + TimeUnit.MILLISECONDS.toSeconds(_flushingInterval));
        pw.println("flush set retry delay " + TimeUnit.MILLISECONDS.toSeconds(_retryDelayOnError));
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        pw.println("   Flush interval                : " + _flushingInterval + " ms");
        pw.println("   Maximum classes flushing      : " + _maxActive);
        pw.println("   Minimum flush delay on error  : " + _retryDelayOnError + " ms");
        if (_future != null) {
            pw.println("   Next flush                    : " + new Date(System.currentTimeMillis() + _future.getDelay(TimeUnit.MILLISECONDS)));
        }
    }

    private class PrivateFlush extends DelayedReply implements Runnable, StorageClassInfoFlushable
    {
        private final PoolFlushDoFlushMessage _flush;

        private PrivateFlush(PoolFlushDoFlushMessage flush)
        {
            _flush = flush;
        }

        @Override
        public void run()
        {
            String hsm = _flush.getHsmName();
            String storageClass = _flush.getStorageClassName();
            String composed = storageClass + "@" + hsm;

            try {
                StorageClassInfo info = _storageQueue.getStorageClassInfo(hsm, storageClass);
                if (info == null) {
                    LOGGER.error("Flush failed: No queue for {}", composed);
                    _flush.setFailed(576, "Flush failed: No queue for " + composed);
                } else {
                    int max = _flush.getMaxFlushCount();
                    long flushId = info.flush((max == 0) ? Integer.MAX_VALUE : max, this);
                    _flush.setFlushId(flushId);
                }
            } catch (RuntimeException e) {
                LOGGER.error("Private flush failed for " + composed + ". Please report to support@dcache.org", e);
                _flush.setFailed(576, e);
            }
            if (_flush.getReplyRequired()) {
                reply(_flush);
            }
        }

        @Override
        public void storageClassInfoFlushed(String hsm, String storageClass, long flushId,
                                            int requests, int failed)
        {
            LOGGER.info("Flushed: {}  {}, id={};R={};f={}", hsm, storageClass, flushId, requests, failed);
            if (_flush.getReplyRequired()) {
                _flush.setCellInfo((PoolCellInfo) getCellInfo());
                _flush.setFlushInfos(_storageQueue.getFlushInfos());
                _flush.setResult(requests, failed);
                reply(_flush);
            }
        }
    }

    private class FlushTask implements Runnable
    {
        @Override
        public void run()
        {
            _storageQueue.flushAll(getMaxActive(), _retryDelayOnError);
        }
    }

    @Command(name = "flush set max active",
            description = "Set the maximum number of storage classes to flush simultaneously.")
    class SetMaxActiveCommand implements Callable<String>
    {
        @Argument
        int concurrency;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(concurrency >= 0, "Concurrency must be non-negative");
            setMaxActive(concurrency);
            return "Max active flush set to " + concurrency;
        }
    }

    @Command(name = "flush set interval",
            description = "Set the interval at which to flush files to tape")
    class SetIntervalCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        int delay;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(delay > 0 , "Delay must be positive");
            setFlushInterval(TimeUnit.SECONDS.toMillis(delay));
            return "flushing interval set to " + delay + " seconds";
        }
    }

    @Command(name = "flush set retry delay",
            description = "Set the minimum delay before the next flush after a failure.")
    class SetRetryDelayCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        int delay;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(delay >= 0 , "Delay must be non-negative");
            setRetryDelayOnError(TimeUnit.SECONDS.toMillis(delay));
            return "Retry delay set to " + delay + " seconds";
        }
    }

    @Command(name = "flush ls",
            description = "List the storage classes queued for flush.")
    class ListCommand implements Callable<Serializable>
    {
        @Option(name = "binary")
        boolean isBinary;

        @Override
        public Serializable call()
        {
            long now = System.currentTimeMillis();
            if (!isBinary) {
                StringBuilder sb = new StringBuilder();
                sb.append(Formats.field("Class", 20, Formats.LEFT));
                sb.append(Formats.field("Active", 8, Formats.RIGHT));
                sb.append(Formats.field("Error", 8, Formats.RIGHT));
                sb.append(Formats.field("Last/min", 10, Formats.RIGHT));
                sb.append(Formats.field("Requests", 10, Formats.RIGHT));
                sb.append(Formats.field("Failed", 10, Formats.RIGHT));
                sb.append("\n");
                for (StorageClassInfo info : _storageQueue.getStorageClassInfos()) {
                    sb.append(Formats.field(info.getStorageClass() + "@" + info.getHsm(),
                            20, Formats.LEFT));
                    sb.append(Formats.field("" + info.getActiveCount(), 8, Formats.RIGHT));
                    sb.append(Formats.field("" + info.getErrorCount(), 8, Formats.RIGHT));
                    long lastSubmit = info.getLastSubmitted();
                    lastSubmit = (lastSubmit == 0L) ? 0L : (now - info.getLastSubmitted()) / 60000L;
                    sb.append(Formats.field("" + lastSubmit, 10, Formats.RIGHT));
                    sb.append(Formats.field("" + info.getRequestCount(), 10, Formats.RIGHT));
                    sb.append(Formats.field("" + info.getFailedRequestCount(), 10, Formats.RIGHT));
                    sb.append("\n");
                }
                return sb.toString();
            } else { // is binary
                List<Object[]> list = new ArrayList<>();
                for (StorageClassInfo info : _storageQueue.getStorageClassInfos()) {
                    Object[] o = new Object[7];
                    o[0] = info.getHsm();
                    o[1] = info.getStorageClass();
                    o[2] = now - info.getLastSubmitted();
                    o[3] = (long) info.getRequestCount();
                    o[4] = (long) info.getFailedRequestCount();
                    o[5] = (long) info.getActiveCount();
                    o[6] = (long) info.getErrorCount();
                    list.add(o);
                }
                return list.toArray();
            }
        }
    }

    @Command(name = "flush class",
            description = "Flush files of storage class to tape.")
    class FlushClassCommand implements Callable<String>
    {
        @Argument(index = 0)
        String hsm;

        @Argument(index = 1)
        String storageClass;

        @Option(name = "count",
                usage = "Maximum number of files to flush.")
        int count = Integer.MAX_VALUE;

        @Override
        public String call() throws IllegalArgumentException
        {
            StorageClassInfo info = _storageQueue.getStorageClassInfo(hsm, storageClass);
            if (info == null) {
                throw new IllegalArgumentException("No such storage class: " + storageClass + "@" + hsm);
            }
            long id = info.flush(count, null);
            return "Flush initiated (id=" + id + ")";
        }
    }

    @Command(name = "flush pnfsid",
            description = "Flush a single file to tape.")
    class FlushPnfsIdCommand implements Callable<String>
    {
        @Argument
        PnfsId pnfsId;

        @Override
        public String call() throws CacheException, InterruptedException
        {
            _storageQueue.flush(pnfsId,
                    new CompletionHandler<Void, PnfsId>()
                    {
                        @Override
                        public void completed(Void result, PnfsId attachment)
                        {
                        }

                        @Override
                        public void failed(Throwable exc, PnfsId attachment)
                        {
                            LOGGER.error("Flush for {} failed: {}", pnfsId, exc.toString());
                        }
                    });
            return "Flush Initiated";
        }
    }
}
