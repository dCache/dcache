package org.dcache.pool.classic;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfos;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.Formats;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;
import dmg.util.command.Option;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.PoolDataBeanProvider;
import org.dcache.pool.classic.json.SweeperData;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryChangeEvent;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.SpaceSweeperPolicy;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StickyChangeEvent;
import org.dcache.util.histograms.CountingHistogram;
import org.dcache.vehicles.FileAttributes;

import static java.util.Comparator.naturalOrder;

public class SpaceSweeper2
    implements Runnable, CellCommandListener, StateChangeListener, CellSetupProvider,
                SpaceSweeperPolicy, PoolDataBeanProvider<SweeperData>
{
    private static final Logger _log = LoggerFactory.getLogger(SpaceSweeper2.class);

    private static final DateTimeFormatter ISO8601_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault());

    private final LruQueue<PnfsId> _queue = new LruQueue<>();

    private Repository _repository;

    private Account _account;
    private Thread _thread;
    private double _margin = 0.0;

    public SpaceSweeper2()
    {
    }

    public void printSetup(PrintWriter pw)
    {
        pw.println("sweeper reclaim margin " + _margin);
    }

    @Required
    public void setRepository(Repository repository)
    {
        _repository = repository;
        _repository.addListener(this);
    }

    @Required
    public void setAccount(Account account)
    {
        _account = account;
    }

    @Required
    public synchronized void setMargin(double margin)
    {
        Preconditions.checkArgument(margin >= 0 && margin <= 1,
                                    String.format("margin percentage must be a "
                                                   + "value between 0.0 and 1.0, "
                                                   + "was given %s.", margin));
        _margin = margin;
    }

    public void start()
    {
        _thread = new Thread(this, "sweeper");
        _thread.start();
    }

    public void stop() throws InterruptedException
    {
        _thread.interrupt();
        _thread.join(1000);
    }

    /**
     * Returns true if this file is removable. This is the case if the
     * file is not sticky and is cached (which under normal
     * circumstances implies that it is ready and not precious).
     */
    @Override
    public boolean isRemovable(CacheEntry entry)
    {
        return entry.getState() == ReplicaState.CACHED && !entry.isSticky();
    }

    @Override
    public double getMargin()
    {
        return _margin;
    }

    /**
     * Returns the pnfsid of the eldest removable entry.
     */
    private synchronized PnfsId getEldest()
    {
        return _queue.getLeastRecentlyUsedElement();
    }

    /**
     * Returns the last access time of the eldest removable entry.
     */
    @Override
    public long getLru()
    {
        return _queue.getTimeOfLeastRecentlyUsedElement();
    }

    /**
     * Add entry to the queue unless it is already on the queue.
     *
     * @throws IllegalArgumentException if entry is precious or not cached
     */
    private synchronized void add(CacheEntry entry)
    {
        if (!isRemovable(entry)) {
            throw new IllegalArgumentException("Cannot add a precious or un-cached file to the sweeper queue.");
        }

        PnfsId id = entry.getPnfsId();
        if (_queue.add(id, entry.getLastAccessTime())) {
            _log.debug("Added {} to sweeper", id);
            /* The sweeper thread may be waiting for more files to
             * delete.
             */
            notifyAll();
        }
    }

    /** Remove entry from the queue.
     */
    private synchronized boolean remove(CacheEntry entry)
    {
        PnfsId id = entry.getPnfsId();
        if (_queue.remove(id)) {
            _log.debug("Removed {} from sweeper", id);
            return true;
        }
        return false;
    }

    @Override
    public synchronized void stateChanged(StateChangeEvent event)
    {
        CacheEntry entry = event.getNewEntry();
        switch (event.getNewState()) {
        case REMOVED:
        case DESTROYED:
            remove(entry);
            break;

        default:
            if (isRemovable(entry)) {
                add(entry);
            } else {
                remove(entry);
            }
            break;
        }
    }

    @Override
    public synchronized void stickyChanged(StickyChangeEvent event)
    {
        CacheEntry entry = event.getNewEntry();
        if (isRemovable(entry)) {
            add(entry);
        } else {
            remove(entry);
        }
    }

    @Override
    public synchronized void accessTimeChanged(EntryChangeEvent event)
    {
        CacheEntry entry = event.getNewEntry();
        if (remove(entry)) {
            add(entry);
        }
    }

    @AffectsSetup
    @Command(name = "sweeper reclaim margin",
                    hint = "greedily reclaim removable space",
                    description = "When the sweeper is triggered to reclaim "
                                    + "space, require free space after the "
                                    + "call to be at least this percentage "
                                    + "of total space.")
    public class SweeperReclaimMargin implements Callable<String>
    {
        @Argument
        double margin = 0.0;

        @Override
        public String call()
        {
            setMargin(margin);
            return "Reclaim margin is now set to " + margin*100 + "% of total space.";
        }
    }

    @Command(name = "sweeper purge", hint = "Purges all removable files from pool",
            description = "Initiate a sweeper thread (in this pool) to delete " +
                    "all marked removable files from the pool. Note that, if a " +
                    "file is currently in used, this file will not be deleted " +
                    "even if it has been marked for removal.")
    public class SweeperPurgeCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            new Thread("sweeper-purge") {
                @Override
                public void run()
                {
                    try {
                        long bytes = reclaim(Long.MAX_VALUE, "'sweeper purge' command");
                        _log.info("'sweeper purge' reclaimed {} bytes.", bytes);
                    } catch (InterruptedException e) {
                    }
                }
            }.start();
            return "Purging all removable files from pool.";
        }
    }

    @Command(name = "sweeper free", hint = "reclaim space",
            description = "A sweeper thread is created to reclaim the specified " +
                    "number of bytes by deleting removable files.")
    public class sweeperFreeCommand implements Callable<String>
    {
        @Argument(usage = "Specify amount of space in bytes.")
        long bytesToFree;

        @Override
        public String call()
        {
            new Thread("sweeper-free") {
                @Override
                public void run()
                {
                    try {
                        long bytes = reclaim(bytesToFree, "'sweeper free' command");
                        _log.info("'sweeper free {}' reclaimed {} bytes.", bytesToFree, bytes);
                    } catch (InterruptedException e) {
                    }
                }
            }.start();

            return String.format("Reclaiming %d bytes", bytesToFree);
        }
    }

    @Command(name = "sweeper ls", hint = "list sweeper queue")
    public class SweeperLsCommand extends DelayedCommand<String>
    {
        @Option(name = "l", usage = "Show creation and last access times.")
        boolean showVerbose;

        @Option(name = "s", usage = "Show storage info of each entry.")
        boolean showStorageInfo;

        @Override
        protected String execute()
                throws CacheException, InterruptedException
        {
            StringBuilder sb = new StringBuilder();
            List<PnfsId> list;
            synchronized (SpaceSweeper2.this) {
                list = _queue.values();
            }
            int i = 0;
            for (PnfsId id : list) {
                try {
                    CacheEntry entry = _repository.getEntry(id);
                    if (showVerbose) {
                        sb.append(Formats.field(String.valueOf(i), 3, Formats.RIGHT)).append(" ");
                        sb.append(id.toString()).append("  ");
                        sb.append(entry.getState()).append("  ");
                        sb.append(Formats.field(String.valueOf(entry.getReplicaSize()), 11, Formats.RIGHT));
                        sb.append(" ");
                        sb.append(ISO8601_FORMAT.format(Instant.ofEpochMilli(entry.getCreationTime()))).append(" ");
                        sb.append(ISO8601_FORMAT.format(Instant.ofEpochMilli(entry.getLastAccessTime()))).append(" ");
                        if (showStorageInfo) {
                            FileAttributes attributes = entry.getFileAttributes();
                            if (attributes.isDefined(FileAttribute.STORAGEINFO)) {
                                sb.append("\n    ").append(StorageInfos.extractFrom(attributes));
                            }
                        }
                        sb.append("\n");
                    } else {
                        sb.append(entry.toString()).append("\n");
                    }
                    i++;
                } catch (FileNotInCacheException e) {
                    // Ignored
                }
            }
            return sb.toString();
        }
    }

    @Override
    public SweeperData getDataObject()
    {
        SweeperData info = new SweeperData();
        info.setLabel("Space Sweeper v2");
        info.setMargin(_margin);

        List<PnfsId> list;

        synchronized (this) {
            list = _queue.values();
            info.setLruQueueSize(_queue.values().size());
            info.setLruTimestamp(System.currentTimeMillis() - getLru());
        }

        List<Double> fileLifetime = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (PnfsId id : list) {
            Long lastAccess = _queue.timeStamps.get(id);
            if (lastAccess == null) {
                continue;
            }
            long lvalue = now - lastAccess;
            if (lvalue < 0L) {
                now = System.currentTimeMillis();
                lvalue = now - lastAccess;
                if (lvalue < 0L) {
                    _log.warn("repository last access time for {}"
                                              + " is later than current "
                                              + "system time - now {}, "
                                              + "last access {}",
                              id, now, lastAccess);
                }
            }
            fileLifetime.add((double)lvalue);
        }

        CountingHistogram histogram = SweeperData.createLastAccessHistogram();
        histogram.setData(fileLifetime);
        histogram.configure();
        info.setLastAccessHistogram(histogram);
        return info;
    }

    private String getTimeString(long secin)
    {
        int sec  = Math.max(0, (int)secin);
        int min  =  sec / 60; sec  = sec  % 60;
        int hour =  min / 60; min  = min  % 60;
        int day  = hour / 24; hour = hour % 24;

        String sS = Integer.toString(sec);
        String mS = Integer.toString(min);
        String hS = Integer.toString(hour);

        StringBuilder sb = new StringBuilder();
        if (day > 0) {
            sb.append(day).append(" d ");
        }
        sb.append(hS.length() < 2 ? ( "0"+hS ) : hS).append(":");
        sb.append(mS.length() < 2 ? ( "0"+mS ) : mS).append(":");
        sb.append(sS.length() < 2 ? ( "0"+sS ) : sS);

        return sb.toString() ;
    }

    @Command(name = "sweeper get lru", hint = "get lru file time",
            description = "Return last access time (in seconds) of the least recently " +
                    "used (lsu) file on the pool.")
    public class SweeperGetLruCommand implements Callable<String>
    {
        @Option(name = "f", usage = "Show a returned time in this format: day hour:minutes:seconds")
        boolean f;

        @Override
        public String call()
        {
            long lru = (System.currentTimeMillis() - getLru()) / 1000L;
            return f ? getTimeString(lru) : (String.valueOf(lru));
        }
    }

    private long reclaim(long amount, String why)
        throws InterruptedException
    {
        _log.debug("Sweeper tries to reclaim {} bytes.", amount);

        /* We copy the entries into a tmp list to avoid
         * ConcurrentModificationException.
         */
        List<PnfsId> tmpList = _queue.values();

        /* Delete the files.
         */
        long deleted = 0;
        for (PnfsId id: tmpList) {
            try {
                CacheEntry entry = _repository.getEntry(id);

                // Removing an open file will not free space until
                // the file is closed, so we skip it this time around.
                if (entry.getLinkCount() > 0) {
                    _log.debug("File skipped by sweeper (in use): {}", entry);
                    continue;
                }
                if (!isRemovable(entry)) {
                    _log.debug("File skipped by sweeper (not removable): {}", entry);
                    continue;
                }

                long size = entry.getReplicaSize();
                _log.debug("Sweeper removes {}.", id);
                _repository.setState(id, ReplicaState.REMOVED, why);
                deleted += size;
            } catch (IllegalTransitionException | FileNotInCacheException e) {
                /* Normal if file got removed just as we wanted to
                 * remove it ourselves.
                 */
            } catch (CacheException e) {
                _log.error(e.getMessage());
            }
            if (deleted >= amount) {
                break;
            }
        }

        return deleted;
    }

    private synchronized long getMarginalBytes()
    {
        double reclaim = _repository.getSpaceRecord().getTotalSpace() * _margin;
        _log.debug("sweeper margin is {}, marginal space to reclaim is {} bytes.",
                   _margin, reclaim);
        return (long)(reclaim);
    }

    /**
     * Blocks until the requested space is larger than the free space
     * and removable space exists. Returns the number of requested
     * space exceeding the amount of free space.
     */
    public long waitForRequests()
        throws InterruptedException
    {
        Account account = _account;
        synchronized (account) {
            while (account.getRequested() <= account.getFree() ||
                   account.getRemovable() == 0) {
                account.wait();
            }
            return getMarginalBytes() + account.getRequested() - account.getFree();
        }
    }

    @Override
    public void run()
    {
        try {
            while (true) {
                if (reclaim(waitForRequests(), "sweeper making space for new data") == 0) {
                    /* The list maintained by the sweeper is imperfect
                     * in the sense that it can contain locked entries
                     * or entries in use. Thus we could be caught in a
                     * busy wait loop in which the list is not empty,
                     * but non of the entries can be removed. To avoid
                     * excessive CPU consumption we sleep for 10
                     * seconds after each iteration.
                     */
                    synchronized(this) {
                        /*
                         * will be waked up if new entry added into list
                         */
                        wait(10000);
                    }
                }
            }
        } catch (InterruptedException e) {
            /* Signals that the sweeper should quit.
             */
        } finally {
            _repository.removeListener(this);
        }
    }

    /**
     * Queue of keys ordered by a timestamp.
     */
    private static class LruQueue<T extends Comparable<T>>
    {
        /**
         * Tracks the time stamp of each element in the queue.
         */
        private final Map<T, Long> timeStamps = new HashMap<>();

        /**
         * Elements sorted by access time and value.
         * <p>
         * The comparator uses {@code timeStamps} to look up the time of keys. A compound comparator is used
         * to ensure consistency with equals (otherwise two keys with the same time would be collapsed to
         * a single element in the set).
         * <p>
         * Any element inserted into this set must have its access time recorded in {@code timeStamps}
         * before being inserted into the set. The time must not change while the key is in the set.
         */
        private final SortedSet<T> queue =
                new TreeSet<>(Comparator.<T, Long>comparing(k -> timeStamps.getOrDefault(k, 0L)).thenComparing(naturalOrder()));

        public synchronized boolean add(T key, long time)
        {
            if (timeStamps.putIfAbsent(key, time) == null) {
                queue.add(key);
                return true;
            }
            return false;
        }

        public synchronized boolean remove(T key)
        {
            if (queue.remove(key)) {
                timeStamps.remove(key);
                return true;
            }
            return false;
        }

        public synchronized T getLeastRecentlyUsedElement()
        {
            if (queue.isEmpty()) {
                return null;
            }
            return queue.first();
        }

        public synchronized long getTimeOfLeastRecentlyUsedElement()
        {
            if (queue.isEmpty()) {
                return 0;
            }
            return timeStamps.get(queue.first());
        }

        public synchronized List<T> values()
        {
            return new ArrayList<>(queue);
        }
    }
}
