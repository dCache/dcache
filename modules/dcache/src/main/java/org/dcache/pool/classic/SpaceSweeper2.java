package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfos;

import dmg.cells.nucleus.CellCommandListener;
import dmg.util.Formats;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;
import dmg.util.command.Option;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryChangeEvent;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.SpaceSweeperPolicy;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StickyChangeEvent;
import org.dcache.util.Args;
import org.dcache.vehicles.FileAttributes;

import static java.util.Comparator.naturalOrder;

public class SpaceSweeper2
    implements Runnable, CellCommandListener, StateChangeListener,
               SpaceSweeperPolicy
{
    private static final Logger _log = LoggerFactory.getLogger(SpaceSweeper2.class);

    private static final DateTimeFormatter ISO8601_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault());

    private LruQueue<PnfsId> _queue = new LruQueue<>();

    private Repository _repository;

    private Account _account;
    private Thread _thread;

    public SpaceSweeper2()
    {
    }

    public void setRepository(Repository repository)
    {
        _repository = repository;
        _repository.addListener(this);
    }

    public void setAccount(Account account)
    {
        _account = account;
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
        return entry.getState() == EntryState.CACHED && !entry.isSticky();
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

    public static final String hh_sweeper_purge = "# Purges all removable files from pool";
    public synchronized String ac_sweeper_purge(Args args)
    {
        new Thread("sweeper-purge") {
            @Override
            public void run()
            {
                try {
                    long bytes = reclaim(Long.MAX_VALUE);
                    _log.info("'sweeper purge' reclaimed {} bytes.", bytes);
                } catch (InterruptedException e) {
                }
            }
        }.start();
        return "Purging all removable files from pool.";
    }

    public static final String hh_sweeper_free = "<bytesToFree>";
    public synchronized String ac_sweeper_free_$_1(Args args)
        throws NumberFormatException
    {
        final long toFree = Long.parseLong(args.argv(0));
        new Thread("sweeper-free") {
            @Override
            public void run()
            {
                try {
                    long bytes = reclaim(toFree);
                    _log.info("'sweeper free {}' reclaimed {} bytes.", toFree, bytes);
                } catch (InterruptedException e) {
                }
            }
        }.start();

        return String.format("Reclaiming %d bytes", toFree);
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
                        sb.append(Formats.field(""+i,3,Formats.RIGHT)).append(" ");
                        sb.append(id.toString()).append("  ");
                        sb.append(entry.getState()).append("  ");
                        sb.append(Formats.field(""+entry.getReplicaSize(), 11, Formats.RIGHT));
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

    public static final String hh_sweeper_get_lru = "[-f] # return lru in seconds [-f means formatted]";
    public String ac_sweeper_get_lru( Args args )
    {
        long lru = (System.currentTimeMillis() - getLru()) / 1000L;
        boolean f = args.hasOption("f");
        return f ? getTimeString(lru) : ("" + lru);
    }

    private long reclaim(long amount)
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
                _repository.setState(id, EntryState.REMOVED);
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
            return account.getRequested() - account.getFree();
        }
    }

    @Override
    public void run()
    {
        try {
            while (true) {
                if (reclaim(waitForRequests()) == 0) {
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
