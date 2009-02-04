// $Id: SpaceSweeper2.java,v 1.2 2007-10-08 08:00:29 behrmann Exp $

package org.dcache.pool.classic;

import diskCacheV111.repository.*;
import diskCacheV111.util.*;
import diskCacheV111.util.event.*;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellSetupProvider;
import org.dcache.pool.repository.Account;

import dmg.util.*;
import dmg.cells.nucleus.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

public class SpaceSweeper2
    extends AbstractCacheRepositoryListener
    implements Runnable, CellCommandListener
{
    private final static Logger _log = Logger.getLogger(SpaceSweeper2.class);

    private static SimpleDateFormat __format =
        new SimpleDateFormat("HH:mm-MM/dd");

    private final Set<PnfsId> _list  = new LinkedHashSet<PnfsId>();

    private CacheRepository _repository;

    private Account _account;

    public SpaceSweeper2()
    {
    }

    public void setRepository(CacheRepository repository)
    {
        _repository = repository;
        _repository.addCacheRepositoryListener(this);
    }

    public void setAccount(Account account)
    {
        _account = account;
        new Thread(this, "sweeper").start();
    }

    /**
     * Returns true if this file is removable. This is the case if the
     * file is not sticky and is cached (which under normal
     * circumstances implies that it is ready and not precious).
     */
    private boolean isRemovable(CacheRepositoryEntry entry)
    {
        try {
            synchronized (entry) {
                return !entry.isReceivingFromClient()
                    && !entry.isReceivingFromStore()
                    && !entry.isPrecious()
                    && !entry.isSticky()
                    && entry.isCached();
            }
        } catch (CacheException e) {
            _log.error("Failed to query state of entry: " + e.getMessage());

            /* Returning false is the safe option.
             */
            return false;
        }
    }

    /**
     * Returns the pnfsid of the eldest removable entry.
     */
    private synchronized PnfsId getEldest()
    {
        if (_list.size() == 0)
            return null;
        return _list.iterator().next();
    }

    /**
     * Returns the last accss time of the eldest removable entry.
     */
    private long getLRU()
    {
        try {
            PnfsId id = getEldest();
            if (id == null)
                return 0;

            return _repository.getEntry(id).getLastAccessTime();
        } catch (CacheException e) {
            return 0L;
        }
    }

    /**
     * Add entry to the queue unless it is already on the queue.
     *
     * @throws IllegalArgumentException if entry is precious or not cached
     */
    private synchronized void add(CacheRepositoryEntry entry)
    {
        if (!isRemovable(entry)) {
            throw new IllegalArgumentException("Cannot add a precious or un-cached file to the sweeper queue.");
        }

        PnfsId id = entry.getPnfsId();
        try {
            if (_list.isEmpty()) {
                _account.setLRU(entry.getLastAccessTime());
            }

            if (_list.add(id)) {
                _log.debug("Added " + id + " to sweeper");
                _account.adjustRemovable(entry.getSize());

                /* The sweeper thread may be waiting for more files to
                 * delete.
                 */
                notifyAll();
            }
        } catch (CacheException e) {
            _log.error("Failed to add " + id.toString() + " to sweeper: " + e);
        }
    }

    /** Remove entry from the queue.
     */
    private synchronized boolean remove(CacheRepositoryEntry entry)
    {
        PnfsId id = entry.getPnfsId();
        long size = entry.getSize();
        boolean eldest = id.equals(getEldest());
        if (_list.remove(id)) {
            _log.debug("Removed " + id + " from sweeper");
            _account.adjustRemovable(-size);
            if (eldest) {
                _account.setLRU(getLRU());
            }
            return true;
        }
        return false;
    }

    public synchronized void precious(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        _log.debug("precious: " + entry);
        remove(entry);
    }

    public synchronized void sticky(CacheRepositoryEvent event)
    {
        _log.debug("sticky: " + event);
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        if (isRemovable(entry)) {
            add(entry);
        } else {
            remove(entry);
        }
    }

    public synchronized void touched(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        PnfsId id = entry.getPnfsId();

        boolean eldest = id.equals(getEldest());

        if (_list.remove(id)) {
            _log.debug("touched : " + entry);
            _list.add(id);
        }

        if (eldest) {
            _account.setLRU(getLRU());
        }
    }

    public synchronized void removed(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        remove(entry);
    }

    public synchronized void scanned(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        _log.debug("scanned event: " + entry);
        if (isRemovable(entry)) {
            add(entry);
        }
    }

    public synchronized void cached(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        PnfsId id = entry.getPnfsId();
        _log.debug("cached event: " + entry);
        if (isRemovable(entry)) {
            add(entry);
        }
    }

    public String hh_sweeper_purge = "# Purges all removable files from pool";
    public synchronized String ac_sweeper_purge(Args args) throws Exception
    {
        final long toFree = _account.getRemovable();
        new Thread("sweeper-free") {
            public void run()
            {
                reclaim(toFree);
            }
        }.start();
        return String.format("Reclaiming %d bytes", toFree);
    }

    public String hh_sweeper_free = "<bytesToFree>";
    public synchronized String ac_sweeper_free_$_1(Args args)
        throws NumberFormatException
    {
        final long toFree = Long.parseLong(args.argv(0));
        new Thread("sweeper-free") {
            public void run()
            {
                reclaim(toFree);
            }
        }.start();

        return String.format("Reclaiming %d bytes", toFree);
    }

    public String hh_sweeper_ls = " [-l] [-s]";
    public String ac_sweeper_ls(Args args)
        throws CacheException
    {
        StringBuilder sb = new StringBuilder();
        boolean l = args.getOpt("l") != null;
        boolean s = args.getOpt("s") != null;
        List<PnfsId> list;
        synchronized (this) {
            list = new ArrayList<PnfsId>(_list);
        }
        int i = 0;
        for (PnfsId id : list) {
            try {
                CacheRepositoryEntry entry = _repository.getEntry(id);
                if (l) {
                    sb.append(Formats.field(""+i,3,Formats.RIGHT)).append(" ");
                    sb.append(id.toString()).append("  ");
                    sb.append(entry.getState()).append("  ");
                    sb.append(Formats.field(""+entry.getSize(), 11, Formats.RIGHT));
                    sb.append(" ");
                    sb.append(__format.format(new Date(entry.getCreationTime()))).append(" ");
                    sb.append(__format.format(new Date(entry.getLastAccessTime()))).append(" ");
                    StorageInfo info = entry.getStorageInfo();
                    if ((info != null) && s)
                        sb.append("\n    ").append(info.toString());
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
        if (day > 0) sb.append(day).append(" d ");
        sb.append(hS.length() < 2 ? ( "0"+hS ) : hS).append(":");
        sb.append(mS.length() < 2 ? ( "0"+mS ) : mS).append(":");
        sb.append(sS.length() < 2 ? ( "0"+sS ) : sS);

        return sb.toString() ;
    }

    public String hh_sweeper_get_lru = "[-f] # return lru in seconds [-f means formatted]";
    public String ac_sweeper_get_lru( Args args )
    {
        long lru = (System.currentTimeMillis() - getLRU()) / 1000L;
        boolean f = args.getOpt("f") != null;
        return f ? getTimeString(lru) : ("" + lru);
    }

    private long reclaim(long amount)
    {
        List<CacheRepositoryEntry> tmpList =
            new ArrayList<CacheRepositoryEntry>();

        _log.info(String.format("Sweeper trying to reclaim %d bytes", amount));

        /* We copy the entries into a tmp list to avoid
         * ConcurrentModificationException.
         */
        synchronized (this) {
            Iterator<PnfsId> i = _list.iterator();
            long minSpaceNeeded = amount;

            while (i.hasNext() && minSpaceNeeded > 0) {
                PnfsId id = i.next();
                try {
                    CacheRepositoryEntry entry = _repository.getEntry(id);

                    //
                    //  we are not allowed to remove the
                    //  file if
                    //    a) it is locked
                    //    b) it is still in use.
                    //
                    if (entry.isLocked()) {
                        _log.warn("File skipped by sweeper (locked): " + entry);
                        continue;
                    }
                    if (entry.getLinkCount() > 0) {
                        _log.warn("file skipped by sweeeper (in use): " + entry);
                        continue;
                    }
                    if (!isRemovable(entry)) {
                        _log.fatal("file skipped by sweeper (not removable): " + entry);
                        continue;
                    }
                    long size = entry.getSize();
                    tmpList.add(entry);
                    minSpaceNeeded -= size;
                    _log.debug("adds to remove list : " + entry.getPnfsId()
                               + " " + size);
                } catch (CacheException e) {
                    _log.error(e.getMessage());
                }
            }
        }

        /* Delete the files.
         */
        long deleted = 0;
        for (CacheRepositoryEntry entry: tmpList) {
            try {
                long size = entry.getSize();
                _log.error("trying to remove " + entry.getPnfsId());
                if (_repository.removeEntry(entry)) {
                    deleted += size;
                } else {
                    _log.info("locked (not removed): " + entry.getPnfsId());
                }
            } catch (CacheException e) {
                _log.error(e.toString());
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
        synchronized (_account) {
            while (_account.getRequested() <= _account.getFree() ||
                   _account.getRemovable() == 0) {
                _account.wait();
            }
            return _account.getRequested() - _account.getFree();
        }
    }

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
                    Thread.currentThread().sleep(10000);
                }
            }
        } catch (InterruptedException e) {
            /* Signals that the sweeper should quit.
             */
        } finally {
            _repository.removeCacheRepositoryListener(this);
        }
    }
}
