// $Id: SpaceSweeper2.java,v 1.2 2007-10-08 08:00:29 behrmann Exp $

package diskCacheV111.pools;

import diskCacheV111.repository.*;
import diskCacheV111.util.*;
import diskCacheV111.util.event.*;
import diskCacheV111.vehicles.StorageInfo;

import dmg.util.*;
import dmg.cells.nucleus.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.io.PrintWriter;

public class SpaceSweeper2 implements SpaceSweeper, Runnable
{
    private final CacheRepository _repository;
    private final CellAdapter     _cell;

    private final Set<PnfsId> _list  = new LinkedHashSet<PnfsId>();

    private long            _spaceNeeded = 0;

    private long            _removableSpace = 0;

    private static SimpleDateFormat __format =
        new SimpleDateFormat("HH:mm-MM/dd");

    public SpaceSweeper2(CellAdapter cell,
                         PnfsHandler pnfs,
                         CacheRepository repository,
                         HsmStorageHandler2 storage)
    {
        _repository = repository;
        _cell       = cell;

        _repository.addCacheRepositoryListener(this);
        _cell.getNucleus().newThread(this, "sweeper").start();
    }

    public synchronized long getRemovableSpace()
    {
        return _removableSpace;
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
            esay("Failed to query state of entry: " + e.getMessage());

            /* Returning false is the safe option.
             */
            return false;
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
            if (_list.add(id)) {
                say("added " + id + " to list");
                entry.touch();
                _removableSpace += entry.getSize();

                /* The sweeper thread may be waiting for more files to
                 * delete.
                 */
                notifyAll();
            }
        } catch (CacheException e) {
            esay("failed to add " + id.toString() + " to list: " + e);
        }
    }

    /** Remove entry from the queue.
     */
    private synchronized boolean remove(CacheRepositoryEntry entry)
    {
        PnfsId id = entry.getPnfsId();
        try {
            long size = entry.getSize();
            if (_list.remove(id)) {
                say("removed " + id + " from list");
                _removableSpace -= size;
                return true;
            }
        } catch (CacheException e) {
            esay("failed to remove " + id.toString() + " from list: " + e);
        }
        return false;
    }

    private synchronized PnfsId getLRUId()
    {
        if (_list.size() == 0)
            return null;
        return _list.iterator().next();
    }

    public long getLRUSeconds()
    {
        try {
            PnfsId id = getLRUId();
            if (id == null)
                return 0;

            CacheRepositoryEntry e = _repository.getEntry(id);
            return (System.currentTimeMillis() - e.getLastAccessTime()) / 1000L;
        } catch (CacheException e) {
            return 0L;
        }
    }

    public void actionPerformed(CacheEvent event)
    {
        /* forced by CacheRepositoryListener interface */
    }

    public synchronized void precious(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        say("precious: " + entry);
        remove(entry);
    }

    public synchronized void sticky(CacheRepositoryEvent event)
    {
        say("sticky: " + event);
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        if (isRemovable(entry)) {
            add(entry);
        } else {
            remove(entry);
        }
    }

    public void available(CacheRepositoryEvent event)
    {
        /* forced by CacheRepositoryListener interface */
    }

    public void created(CacheRepositoryEvent event)
    {
        /* forced by CacheRepositoryListener interface */
    }

    public void destroyed(CacheRepositoryEvent event)
    {
        /* forced by CacheRepositoryListener interface */
    }

    public synchronized void touched(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        PnfsId id = entry.getPnfsId();

        if (_list.remove(id)) {
            say("touched : " + entry);
            try {
                entry.touch();
            } catch (CacheException e) {
                say("failed to touch data file: " + e.getMessage());
            }
            _list.add(id);
        }
    }

    public synchronized void removed(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        remove(entry);
    }

    public synchronized void needSpace(CacheNeedSpaceEvent event)
    {
        long space = event.getRequiredSpace();
        _spaceNeeded += space;
        say("needSpace event " + space + " -> " + _spaceNeeded);
        notifyAll();
    }

    public synchronized void scanned(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        say("scanned event : " + entry);
        if (isRemovable(entry)) {
            add(entry);
        }
    }

    public synchronized void cached(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        PnfsId id = entry.getPnfsId();
        say("cached event : " + entry);
        if (isRemovable(entry)) {
            add(entry);
        }
    }

    public String hh_sweeper_purge = "# Purges all removable files from pool";
    public synchronized String ac_sweeper_purge(Args args) throws Exception
    {
        long toFree = getRemovableSpace();
        _spaceNeeded += toFree;
        notifyAll();
        return "" + toFree + " bytes added to reallocation queue";
    }

    public String hh_sweeper_free = "<bytesToFree>";
    public synchronized String ac_sweeper_free_$_1(Args args)
        throws NumberFormatException
    {
        long toFree = Long.parseLong(args.argv(0));
        _spaceNeeded += toFree;
        notifyAll();
        return "" + toFree + " bytes added to reallocation queue";
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
        long lru = getLRUSeconds();
        boolean f = args.getOpt("f") != null;
        return f ? getTimeString(lru) : ("" + lru);
    }

    public void run()
    {
        long spaceNeeded = 0;
        say("started");
        List<CacheRepositoryEntry> tmpList =
            new ArrayList<CacheRepositoryEntry>();

        try {
            while (!Thread.interrupted()) {
                /* The list maintained by the sweeper is imperfect in
                 * the sense that it can contain locked entries or
                 * entries in use. Thus we could be caught in a busy
                 * wait loop in which the list is not empty, but non
                 * of the entries can be removed. To aoid excessive
                 * CPU consumption we sleep for 10 seconds after each
                 * iteration.
                 */
                Thread.currentThread().sleep(10000);

                /* Take the needed space out of the 'queue'.
                 */
                synchronized (this) {
                    while (spaceNeeded + _spaceNeeded == 0 || _list.isEmpty()) {
                        wait();
                    }
                    spaceNeeded += _spaceNeeded;
                    _spaceNeeded = 0;
                }

                say("request to remove : " + spaceNeeded);

                /* We copy the entries into a tmp list to avoid the
                 * ConcurrentModificationException.
                 */
                try {
                    Iterator<PnfsId> i = _list.iterator();
                    long minSpaceNeeded = spaceNeeded;

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
                                esay("file skipped by remove (locked) : " + entry);
                                continue;
                            }
                            if (entry.getLinkCount() > 0) {
                                esay("file skipped by remove (in use) : " + entry);
                                continue;
                            }
                            if (!isRemovable(entry)) {
                                esay("FATAL: file skipped by remove (not removable) : " + entry);
                                continue;
                            }
                            long size = entry.getSize();
                            tmpList.add(entry);
                            minSpaceNeeded -= size;
                            say("adds to remove list : " + entry.getPnfsId()
                                + " " + size + " -> " + spaceNeeded);
                        } catch (CacheException e) {
                            esay(e.getMessage());
                        }
                    }
                } catch (ConcurrentModificationException e) {
                    /* Loop exited, this is not an error.  We are not
                     * supposed to do exact space allocation.
                     */
                }

                /* Delete the files.
                 */
                for (CacheRepositoryEntry entry : tmpList) {
                    try {
                        long size = entry.getSize();
                        say("trying to remove " + entry.getPnfsId());
                        if (_repository.removeEntry(entry)) {
                            spaceNeeded -= size;
                        } else {
                            say("locked (not removed) : "
                                + entry.getPnfsId());
                        }
                    } catch (CacheException e) {
                        esay(e.toString());
                    }
                }
                spaceNeeded = Math.max(spaceNeeded, 0);
                tmpList.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            _repository.removeCacheRepositoryListener(this);
            say("finished");
        }
    }

    public void printSetup(PrintWriter pw)
    {
        pw.println("#\n# Nothing from the "
                   + this.getClass().getName() + "#");
    }

    private void say(String msg)
    {
        _cell.say("SWEEPER : " + msg);
    }

    private void esay(String msg)
    {
        _cell.esay("SWEEPER ERROR : " + msg);
    }
}
