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
    private final PnfsHandler     _pnfs;
    private final HsmStorageHandler2 _storage;

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
        _pnfs       = pnfs;
        _storage    = storage;

        _repository.addCacheRepositoryListener(this);
        _cell.getNucleus().newThread(this, "sweeper").start();
    }

    public long getRemovableSpace()
    {
        return _removableSpace;
    }

    public synchronized long getLRUSeconds()
    {
        try {
            if (_list.size() == 0)
                return 0L;
            CacheRepositoryEntry e = 
                _repository.getEntry(_list.iterator().next());
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
        // FIXME: This is very slow!!!
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        say("precious event : " + entry);
        try {
            PnfsId id = entry.getPnfsId();
            if (_list.remove(id))
                _removableSpace -= entry.getSize();
        } catch (CacheException e) {
            esay(entry.getPnfsId().toString()
                 + " : remove : can't get size " + e);
        }
    }

    public void sticky(CacheRepositoryEvent event)
    {
        //
        // the definition of the sticky callback guarantees that
        // we are only called if something really changed.
        //
        boolean isSticky, isPrecious;
        long    size;
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        PnfsId id = entry.getPnfsId();
        try {
            isSticky   = entry.isSticky();
            isPrecious = entry.isPrecious();
            size       = entry.getSize();
        } catch (CacheException e) {
            //
            // better to do nothing here.
            //
            esay(entry.getPnfsId().toString()
                 + " : can't get status (sticky/size) " + e);
            return;
        }
        if (isSticky) {
            say("STICKY : received sticky event : " + entry);
            synchronized (this) {
                if (_list.remove(id)) {
                    _removableSpace -= size;
                    say("STICKY : removed from list " + entry);
                }
            }
        } else {
            say("STICKY : received unsticky event : " + entry);
            if (!isPrecious) {
                try {
                    entry.touch();
                } catch (Exception e) {
                    esay(id.toString() + " : can't touch" + e);
                }
                synchronized (this) {
                    _list.add(id);
                    _removableSpace += size;
                }
                say("STICKY : added to remove list " + entry);
            }
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

        try {
            entry.touch();
        } catch(CacheException e) {
        }

        boolean found = _list.remove(id);
        try {
            if (found && !entry.isSticky())
                _list.add(id);
        } catch (CacheException e) {
            esay("can't determine stickyness : " + entry);
            _list.add(id);
        }
        say("touched event (" + found + ") : " + entry);
    }

    public synchronized void removed(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        PnfsId id = entry.getPnfsId();
        try {
            if (_list.remove(id))
                _removableSpace -= entry.getSize();
            say("removed event : " + entry);
        } catch (CacheException e) {
            esay(id.toString() + " : remove : can't get size " + e);
        }
    }

    public synchronized void needSpace(CacheNeedSpaceEvent event)
    {
        long space = event.getRequiredSpace();
        _spaceNeeded += space;
        say("needSpace event " + space + " -> " + _spaceNeeded);
    }

    public synchronized void scanned(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        PnfsId id = entry.getPnfsId();
        try {
            synchronized (entry) {
                if (entry.isCached() && !entry.isSticky()) {
                    _list.add(id);
                    try {
                        _removableSpace += entry.getSize();
                    } catch (CacheException e) {
                        esay(id.toString() + " : scanned : " + e);
                    }
                }
                say("scanned event : " + entry);
            }
        } catch (CacheException e) {
            esay("scanned event : CE " + e.getMessage());
        }
    }

    public synchronized void cached(CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry = event.getRepositoryEntry();
        PnfsId id = entry.getPnfsId();
        try {
            if (entry.isSticky())
                return;
            entry.touch();
            _list.add(id);
            say("cached event : " + entry);
            _removableSpace += entry.getSize();
        } catch(Exception e) {
            esay(entry.getPnfsId().toString() + " : cached : " + e);
        }
    }

    public String hh_sweeper_free = "<bytesToFree>";
    public String ac_sweeper_free_$_1(Args args) 
        throws NumberFormatException
    {
        long toFree = Long.parseLong(args.argv(0));
        synchronized (this) {
            _spaceNeeded += toFree ;
        }
        return "" + toFree + " bytes added to reallocation queue";
    }

    public String hh_sweeper_ls = " [-l] [-s]";
    public String ac_sweeper_ls(Args args)
        throws Exception 
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
        say("started");
        long spaceNeeded = 0;
        List<CacheRepositoryEntry> tmpList = 
            new ArrayList<CacheRepositoryEntry>();

        while (!Thread.interrupted()) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
            //
            // take the needed space out of the 'queue' and
            // added to the managed space.
            //
            synchronized (this) {
                spaceNeeded += _spaceNeeded;
                _spaceNeeded = 0;
            }

            if (spaceNeeded <= 0) continue;

            say("SS0 request to remove : " + spaceNeeded);
            //
            // we copy the entries into a tmp list to avoid
            // the ConcurrentModificationExceptions
            //
            Iterator<PnfsId> i = _list.iterator();
            try {
                long minSpaceNeeded = spaceNeeded;

                while (i.hasNext() && (minSpaceNeeded > 0)) {
                    PnfsId id = i.next();
                    try {
                        CacheRepositoryEntry entry = _repository.getEntry(id);

                        //
                        //  we are not allowed to remove the
                        //  file is
                        //    a) it is locked
                        //    b) it is still in use.
                        //
                        if (entry.isLocked() ||
                            (entry.getLinkCount() > 0) || entry.isSticky()) {
                            esay("SS0 : file skipped by remove (locked,in use,sticky) : " + entry);
                            continue;
                        }

                        if (entry.isPrecious()) {
                            esay("SS0 : PANIC file skipped by remove (precious) : " + entry);
                            continue;
                        }
                        long size = entry.getSize();
                        tmpList.add(entry);
                        minSpaceNeeded -= size;
                        say("SS0 adds to remove list : " + entry.getPnfsId() 
                            + " " + size + " -> " + spaceNeeded);
                        //
                        // the _list space will be substracted with the
                        // remove event.
                        //
                    } catch (FileNotInCacheException e) {
                        esay("SS0 : " + e);
                        _list.remove(id);
                    } catch (CacheException e) {
                        esay("SS0 : " + e);
                    }
                }
            } catch (ConcurrentModificationException e) {
                esay("SS0 (loop exited, this is not an error) : " + e);
            }
            //
            // we are not supposed to do exact space allocation.
            //

            //
            // now do it
            //
            for (CacheRepositoryEntry entry : tmpList) {
                try {
                    long size = entry.getSize();
                    say("SS0 : trying to remove " + entry.getPnfsId());
                    if (_repository.removeEntry(entry)) {
                        spaceNeeded -= size;
                    } else {
                        say("SS0 : locked (not removed) : "
                            + entry.getPnfsId());
                    }
                } catch (FileNotInCacheException e) {
                    esay("SS0 : " + e);
                    synchronized (this) {
                        _list.remove(entry.getPnfsId()); 
                    }
                } catch (CacheException e) {
                    esay("SS0 : " + e);
                }
            }
            say("SS0 loop done [cleaning tmp]");
            spaceNeeded = Math.max(spaceNeeded, 0);
            tmpList.clear();
        }
        _repository.removeCacheRepositoryListener(this);
        say("SS0 : finished");
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
