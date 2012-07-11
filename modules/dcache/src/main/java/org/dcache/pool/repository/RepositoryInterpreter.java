package org.dcache.pool.repository;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import dmg.util.Args;
import dmg.util.Formats;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.cells.CellCommandListener;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;

import static com.google.common.base.Preconditions.checkArgument;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryInterpreter
    implements CellCommandListener
{
    private final static Logger _log =
        LoggerFactory.getLogger(RepositoryInterpreter.class);
    private Repository _repository;
    private Account _account;

    public RepositoryInterpreter()
    {
    }

    public void setRepository(Repository repository)
    {
        _repository = repository;
    }

    public void setAccount(Account account)
    {
        _account = account;
    }

    public String hh_rep_set_sticky =
        "[-o=<owner>] [-l=<lifetime in ms>] <pnfsid> on|off";
    public String ac_rep_set_sticky_$_2(Args args)
        throws CacheException, InterruptedException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        String state = args.argv(1);
        String owner = "system";
        if (args.hasOption("o")) {
            owner = args.getOpt("o");
        }

        long expire = -1;
        if (args.hasOption("l")) {
            long argValue = Long.parseLong(args.getOpt("l"));
            checkArgument(argValue > 0, "the -l option must be a positive integer.");
            expire = System.currentTimeMillis() + argValue;
        }

        if (state.equals("on")) {
            _repository.setSticky(pnfsId, owner, expire, true);
        } else if (state.equals("off")) {
            _repository.setSticky(pnfsId, owner, 0, true);
        } else {
            throw new
                IllegalArgumentException("invalid sticky state : " + state);
        }
        return "";
    }

    public String hh_rep_sticky_ls = "<pnfsid>";
    public String ac_rep_sticky_ls_$_1(Args args)
        throws CacheException, InterruptedException
    {
        PnfsId pnfsId  = new PnfsId(args.argv(0));
        CacheEntry entry = _repository.getEntry(pnfsId);
        StringBuilder sb = new StringBuilder();
        for(StickyRecord record: entry.getStickyRecords()) {
            sb.append(record).append('\n');
        }
        return sb.toString();
    }

    public String fh_rep_ls =
        "\n"+
        " Format I  :  [<pnfsId> [<pnfsId> [...]]]\n"+
        " Format II : -l[=<selectionOptions>] [-s]\n"+
        "              Options :\n"+
        "              -l[=splunc]  # selected list\n"+
        "                 s  : sticky files\n"+
        "                 p  : precious files\n"+
        "                 l  : locked files\n"+
        "                 u  : files in use\n"+
        "                 nc : files which are not cached\n" +
        "                 e  : files which error condition\n" +
        "              -s[=kmgt] [-sum]       # statistics\n" +
        "                 k  : data amount in KBytes\n"+
        "                 m  : data amount in MBytes\n"+
        "                 g  : data amount in GBytes\n"+
        "                 t  : data amount in TBytes\n"+
        " Output is a list of repository entries, one per line\n" +
        " each line has the followin syntax:\n" +
        "<pnfsid> <state> <size> <storageinfo>\n"+
        " state is a sequence of state bits inclosed in angular \"<>\" brackets \n"+
        " bit 1 is \"C\" if entry is cached or \"-\" if not \n"+
        " bit 2 is \"P\" if entry is precious or \"-\" if not \n"+
        " bit 3 is \"C\" if entry is being transfered \"from client\" or \"-\" if not \n"+
        " bit 4 is \"S\" if entry is being transfered \"from store\" or \"-\" if not \n"+
        " bit 5 is \"c\" if entry is being transfered \"to client\" or \"-\" if not \n"+
        " bit 6 is \"s\" if entry is being transfered \"to store\" or \"-\" if not \n"+
        " bit 7 is \"R\" if entry is removed or \"-\" if not \n"+
        " bit 8 is is always \"-\" \n"+
        " bit 9 is \"X\" if entry is sticky or \"-\" if not \n"+
        " bit 10 is \"E\" if entry is in error state or \"-\" if not \n"+
        " bit 11 is \"L(x)(y)\" if entry is in locked or \"-\" if not \n"+
        "        x is epoch until which the entry is locked, 0 for non expiring lock \n"+
        "        y is the link count";

    public String hh_rep_ls = "[-l[=s,l,u,nc,p]] [-s[=kmgt]] | [<pnfsId> [...] ]";
    public Object ac_rep_ls_$_0_99(final Args args) throws Exception
    {
        if (args.argc() > 0) {
            StringBuilder sb   = new StringBuilder();
            for (int i = 0; i < args.argc(); i++) {
                PnfsId pnfsid = new PnfsId(args.argv(i));
                sb.append(_repository.getEntry(pnfsid));
                sb.append("\n");
            }
            return sb.toString();
        }

        final DelayedReply reply = new DelayedReply();
        Thread task = new Thread() {
                void reply(Object o)
                    throws InterruptedException
                {
                    try {
                        reply.send(o);
                    } catch (NoRouteToCellException e) {
                        _log.error("Failed to send reply for 'rep ls': " + e);
                    }
                }

                @Override
                public void run()
                {
                    try {
                        try {
                            reply(list());
                        } catch (CacheException e) {
                            reply(e);
                        } catch (RuntimeException e) {
                            reply(e);
                        }
                    } catch (InterruptedException e) {
                        _log.warn("Interrupted while listing: " + e);
                    }
                }

                private Object list()
                    throws CacheException, InterruptedException
                {
                    StringBuilder sb = new StringBuilder();
                    String stat = args.getOpt("s");
                    if (stat != null) {
                        long dev = 1;
                        dev = (stat.indexOf("k") > -1) ||
                            (stat.indexOf("K") > -1) ? 1024L : dev;
                        dev = (stat.indexOf("m") > -1) ||
                            (stat.indexOf("M") > -1) ? (1024L*1024L) : dev;
                        dev = (stat.indexOf("g") > -1) ||
                            (stat.indexOf("G") > -1) ? (1024L*1024L*1024L) : dev;
                        dev = (stat.indexOf("t") > -1) ||
                            (stat.indexOf("T") > -1) ? (1024L*1024L*1024L*1024L) : dev;
                        Map<String,long[]> map = new HashMap<String,long[]>();
                        long removable = 0L;
                        for (PnfsId id: _repository) {
                            try {
                                CacheEntry entry = _repository.getEntry(id);
                                StorageInfo info = entry.getStorageInfo();
                                if (info == null) {
                                    continue;
                                }
                                String sc = info.getStorageClass()
                                    + "@" + info.getHsm();

                                long[] counter = map.get(sc);
                                if (counter == null) {
                                    map.put(sc, counter = new long[8]);
                                }

                                boolean sticky = entry.isSticky();
                                boolean precious =
                                    (entry.getState() == EntryState.PRECIOUS);
                                long entrySize = entry.getReplicaSize();

                                counter[0] += entrySize;
                                counter[1]++;

                                if (precious) {
                                    counter[2] += entrySize;
                                    counter[3]++;
                                }
                                if (sticky) {
                                    counter[4] += entrySize;
                                    counter[5]++;
                                }
                                if (!precious && !sticky) {
                                    counter[6] += entrySize;
                                    removable  += entrySize;
                                    counter[7]++;
                                }
                            } catch (FileNotInCacheException e) {
                                // Entry deleted - no problem
                            }
                        }
                        if (args.hasOption("sum")) {
                            long[] counter = new long[10];
                            map.put("total", counter);
                            SpaceRecord record = _repository.getSpaceRecord();
                            counter[0] = record.getTotalSpace();
                            counter[1] = record.getFreeSpace();
                            counter[2] = removable;
                        }

                        Iterator<String> e2 = map.keySet().iterator();
                        if (args.hasOption("binary")) {
                            Object [] result = new Object[map.size()];
                            for (int i = 0; e2.hasNext(); i++) {
                                Object[] ex =  new Object[2];
                                ex[0]  = e2.next();
                                ex[1]  = map.get(ex[0]);
                                result[i] = ex;
                            }
                            return result;
                        }

                        while (e2.hasNext()) {
                            String sc = e2.next();
                            long[] counter = map.get(sc);
                            sb.append(Formats.field(sc,24,Formats.LEFT)).
                                append("  ").
                                append(Formats.field(""+counter[0]/dev,10,Formats.RIGHT)).
                                append("  ").
                                append(Formats.field(""+counter[1],8,Formats.RIGHT)).
                                append("  ").
                                append(Formats.field(""+counter[2]/dev,10,Formats.RIGHT)).
                                append("  ").
                                append(Formats.field(""+counter[3],8,Formats.RIGHT)).
                                append("  ").
                                append(Formats.field(""+counter[4]/dev,10,Formats.RIGHT)).
                                append("  ").
                                append(Formats.field(""+counter[5],8,Formats.RIGHT)).
                                append("  ").
                                append(Formats.field(""+counter[6]/dev,10,Formats.RIGHT)).
                                append("  ").
                                append(Formats.field(""+counter[7],8,Formats.RIGHT)).
                                append("\n");
                        }
                    } else  {
                        String format = args.getOpt("l");
                        format = format == null ? "" : format;

                        boolean notcached = format.indexOf("nc") > -1;
                        boolean precious  = format.indexOf('p')  > -1;
                        boolean locked    = format.indexOf('l')  > -1;
                        boolean sticky    = format.indexOf('s')  > -1;
                        boolean used      = format.indexOf('u')  > -1;
                        boolean broken    = format.indexOf('e')  > -1;
                        boolean cached    = format.indexOf('c')  > -1;

                        for (PnfsId id: _repository) {
                            try {
                                CacheEntry entry = _repository.getEntry(id);
                                EntryState state = entry.getState();
                                if (format.length() == 0 ||
                                    (notcached && state != EntryState.CACHED) ||
                                    (precious && state == EntryState.PRECIOUS) ||
                                    (sticky && entry.isSticky()) ||
                                    (broken && state == EntryState.BROKEN) ||
                                    (cached && state == EntryState.CACHED) ||
                                    (used && entry.getLinkCount() > 0)) {

                                    sb.append(entry).append('\n');
                                }
                            } catch (FileNotInCacheException e) {
                                // Entry was deleted; no problem
                            }
                        }
                    }
                    return sb.toString();
                }
            };
        task.start();
        return reply;
    }

    public String hh_rep_rmclass = "<storageClass> # removes the from the cache";
    public String ac_rep_rmclass_$_1(Args args) throws Exception
    {
        final String storageClassName = args.argv(0);
        new Thread(new Runnable() {
                public void run()
                {
                    for (PnfsId id: _repository) {
                        try {
                            CacheEntry entry = _repository.getEntry(id);

                            StorageInfo info = entry.getStorageInfo();
                            if (info == null) {
                                continue;
                            }
                            String sc = info.getStorageClass();
                            if (sc.equals(storageClassName)) {
                                _repository.setState(id, EntryState.REMOVED);
                            }
                        } catch (FileNotInCacheException e) {
                            // File was deleted - no problem
                        } catch (IllegalTransitionException e) {
                            // File is transient - no problem
                        } catch (CacheException e) {
                            _log.error("File removal failed: " + e.getMessage());
                        } catch (InterruptedException e) {
                            _log.warn("File removal was interrupted: " +
                                      e.getMessage());
                            break;
                        }
                    }
                }
            }, "rmclass").start();
        return "Backgrounded";
    }

    public String fh_rep_rm =
        " rep rm [-force] <pnfsid>\n" +
        "        removes <pnfsid> from the pool. The file is only \n"+
        "        removed if it is CACHED and not STICKY.\n"+
        "  -force overwrites this protection and tries to remove the file\n"+
        "         in any case. If the link count is not yet 0, the file\n"+
        "         exists until zero is reached.\n"+
        "  SEE ALSO :\n"+
        "     rep rmclass ...\n";
    public String hh_rep_rm = "<pnfsid> [-force]# removes the pnfsfile from the cache";
    public String ac_rep_rm_$_1(Args args) throws Exception
    {
        boolean forced = args.hasOption("force");
        PnfsId pnfsId  = new PnfsId(args.argv(0));
        CacheEntry entry = _repository.getEntry(pnfsId);
        int client = 0;
        if (forced ||
            (entry.getState() == EntryState.CACHED && !entry.isSticky())) {
            _log.warn("rep rm: removing " + pnfsId);
            _repository.setState(pnfsId, EntryState.REMOVED);
            return "Removed " + pnfsId;
        } else {
            return "File is not removable; use -force to override";
        }
    }

    public String hh_rep_set_precious = "<pnfsId>";
    public String ac_rep_set_precious_$_1(Args args)
        throws IllegalTransitionException, CacheException, InterruptedException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        _repository.setState(pnfsId, EntryState.PRECIOUS);
        return "";
    }

    public String hh_rep_set_cached = "<pnfsId> # DON'T USE, Potentially dangerous";
    public String ac_rep_set_cached_$_1(Args args)
        throws IllegalTransitionException, CacheException, InterruptedException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        _repository.setState(pnfsId, EntryState.CACHED);
        return "";
    }

    public String hh_rep_set_broken = "<pnfsid>";
    public String ac_rep_set_broken_$_1(Args args)
        throws IllegalTransitionException, CacheException, InterruptedException
    {
        PnfsId pnfsId  = new PnfsId(args.argv(0));
        _repository.setState(pnfsId, EntryState.BROKEN);
        return "";
    }
}
