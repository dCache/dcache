package org.dcache.pool.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.DelayedReply;
import dmg.util.Formats;

import org.dcache.namespace.FileAttribute;
import org.dcache.util.Args;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;

public class RepositoryInterpreter
    implements CellCommandListener
{
    private static final Logger _log =
            LoggerFactory.getLogger(RepositoryInterpreter.class);

    private Repository _repository;

    private StatisticsListener _statiStatisticsListener = new StatisticsListener();

    public void setRepository(Repository repository)
    {
        _repository = repository;
        _repository.addListener(_statiStatisticsListener);
    }

    public static final String hh_rep_set_sticky =
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

        switch (state) {
        case "on":
            _repository.setSticky(pnfsId, owner, expire, true);
            break;
        case "off":
            _repository.setSticky(pnfsId, owner, 0, true);
            break;
        default:
            throw new
                    IllegalArgumentException("invalid sticky state : " + state);
        }
        return "";
    }

    public static final String hh_rep_sticky_ls = "<pnfsid>";
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

    public static final String fh_rep_ls =
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

    public static final String hh_rep_ls = "[-l[=s,l,u,nc,p]] [-s[=kmgt]] | [<pnfsId> [...] ]";
    public Object ac_rep_ls_$_0_99(final Args args) throws Exception
    {
        if (args.argc() > 0) {
            StringBuilder sb   = new StringBuilder();
            StringBuilder exceptionMessages = new StringBuilder();
            for (int i = 0; i < args.argc(); i++) {
                PnfsId pnfsid = new PnfsId(args.argv(i));
                try {
                    sb.append(_repository.getEntry(pnfsid));
                    sb.append("\n");
                } catch (FileNotInCacheException fnice) {
                    exceptionMessages.append(fnice.getMessage()).append("\n");
                }
            }
            sb.append(exceptionMessages.toString());
            return sb.toString();
        }

        final DelayedReply reply = new DelayedReply();
        Thread task = new Thread() {
                @Override
                public void run()
                {
                    try {
                        try {
                            reply.reply(list());
                        } catch (CacheException | RuntimeException e) {
                            reply.reply(e);
                        }
                    } catch (InterruptedException e) {
                        _log.warn("Interrupted while listing: " + e);
                    }
                }

                private Serializable list()
                    throws CacheException, InterruptedException
                {
                    StringBuilder sb = new StringBuilder();
                    String stat = args.getOpt("s");
                    if (stat != null) {
                        Map<String,long[]> map = _statiStatisticsListener.toMap();

                        if (args.hasOption("sum")) {
                            long[] counter = new long[10];
                            map.put("total", counter);
                            SpaceRecord record = _repository.getSpaceRecord();
                            counter[0] = record.getTotalSpace();
                            counter[1] = record.getFreeSpace();
                            counter[2] = _statiStatisticsListener.getOtherBytes();
                        }

                        Iterator<String> e2 = map.keySet().iterator();
                        if (args.hasOption("binary")) {
                            Object [] result = new Object[map.size()];
                            for (int i = 0; e2.hasNext(); i++) {
                                String key = e2.next();
                                Object[] ex =  new Object[2];
                                ex[0]  = key;
                                ex[1]  = map.get(key);
                                result[i] = ex;
                            }
                            return result;
                        }

                        long dev = 1L;
                        dev = (stat.contains("k")) ||
                              (stat.contains("K")) ? 1024L : dev;
                        dev = (stat.contains("m")) ||
                              (stat.contains("M")) ? (1024L*1024L) : dev;
                        dev = (stat.contains("g")) ||
                              (stat.contains("G")) ? (1024L*1024L*1024L) : dev;
                        dev = (stat.contains("t")) ||
                              (stat.contains("T")) ? (1024L*1024L*1024L*1024L) : dev;

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

                        boolean notcached = format.contains("nc");
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

    public static final String hh_rep_rmclass = "<storageClass> # removes the from the cache";
    public String ac_rep_rmclass_$_1(Args args)
    {
        final String storageClassName = args.argv(0);
        new Thread(new Runnable() {
                @Override
                public void run()
                {
                    int cnt = 0;
                    for (PnfsId id: _repository) {
                        try {
                            CacheEntry entry = _repository.getEntry(id);
                            FileAttributes fileAttributes = entry.getFileAttributes();
                            if (fileAttributes.isDefined(FileAttribute.STORAGECLASS)) {
                                String sc = fileAttributes.getStorageClass();
                                if (sc.equals(storageClassName)) {
                                    _repository.setState(id, EntryState.REMOVED);
                                    cnt++;
                                }
                            }
                        } catch (FileNotInCacheException ignored) {
                            // File was deleted - no problem
                        } catch (IllegalTransitionException ignored) {
                            // File is transient - no problem
                        } catch (CacheException e) {
                            _log.error("Failed to delete {}: {}", id, e.getMessage());
                        } catch (InterruptedException e) {
                            _log.warn("File removal was interrupted.");
                            break;
                        }
                    }
                    _log.info("'rep rmclass {}' removed {} files.", storageClassName, cnt);
                }
            }, "rmclass").start();
        return "Backgrounded";
    }

    public static final String fh_rep_rm =
        " rep rm [-force] <pnfsid>\n" +
        "        removes <pnfsid> from the pool. The file is only \n"+
        "        removed if it is CACHED and not STICKY.\n"+
        "  -force overwrites this protection and tries to remove the file\n"+
        "         in any case. If the link count is not yet 0, the file\n"+
        "         exists until zero is reached.\n"+
        "  SEE ALSO :\n"+
        "     rep rmclass ...\n";
    public static final String hh_rep_rm = "<pnfsid> [-force] # removes the pnfsfile from the cache";
    public String ac_rep_rm_$_1(Args args) throws Exception
    {
        boolean forced = args.hasOption("force");
        PnfsId pnfsId  = new PnfsId(args.argv(0));
        CacheEntry entry = _repository.getEntry(pnfsId);
        if (forced ||
            (entry.getState() == EntryState.CACHED && !entry.isSticky())) {
            _log.warn("rep rm: removing " + pnfsId);
            _repository.setState(pnfsId, EntryState.REMOVED);
            return "Removed " + pnfsId;
        } else {
            return "File is not removable; use -force to override";
        }
    }

    public static final String hh_rep_set_precious = "<pnfsId>";
    public String ac_rep_set_precious_$_1(Args args)
        throws IllegalTransitionException, CacheException, InterruptedException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        _repository.setState(pnfsId, EntryState.PRECIOUS);
        return "";
    }

    public static final String hh_rep_set_cached = "<pnfsId> # DON'T USE, Potentially dangerous";
    public String ac_rep_set_cached_$_1(Args args)
        throws IllegalTransitionException, CacheException, InterruptedException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        _repository.setState(pnfsId, EntryState.CACHED);
        return "";
    }

    public static final String hh_rep_set_broken = "<pnfsid>";
    public String ac_rep_set_broken_$_1(Args args)
        throws IllegalTransitionException, CacheException, InterruptedException
    {
        PnfsId pnfsId  = new PnfsId(args.argv(0));
        _repository.setState(pnfsId, EntryState.BROKEN);
        return "";
    }

    private static class Statistics
    {
        long bytes;
        int entries;

        long preciousBytes;
        int preciousEntries;

        long stickyBytes;
        int stickyEntries;

        long otherBytes;
        int otherEntries;

        long[] toArray()
        {
            return new long[] { bytes, entries, preciousBytes, preciousEntries, stickyBytes, stickyEntries, otherBytes, otherEntries };
        }
    }

    private static class StatisticsListener implements StateChangeListener
    {
        private final Map<String, Statistics> statistics = new HashMap<>();

        @Override
        public void stateChanged(StateChangeEvent event)
        {
            updateStatistics(event, event.getOldState(), event.getNewState());
        }

        @Override
        public void accessTimeChanged(EntryChangeEvent event)
        {
        }

        @Override
        public void stickyChanged(StickyChangeEvent event)
        {
            updateStatistics(event, event.getOldEntry().getState(), event.getNewEntry().getState());
        }

        private boolean isPrecious(CacheEntry entry)
        {
            return entry.getState() == EntryState.PRECIOUS;
        }

        private boolean isSticky(CacheEntry entry)
        {
            return entry.isSticky();
        }

        private boolean isOther(CacheEntry entry)
        {
            return !isPrecious(entry) && !isSticky(entry);
        }

        private Statistics getStatistics(FileAttributes fileAttributes)
        {
            String store = fileAttributes.getStorageClass() + "@" + fileAttributes.getHsm();
            return statistics.computeIfAbsent(store, s -> new Statistics());
        }

        private void removeStatistics(FileAttributes fileAttributes)
        {
            String store = fileAttributes.getStorageClass() + "@" + fileAttributes.getHsm();
            statistics.remove(store);
        }

        private synchronized void updateStatistics(EntryChangeEvent event, EntryState oldState, EntryState newState)
        {
            if (oldState == EntryState.CACHED || oldState == EntryState.PRECIOUS) {
                CacheEntry oldEntry = event.getOldEntry();
                Statistics stats = getStatistics(oldEntry.getFileAttributes());
                stats.bytes -= oldEntry.getReplicaSize();
                stats.entries--;
                if (isPrecious(oldEntry)) {
                    stats.preciousBytes -= oldEntry.getReplicaSize();
                    stats.preciousEntries--;
                }
                if (isSticky(oldEntry)) {
                    stats.stickyBytes -= oldEntry.getReplicaSize();
                    stats.stickyEntries--;
                }
                if (isOther(oldEntry)) {
                    stats.otherBytes -= oldEntry.getReplicaSize();
                    stats.otherEntries--;
                }
                if (stats.entries == 0) {
                    removeStatistics(oldEntry.getFileAttributes());
                }
            }
            if (newState == EntryState.CACHED || newState == EntryState.PRECIOUS) {
                CacheEntry newEntry = event.getNewEntry();
                Statistics stats = getStatistics(newEntry.getFileAttributes());
                stats.bytes += newEntry.getReplicaSize();
                stats.entries++;
                if (isPrecious(newEntry)) {
                    stats.preciousBytes += newEntry.getReplicaSize();
                    stats.preciousEntries++;
                }
                if (isSticky(newEntry)) {
                    stats.stickyBytes += newEntry.getReplicaSize();
                    stats.stickyEntries++;
                }
                if (isOther(newEntry)) {
                    stats.otherBytes += newEntry.getReplicaSize();
                    stats.otherEntries++;
                }
            }
        }

        public synchronized Map<String,long[]> toMap()
        {
            Map<String,long[]> map = new HashMap<>();
            for (Map.Entry<String, Statistics> entry : statistics.entrySet()) {
                map.put(entry.getKey(), entry.getValue().toArray());
            }
            return map;
        }

        public synchronized long getOtherBytes()
        {
            long sum = 0;
            for (Statistics stats : statistics.values()) {
                sum += stats.otherBytes;
            }
            return sum;
        }
    }
}
