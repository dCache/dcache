package org.dcache.pool.repository;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;

import dmg.cells.nucleus.CellCommandListener;
import dmg.util.Formats;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;
import dmg.util.command.Option;

import org.dcache.namespace.FileAttribute;
import org.dcache.util.Args;
import org.dcache.util.ByteUnit;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

import static java.util.stream.Collectors.joining;
import static org.dcache.util.ByteUnit.*;

public class RepositoryInterpreter
    implements CellCommandListener
{
    private static final Logger _log =
            LoggerFactory.getLogger(RepositoryInterpreter.class);

    private static final Map<String,ByteUnit> STRING_TO_UNIT =
            ImmutableMap.of("k", KiB, "m", MiB, "g", GiB, "t", TiB);

    private Repository _repository;

    private final StatisticsListener _statisticsListener = new StatisticsListener();

    public void setRepository(Repository repository)
    {
        _repository = repository;
        _repository.addListener(_statisticsListener);
    }

    @Command(name = "rep set sticky", hint = "change sticky flags",
            description = "Changes the sticky flags on one or more replicas. Sticky flags prevent sweeper " +
                          "from garbage collecting files. A sticky flag has an owner (a name) and an expiration " +
                          "date. The expiration date may be infinite, in which case the sticky flag never expires. " +
                          "Each replica can have zero or more sticky flags.\n\n" +
                          "The command may set or clear a sticky flag of a specific replica or for a set of " +
                          "replicas matches the given filter cafeterias.")
    public class SetStickyCommand implements Callable<String>
    {
        @Argument(index = -2, required = false,
                usage = "Only change the replica with the given ID.")
        PnfsId pnfsId;

        @Argument(index = -1, valueSpec = "on|off",
                usage = "Whether to set or clear the sticky flag.")
        String state;

        @Option(name = "o", metaVar = "name", category = "Sticky properties",
                usage = "The owner is a name for the flag. A replica can only have one sticky flag per owner.")
        String owner = "system";

        @Option(name = "l", metaVar = "millis", category = "Sticky properties",
                usage = "The lifetime in milliseconds from now. Once the lifetime expires, the sticky flag is " +
                        "removed. If no other sticky flags are left and the replica is marked as a cache, sweeper " +
                        "may garbage collect it. A sticky flag with a lifetime of -1 never expires.")
        Long lifetime;

        @Option(name = "al", category = "Filter options", values={"online", "nearline"},
                usage = "Only change replicas with the given access latency.")
        AccessLatency al;

        @Option(name = "rp", category = "Filter options", values={"custodial", "replica", "output"},
                usage = "Only change replicas with the given retention policy.")
        RetentionPolicy rp;

        @Option(name = "storage", metaVar = "class", category = "Filter options",
                usage = "Only change replicas with the given storage class.")
        String storage;

        @Option(name = "cache", metaVar = "class", category = "Filter options",
                usage ="Only change replicas with the given cache class. If set to the empty string, the condition " +
                       "will match any replica that does not have a cache class.")
        String cache;

        @Option(name = "all", category = "Filter options",
                usage = "Allow using the command without any filter options and without a PNFS ID. This is a safe " +
                        "guard against accidentally changing all replicas.")
        boolean all;

        @Override
        public String call() throws Exception
        {
            long expire;
            switch (state) {
            case "on":
                expire = (lifetime != null) ? Math.max(0, System.currentTimeMillis() + lifetime) : -1;
                break;
            case "off":
                expire = 0;
                break;
            default:
                throw new IllegalArgumentException("Invalid sticky state : " + state);
            }

            if (pnfsId != null) {
                if (!matches(pnfsId)) {
                    throw new IllegalArgumentException("Replica does not match filter conditions.");
                }
                _repository.setSticky(pnfsId, owner, expire, true);
                return _repository.getEntry(pnfsId).getStickyRecords().stream()
                        .filter(StickyRecord::isValid).map(Object::toString).collect(joining("\n"));
            }

            if (al == null && rp == null && storage == null && cache == null && !all) {
                throw new IllegalArgumentException("Use -all to change sticky flag for all replicas.");
            }

            long cnt = 0;
            for (PnfsId id : _repository) {
                try {
                    if (matches(id)) {
                        _repository.setSticky(id, owner, expire, true);
                        cnt++;
                    }
                } catch (FileNotInCacheException ignored) {
                }
            }

            return cnt + " replicas updated.";
        }

        protected boolean matches(PnfsId id) throws CacheException, InterruptedException
        {
            CacheEntry entry = _repository.getEntry(id);
            FileAttributes attributes = entry.getFileAttributes();
            return (al == null || attributes.isDefined(FileAttribute.ACCESS_LATENCY) && attributes.getAccessLatency().equals(al)) &&
                   (rp == null || attributes.isDefined(FileAttribute.RETENTION_POLICY) && attributes.getRetentionPolicy().equals(rp)) &&
                   (storage == null || attributes.isDefined(FileAttribute.STORAGECLASS) && Objects.equals(attributes.getStorageClass(), storage)) &&
                   (cache == null || attributes.isDefined(FileAttribute.CACHECLASS) && Objects.equals(attributes.getCacheClass(), Strings.emptyToNull(cache)));
        }
    }

    @Command(name = "rep sticky ls", hint = "list sticky flags",
            description = "List sticky flags of a replica.")
    public class ListStickyCommand implements Callable<String>
    {
        @Argument
        PnfsId pnfsId;

        @Override
        public String call() throws CacheException, InterruptedException
        {
            CacheEntry entry = _repository.getEntry(pnfsId);
            return entry.getStickyRecords().stream()
                    .filter(StickyRecord::isValid).map(Object::toString).collect(joining("\n"));
        }
    }

    @Command(name = "rep ls", hint = "list replicas",
            description = "List the replicas in this pool.\n\n" +
                          "Each line has the following format:\n\n" +
                          "   PNFSID <STATE> <SIZE> <STORAGE CLASS>\n\n"+
                          "STATE is a sequence of fields:\n"+
                          "   field 1 is \"C\"\n" +
                          "      if entry is cached and \"-\" otherwise.\n"+
                          "   field 2 is \"P\"\n" +
                          "      if entry is precious and \"-\" otherwise.\n"+
                          "   field 3 is \"C\"\n" +
                          "      if entry is being transferred \"from client\" and \"-\" otherwise.\n" +
                          "   field 4 is \"S\"\n" +
                          "      if entry is being transferred \"from store\" and \"-\" otherwise.\n" +
                          "   field 5 is unused.\n" +
                          "   field 6 is unused.\n" +
                          "   field 7 is \"R\"\n" +
                          "      if entry is removed but still open and \"-\" otherwise.\n"+
                          "   field 8 is \"D\"\n" +
                          "      if entry is removed and \"-\" otherwise.\n"+
                          "   field 9 is \"X\"\n" +
                          "      if entry is sticky and \"-\" otherwise.\n"+
                          "   field 10 is \"E\"\n" +
                          "      if entry is in error state and \"-\" otherwise.\n"+
                          "   field 11 is unused.\n"+
                          "   field 12 is \"L(0)(n)\"\n" +
                          "      where is the link count.")
    public class ListCommand extends DelayedCommand<Serializable>
    {
        @Argument(usage = "Limit to these replicas.", required = false)
        PnfsId[] pnfsIds;

        @Option(name = "l", valueSpec = "[s|p|l|u|nc|e...]",
                usage = "Limit to replicas with these flags: \n" +
                        "   s  : sticky\n"+
                        "   p  : precious\n"+
                        "   l  : locked\n"+
                        "   u  : in use\n"+
                        "   nc : not cached\n" +
                        "   e  : error")
        String format;

        @Option(name = "storage", metaVar = "GLOB", usage = "Limit to replicas with matching storage class.")
        Glob si;

        @Option(name = "s", valueSpec = "[k|m|g|t]", values = { "k", "m", "g", "t", "" },
                usage = "Output per storage class statistics instead. Optionally use KiB, MiB, " +
                        "GiB, or TiB.")
        String stat;

        @Option(name = "sum", usage = "Include totals for all storage classes when used with -s or -binary.")
        boolean sum;

        @Option(name = "binary", usage = "Return statistics in binary format instead.")
        boolean binary;

        @Override
        public Serializable execute() throws CacheException, InterruptedException
        {
            if (pnfsIds != null) {
                return listById(pnfsIds);
            } else if (binary) {
                return listBinary();
            } else if (stat != null) {
                return listStatistics(STRING_TO_UNIT.getOrDefault(stat, BYTES));
            } else {
                return listAll();
            }
        }

        private Serializable listAll() throws CacheException, InterruptedException
        {
            String format = Strings.nullToEmpty(this.format);
            boolean notcached = format.contains("nc");
            boolean precious  = format.indexOf('p')  > -1;
            boolean sticky    = format.indexOf('s')  > -1;
            boolean used      = format.indexOf('u')  > -1;
            boolean broken    = format.indexOf('e')  > -1;
            boolean cached    = format.replace("nc", "").indexOf('c') > -1;

            Pattern siFilter = (si == null) ? null : si.toPattern();

            StringBuilder sb = new StringBuilder();
            for (PnfsId id: _repository) {
                try {
                    CacheEntry entry = _repository.getEntry(id);
                    EntryState state = entry.getState();
                    if (siFilter != null) {
                        FileAttributes attributes = entry.getFileAttributes();
                        String siValue = attributes.isDefined(FileAttribute.STORAGECLASS)
                                         ? attributes.getStorageClass()
                                         : "<unknown>";
                        if (!siFilter.matcher(siValue).matches()) {
                            continue;
                        }
                    }
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
            return sb.toString();
        }

        private Object[] listBinary()
        {
            return getStatistics(sum).entrySet().stream()
                    .map(e -> new Object[] { e.getKey(), e.getValue() })
                    .toArray(Object[]::new);
        }

        private Serializable listStatistics(ByteUnit unit)
        {
            Map<String, long[]> stats = getStatistics(sum);
            StringBuilder sb = new StringBuilder();
            stats.forEach((sc, counter) -> {
                sb.append(Formats.field(sc, 24, Formats.LEFT)).
                        append("  ").
                        append(Formats.field("" + unit.convert(counter[0], BYTES), 10, Formats.RIGHT)).
                        append("  ").
                        append(Formats.field("" + counter[1], 8, Formats.RIGHT)).
                        append("  ").
                        append(Formats.field("" + unit.convert(counter[2], BYTES), 10, Formats.RIGHT)).
                        append("  ").
                        append(Formats.field("" + counter[3], 8, Formats.RIGHT)).
                        append("  ").
                        append(Formats.field("" + unit.convert(counter[4], BYTES), 10, Formats.RIGHT)).
                        append("  ").
                        append(Formats.field("" + counter[5], 8, Formats.RIGHT)).
                        append("  ").
                        append(Formats.field("" + unit.convert(counter[6], BYTES), 10, Formats.RIGHT)).
                        append("  ").
                        append(Formats.field("" + counter[7], 8, Formats.RIGHT)).
                        append("\n");
            });
            return sb.toString();
        }

        private Serializable listById(PnfsId[] pnfsIds) throws CacheException, InterruptedException
        {
            StringBuilder sb   = new StringBuilder();
            StringBuilder exceptionMessages = new StringBuilder();
            for (PnfsId pnfsId : pnfsIds) {
                try {
                    sb.append(_repository.getEntry(pnfsId));
                    sb.append("\n");
                } catch (FileNotInCacheException fnice) {
                    exceptionMessages.append(fnice.getMessage()).append("\n");
                }
            }
            sb.append(exceptionMessages.toString());
            return sb.toString();
        }
    }

    public static final String hh_rep_rmclass = "<storageClass> # removes the from the cache";
    public String ac_rep_rmclass_$_1(Args args)
    {
        final String storageClassName = args.argv(0);
        new Thread(() -> {
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

    private Map<String, long[]> getStatistics(boolean isSumIncluded)
    {
        Map<String,long[]> map = _statisticsListener.toMap();
        if (isSumIncluded) {
            long[] counter = new long[10];
            map.put("total", counter);
            SpaceRecord record = _repository.getSpaceRecord();
            counter[0] = record.getTotalSpace();
            counter[1] = record.getFreeSpace();
            counter[2] = _statisticsListener.getOtherBytes();
        }
        return map;
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
