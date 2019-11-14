package org.dcache.pool.repository;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.LongFunction;
import java.util.regex.Pattern;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;

import dmg.cells.nucleus.CellCommandListener;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;
import dmg.util.command.Option;

import org.dcache.namespace.FileAttribute;
import org.dcache.util.ByteUnit;
import org.dcache.util.ColumnWriter;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

import static java.util.stream.Collectors.joining;
import static org.dcache.util.ByteUnit.*;
import static org.dcache.util.ByteUnits.jedecPrefix;
import static dmg.util.CommandException.checkCommand;
import static org.dcache.util.ByteUnit.Type.BINARY;
import static org.dcache.util.Strings.toThreeSigFig;

public class RepositoryInterpreter
    implements CellCommandListener
{
    private static final Logger _log =
            LoggerFactory.getLogger(RepositoryInterpreter.class);

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
        public String call() throws CacheException, InterruptedException, CommandException
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
                checkCommand(pnfsId != null, "No sticky state provided.");
                throw new CommandException("Invalid sticky state : " + state);
            }

            if (pnfsId != null) {
                checkCommand(matches(pnfsId), "Replica does not match filter conditions.");
                _repository.setSticky(pnfsId, owner, expire, true);
                return _repository.getEntry(pnfsId).getStickyRecords().stream()
                        .filter(StickyRecord::isValid).map(Object::toString).collect(joining("\n"));
            }

            checkCommand(al != null || rp != null || storage != null || cache != null || all,
                    "Use -all to change sticky flag for all replicas.");

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

        @Option(name = "s", valueSpec = "[b|k|m|g|t]", values = { "b", "k", "m", "g", "t", "" },
                usage = "Output per storage class statistics instead. Optionally expressing values in units: B, KiB, MiB, " +
                        "GiB, or TiB.")
        String stat;

        @Option(name = "sum", usage = "Include storage summary when used with -s or -binary.")
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
                Optional<ByteUnit> units;
                if (stat.equals("")) {
                    units = Optional.empty();
                } else {
                    units = stat.equals("b")
                            ? Optional.of(BYTES)
                            : jedecPrefix().parse(stat.toUpperCase());
                }
                return listStatistics(units);
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
                    ReplicaState state = entry.getState();
                    if (siFilter != null) {
                        FileAttributes attributes = entry.getFileAttributes();
                        String siValue = attributes.isDefined(FileAttribute.STORAGECLASS)
                                         ? attributes.getStorageClass()
                                         : "<unknown>";
                        if (!siFilter.matcher(siValue).matches()) {
                            continue;
                        }
                    }
                    if (format.isEmpty() ||
                        (notcached && state != ReplicaState.CACHED) ||
                        (precious && state == ReplicaState.PRECIOUS) ||
                        (sticky && entry.isSticky()) ||
                        (broken && state == ReplicaState.BROKEN) ||
                        (cached && state == ReplicaState.CACHED) ||
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

        private Serializable listStatistics(Optional<ByteUnit> unitsArg)
        {
            Map<String, long[]> stats = getStatistics(sum);

            Optional<String> sumLine = stats.entrySet().stream()
                    .filter(e -> e.getKey().equals("total"))
                    .map(Map.Entry::getValue)
                    .findAny()
                    .map(c -> {
                                LongFunction<String> toString = unitsArg.map(u -> {
                                            LongFunction<String> s;
                                            if (u == BYTES) {
                                                s = Long::toString;
                                            } else {
                                                s = v -> toThreeSigFig(u.convert((double)v, BYTES), 1024d);
                                            }
                                            return s;
                                        }).orElse(org.dcache.util.Strings::describeSize);

                                return "POOL SIZES: total: " + toString.apply(c[0])
                                        + ", free: " + toString.apply(c[1])
                                        + ", other: " + toString.apply(c[2]);
                            });

            ColumnWriter table = new ColumnWriter()
                    .headersInColumns()
                    .header("Storage class").left("class")
                    .space().header("Total: size").bytes("totalSize", unitsArg, BINARY).header(",").fixed(" ").space().header("files").right("totalFiles").header(";").fixed(" ")
                    .space().space().header("Precious: size").bytes("preciousSize", unitsArg, BINARY).header(",").fixed(" ").space().header("files").right("preciousFile").header(";").fixed(" ")
                    .space().space().header("Sticky: size").bytes("stickySize", unitsArg, BINARY).header(",").fixed(" ").space().header("files").right("stickyFile").header(";").fixed(" ")
                    .space().space().header("others: size").bytes("otherSize", unitsArg, BINARY).header(",").fixed(" ").space().header("files").right("otherFiles");

            stats.entrySet().stream()
                    .filter(e -> !e.getKey().equals("total"))
                    .forEach(e -> {
                        long[] counter = e.getValue();
                        table.row()
                                .value("class", e.getKey())
                                .value("totalSize", counter[0]).value("totalFiles", counter[1])
                                .value("preciousSize", counter[2]).value("preciousFiles", counter[3])
                                .value("stickySize", counter[4]).value("stickyFiles", counter[5])
                                .value("otherSize", counter[6]).value("otherFiles", counter[7]);
                    });

            return sumLine.map(s -> table + "\n\n" + s).orElse(table.toString());
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

    @Command(name = "rep rmclass", hint = "remove replicas by storage class",
            description = "Removed all replicas of this pool that belong to a given storage class. " +
                          "WARNING: This is a dangerous command and may result in data loss if misused.")
    public class RemoveClassCommand extends DelayedCommand<String>
    {
        @Argument(usage = "A storage class.")
        String storageClassName;

        @Override
        public String execute()
        {
            int cnt = 0;
            for (PnfsId id: _repository) {
                try {
                    CacheEntry entry = _repository.getEntry(id);
                    FileAttributes fileAttributes = entry.getFileAttributes();
                    if (fileAttributes.isDefined(FileAttribute.STORAGECLASS)) {
                        String sc = fileAttributes.getStorageClass();
                        if (sc.equals(storageClassName)) {
                            _repository.setState(id, ReplicaState.REMOVED,
                                    "'rep rmclass' command");
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
            return cnt + " files removed.";
        }
    }

    @Command(name = "rep rm", hint = "remove replica",
            description = "Removes a replica from the pool. The replica is only "+
                          "removed if it is CACHED and not STICKY.")
    public class RemoveCommand implements Callable<String>
    {
        @Argument(usage = "PNFS ID of replica to remove.")
        PnfsId pnfsId;

        @Option(name = "force",
                usage = "Removes replica even if it is not garbage collectable. WARNING: This is " +
                        "a dangerous option and may result in data loss if misused.")
        boolean isForced;

        @Override
        public String call() throws CacheException, InterruptedException
        {
            CacheEntry entry = _repository.getEntry(pnfsId);
            if (isForced || entry.getState() == ReplicaState.CACHED && !entry.isSticky()) {
                _log.warn("rep rm: removing {}", pnfsId);
                _repository.setState(pnfsId, ReplicaState.REMOVED,
                        "'rep rm' command");
                return "Removed " + pnfsId;
            } else {
                return "File is not removable; use -force to override";
            }
        }
    }

    @Command(name = "rep set precious", hint = "set replica precious",
            description = "Marks a replica as precious. On tape connected pools, precious " +
                          "replicas are flushed to tape.")
    public class SetPreciousCommand implements Callable<String>
    {
        @Argument(usage = "PNFS ID of replica to make precious.")
        PnfsId pnfsId;

        @Override
        public String call() throws IllegalArgumentException, IllegalTransitionException, InterruptedException, CacheException
        {
            _repository.setState(pnfsId, ReplicaState.PRECIOUS,
                    "'rep set precious' command");
            return "";
        }
    }

    @Command(name = "rep set cached", hint = "set replica cached",
            description = "Marks a replica as cached. Unless also marked sticky, cached files " +
                          "can be garbage collected. WARNING: This is a dangerous command and may " +
                          "result in data loss if misused.")
    public class SetCachedCommand implements Callable<String>
    {
        @Argument(usage = "PNFS ID of replica to make cached.")
        PnfsId pnfsId;

        @Override
        public String call() throws IllegalArgumentException, IllegalTransitionException, InterruptedException, CacheException
        {
            _repository.setState(pnfsId, ReplicaState.CACHED,
                    "'rep set cached' command");
            return "";
        }
    }

    @Command(name = "rep set broken", hint = "set replica broken",
            description = "Marks a replica as broken. Broken replicas are not served to clients. Such " +
                          "replicas are subject to an automatic error recovery upon pool restart.")
    public class SetBrokenommand implements Callable<String>
    {
        @Argument(usage = "PNFS ID of replica to mark as broken.")
        PnfsId pnfsId;

        @Override
        public String call() throws IllegalArgumentException, IllegalTransitionException, InterruptedException, CacheException
        {
            _repository.setState(pnfsId, ReplicaState.BROKEN,
                    "'rep set broken' command");
            return "";
        }
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
            return entry.getState() == ReplicaState.PRECIOUS;
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

        private synchronized void updateStatistics(EntryChangeEvent event, ReplicaState oldState, ReplicaState newState)
        {
            if (oldState == ReplicaState.CACHED || oldState == ReplicaState.PRECIOUS) {
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
            if (newState == ReplicaState.CACHED || newState == ReplicaState.PRECIOUS) {
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
