/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2007-2013 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.classic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileCorruptedCacheException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.pool.PoolDataBeanProvider;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.pool.classic.json.ChecksumModuleData;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ReplicaRecord;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.isEmpty;
import static org.dcache.pool.classic.ChecksumModuleV1.PolicyFlag.*;
import static org.dcache.util.ByteUnit.*;
import static org.dcache.util.ChecksumType.ADLER32;
import static org.dcache.util.ChecksumType.MD5_TYPE;

public class ChecksumModuleV1
    implements CellCommandListener, ChecksumModule, CellSetupProvider, CellInfoProvider,
                PoolDataBeanProvider<ChecksumModuleData>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ChecksumModuleV1.class);
    private static final Map<ChecksumType,String> CHECKSUM_NAMES = ImmutableMap.of(ADLER32, "adler32", MD5_TYPE, "md5");
    private static final long MILLISECONDS_IN_SECOND = 1000;

    /**
     * The policy implemented by a ChecksumModule is determined by these policy flags.
     */
    enum PolicyFlag {
        /** Validate checksum on file read. Not implemented. */
        ON_READ,

        /** Validate checksum before flush to HSM. */
        ON_FLUSH,

        /** Validate checsum after restore from HSM. */
        ON_RESTORE,

        /** Validate checksum after file was written to pool. */
        ON_WRITE,

        /** Validate checksum while file is being written to pool. */
        ON_TRANSFER,

        /** Enforce availability of a checksum on upload. */
        ENFORCE_CRC,

        /** Retrieve checksums from HSM after restore and register in name space. */
        GET_CRC_FROM_HSM,

        /** Background checksum verification. */
        SCRUB
    }

    private final EnumSet<PolicyFlag> _policy = EnumSet.of(ON_TRANSFER, ENFORCE_CRC);

    private double _throughputLimit = Double.POSITIVE_INFINITY;
    private long _scrubPeriod = TimeUnit.HOURS.toMillis(24L);
    private EnumSet<ChecksumType> _defaultChecksumType = EnumSet.of(ADLER32);

    private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

    public void addListener(Runnable listener)
    {
        listeners.add(listener);
    }

    public void removeListener(Runnable listener)
    {
        listeners.remove(listener);
    }

    public synchronized long getScrubPeriod()
    {
        return _scrubPeriod;
    }

    public synchronized double getThroughputLimit()
    {
        return _throughputLimit;
    }

    @Override
    public synchronized void printSetup(PrintWriter pw)
    {
        pw.println("csm set checksumtype " + defaultChecksumTypes());
        if (hasPolicy(SCRUB)) {
            pw.print("csm set policy -scrub=on");
            pw.print(" -limit=" +
                    (Double.isInfinite(_throughputLimit) ? "off" : BYTES.toMiB(_throughputLimit)));
            pw.println(" -period=" + TimeUnit.MILLISECONDS.toHours(_scrubPeriod));
        } else {
            pw.println("csm set policy -scrub=off");
        }
        pw.print("csm set policy");
        pw.print(" -onread="); pw.print(getPolicy(ON_READ));
        pw.print(" -onwrite="); pw.print(getPolicy(ON_WRITE));
        pw.print(" -onflush="); pw.print(getPolicy(ON_FLUSH));
        pw.print(" -onrestore="); pw.print(getPolicy(ON_RESTORE));
        pw.print(" -enforcecrc="); pw.print(getPolicy(ENFORCE_CRC));
        pw.print(" -getcrcfromhsm="); pw.print(getPolicy(GET_CRC_FROM_HSM));
        pw.println("");
    }

    @GuardedBy("this")
    private String defaultChecksumTypes()
    {
         return _defaultChecksumType.stream().map(CHECKSUM_NAMES::get).collect(Collectors.joining(" "));
    }

    public synchronized EnumSet<ChecksumType> getDefaultChecksumTypes()
    {
        return EnumSet.copyOf(_defaultChecksumType);
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        getDataObject().print(pw);
    }

    @Override
    public synchronized ChecksumModuleData getDataObject() {
        ChecksumModuleData info = new ChecksumModuleData();
        info.setLabel("Checksum Module");
        info.setType(defaultChecksumTypes());
        Map<String, String> policies = new HashMap<>();
        _policy.stream().forEach((p) -> policies.put(p.name(), getPolicy(p)));
        policies.put(ON_TRANSFER.name(), "on");
        if (hasPolicy(SCRUB)) {
            if (!Double.isInfinite(_throughputLimit)) {
                info.setThroughputLimitInMibPerSec(BYTES.toMiB(_throughputLimit));
            }
            info.setPeriodInHours(TimeUnit.MILLISECONDS.toHours(_scrubPeriod));
        }
        return info;
    }

    private synchronized String getPolicies()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(" Policies :\n").
            append("        on read : ").append(getPolicy(ON_READ)).append("\n").
            append("       on write : ").append(getPolicy(ON_WRITE)).append("\n").
            append("       on flush : ").append(getPolicy(ON_FLUSH)).append("\n").
            append("     on restore : ").append(getPolicy(ON_RESTORE)).append("\n").
            append("    on transfer : ").append("on").append("\n").
            append("    enforce crc : ").append(getPolicy(ENFORCE_CRC)).append("\n").
            append("  getcrcfromhsm : ").append(getPolicy(GET_CRC_FROM_HSM)).append("\n").
            append("          scrub : ").append(getPolicy(SCRUB)).append("\n");
        if (hasPolicy(SCRUB)) {
            if (Double.isInfinite(_throughputLimit)) {
                sb.append("             limit  = off\n");
            } else {
                sb.append("             limit  = ").append(BYTES.toMiB(_throughputLimit)).append(" MiB/s\n");
            }
            sb.append("             period = ").append(TimeUnit.MILLISECONDS.toHours(_scrubPeriod)).append(" hours\n");
        }
        return sb.toString();
    }

    @Command(name = "csm info",
            description = "Shows current checksum module configuration.")
    public class InfoCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return getPolicies();
        }
    }

    @AffectsSetup
    @Command(name = "csm set policy",
            description = "Define the checksum policy of the pool.")
    public class SetPolicyCommand implements Callable<String>
    {
        @Option(name = "scrub",
                category = "Scrubber options",
                usage = "Periodically verify pool data against checksums.",
                values = { "", "on", "off" },
                valueSpec = "on|off")
        String scrub;

        @Option(name = "limit",
                category = "Scrubber options",
                usage = "Checksum computation throughput limit.",
                valueSpec = "<MiB/s>|off")
        String limit;

        @Option(name = "period",
                category = "Scrubber options",
                usage = "Run scrubber every HOURS hours.",
                metaVar = "hours")
        Integer period;

        @Option(name = "onread",
                category = "Transfer options",
                usage = "Not implemented.",
                values = { "", "on", "off" },
                valueSpec = "on|off")
        String onRead;

        @Option(name = "onwrite",
                category = "Transfer options",
                usage = "Compute checksum after receiving the file form the client. In contrast to " +
                        "-ontransfer, -onwrite will read back the file from disk or the disk cache " +
                        "after the transfer has completed. Be aware that this will introduce a delay " +
                        "after the transfer and that some clients may time out in the meantime.",
                values = { "", "on", "off" },
                valueSpec = "on|off")
        String onWrite;

        @Option(name = "ontransfer",
                category = "Transfer options",
                usage = "Deprecated. Always ON." +
                        "Compute checksum while receiving data from the client. If not supported " +
                        "by the transfer protocol, the checksum is computed after the upload has " +
                        "completed.",
                values = { "", "on", "off" },
                valueSpec = "on|off")
        String onTransfer;

        @Option(name = "enforcecrc",
                category = "Transfer options",
                usage = "If no checksum was calculated during the transfer and no checksum was provided " +
                        "by the client, then a checksum will be computed from the uploaded file.",
                values = { "", "on", "off" },
                valueSpec = "on|off")
        String enforceCrc;

        @Option(name = "onflush",
                category = "HSM options",
                usage = "Compute checksum before flush to HSM.",
                values = { "", "on", "off" },
                valueSpec = "on|off")
        String onFlush;

        @Option(name = "onrestore",
                category = "HSM options",
                usage = "Compute checksum after restore from HSM.",
                values = { "", "on", "off" },
                valueSpec = "on|off")
        String onRestore;

        @Option(name = "getcrcfromhsm",
                category = "HSM options",
                usage = "If enabled, the pool will collect any checksum provided by the HSM and " +
                        "store it in the name space.",
                values = { "", "on", "off" },
                valueSpec = "on|off")
        String getCrcFromHsm;

        @Option(name = "v",
                usage = "Verbose.")
        boolean verbose;

        @Option(name = "frequently",
                metaVar = "IGNORED_VALUE",
                usage = "This option is accepted but ignored.  It exists only " +
                "for backwards compatibility with older dCache pool 'setup' files")
        String ignoredValue;

        private void updatePolicy(String value, PolicyFlag flag)
        {
            if (value != null) {
                switch (value) {
                case "on":
                case "":
                    _policy.add(flag);
                    break;
                case "off":
                    _policy.remove(flag);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid value: " + value);
                }
            }
        }

        @Override
        public String call() throws IllegalArgumentException
        {
            synchronized (ChecksumModuleV1.this) {
                updatePolicy(onRead, ON_READ);
                updatePolicy(onWrite, ON_WRITE);
                updatePolicy(onFlush, ON_FLUSH);
                updatePolicy(onRestore, ON_RESTORE);
                updatePolicy(enforceCrc, ENFORCE_CRC);
                updatePolicy(getCrcFromHsm, GET_CRC_FROM_HSM);
                updatePolicy(scrub, SCRUB);

                if (limit != null) {
                    if (limit.equals("off")) {
                        _throughputLimit = Double.POSITIVE_INFINITY;
                    } else {
                        double value = MiB.toBytes(Double.parseDouble(limit));
                        if (value <= 0) {
                            throw new IllegalArgumentException("Throughput limit must be > 0");
                        }
                        _throughputLimit = value;
                    }
                }

                if (period != null) {
                    long value = TimeUnit.HOURS.toMillis(period);
                    if (value <= 0) {
                        throw new IllegalArgumentException("Scrub interval must be > 0");
                    }
                    _scrubPeriod = value;
                }
            }
            listeners.forEach(Runnable::run);
            return verbose ? getPolicies() : "";
        }
    }

    @AffectsSetup
    @Command(name = "csm set checksumtype",
            description = "Sets the default checksum types to compute and store for new files.\n\n" +
                    "Checksums of this type are always calculated and stored in the namespace.  " +
                    "Some protocols allow the client to request additional checksum types, which " +
                    "will also be stored in the namespace.")
    public class SetChecksumTypeCommand implements Callable<String>
    {
        @Argument(valueSpec = "adler32|md5")
        String[] arguments;

        @Override
        public String call() throws CommandException
        {
            EnumSet<ChecksumType> newChecksums = EnumSet.noneOf(ChecksumType.class);
            for (String argument : arguments) {
                newChecksums.add(CHECKSUM_NAMES.entrySet().stream()
                        .filter(e -> e.getValue().equalsIgnoreCase(argument))
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElseThrow(() -> new CommandException("Unknown checksum type " + argument)));
            }
            String checksumList;
            synchronized (ChecksumModuleV1.this) {
                _defaultChecksumType = newChecksums;
                checksumList = defaultChecksumTypes();
            }
            listeners.forEach(Runnable::run);
            return "New checksumtype : "+ checksumList;
        }
    }

    private synchronized String getPolicy(PolicyFlag flag)
    {
        return hasPolicy(flag) ? "on" : "off";
    }

    public boolean isScrubEnabled()
    {
        return hasPolicy(SCRUB);
    }

    private synchronized boolean hasPolicy(PolicyFlag flag)
    {
        return _policy.contains(flag);
    }

    @Override
    public Set<Checksum> verifyBrokenFile(ReplicaRecord entry, Set<Checksum> expectedChecksums) throws IOException, FileCorruptedCacheException, InterruptedException
    {
        Set<Checksum> additionalChecksums = Collections.emptySet();

        switch (entry.getState()) {
        case FROM_CLIENT:
            EnumSet<ChecksumType> types = EnumSet.noneOf(ChecksumType.class);
            expectedChecksums.stream().forEach(c -> types.add(c.getType()));
            if (hasPolicy(ON_WRITE) || (hasPolicy(ON_TRANSFER) && hasPolicy(ENFORCE_CRC))) {
                types.addAll(_defaultChecksumType);
            }

            if (!types.isEmpty()) {
                List<MessageDigest> digests = types.stream()
                        .map(ChecksumType::createMessageDigest)
                        .collect(Collectors.toList());

                try (RepositoryChannel channel = entry.openChannel(FileStore.O_READ)) {
                    Set<Checksum> actualChecksums = computeChecksums(channel, digests);
                    compareChecksums(expectedChecksums, actualChecksums);

                    additionalChecksums = Sets.difference(actualChecksums, expectedChecksums)
                            .copyInto(new HashSet<>());
                }
            }
        }

        return additionalChecksums;
    }

    @Override
    public void enforcePostTransferPolicy(
            ReplicaDescriptor handle, Iterable<Checksum> actualChecksums)
            throws CacheException, NoSuchAlgorithmException, IOException, InterruptedException
    {
        Iterable<Checksum> expectedChecksums = handle.getChecksums();
        if (hasPolicy(ON_WRITE)
                || (hasPolicy(ENFORCE_CRC) && isEmpty(expectedChecksums) && isEmpty(actualChecksums))) {
            EnumSet<ChecksumType> types = EnumSet.copyOf(_defaultChecksumType);
            expectedChecksums.forEach(c -> types.add(c.getType()));
            actualChecksums.forEach(c -> types.add(c.getType())); // REVISIT do we really need to recalculate these?

            List<MessageDigest> digests = types.stream()
                    .map(ChecksumType::createMessageDigest)
                    .collect(Collectors.toList());

            try (RepositoryChannel channel = handle.createChannel()) {
                actualChecksums = computeChecksums(channel, digests);
            }
        }
        compareChecksums(expectedChecksums, actualChecksums);
        handle.addChecksums(actualChecksums);
    }

    @Override
    public void enforcePreFlushPolicy(ReplicaDescriptor handle)
            throws CacheException, InterruptedException, NoSuchAlgorithmException, IOException
    {
        if (hasPolicy(ON_FLUSH)) {
            verifyChecksum(handle);
        }
    }

    @Override
    public void enforcePostRestorePolicy(ReplicaDescriptor handle, Set<Checksum> expectedChecksums)
            throws CacheException, NoSuchAlgorithmException, IOException, InterruptedException
    {
        if (hasPolicy(GET_CRC_FROM_HSM)) {
            LOGGER.info("Obtained checksums {} for {} from HSM", expectedChecksums,
                    handle.getFileAttributes().getPnfsId());
            handle.addChecksums(expectedChecksums);
        }

        if (hasPolicy(ON_RESTORE)) {
            handle.addChecksums(verifyChecksum(handle));
        }
    }

    @Nonnull
    @Override
    public Iterable<Checksum> verifyChecksum(ReplicaDescriptor handle)
            throws NoSuchAlgorithmException, IOException, InterruptedException, CacheException
    {
        try (RepositoryChannel channel = handle.createChannel()) {
            return verifyChecksum(channel, handle.getChecksums());
        }
    }

    @Nonnull
    @Override
    public Iterable<Checksum> verifyChecksum(RepositoryChannel channel, Iterable<Checksum> expectedChecksums)
            throws NoSuchAlgorithmException, IOException, InterruptedException, CacheException
    {
        return verifyChecksum(channel, expectedChecksums, Double.POSITIVE_INFINITY);
    }

    public Iterable<Checksum> verifyChecksumWithThroughputLimit(ReplicaDescriptor handle)
            throws IOException, InterruptedException, NoSuchAlgorithmException, CacheException
    {
        try (RepositoryChannel channel = handle.createChannel()) {
            return verifyChecksum(channel, handle.getChecksums(), getThroughputLimit());
        }
    }

    private Iterable<Checksum> verifyChecksum(RepositoryChannel channel, Iterable<Checksum> expectedChecksums, double throughputLimit)
            throws NoSuchAlgorithmException, IOException, InterruptedException, CacheException
    {
        checkArgument(!Iterables.isEmpty(expectedChecksums), "No expected checksums");

        List<MessageDigest> digests = StreamSupport.stream(expectedChecksums.spliterator(), false)
                .map(Checksum::getType)
                .map(ChecksumType::createMessageDigest)
                .collect(Collectors.toList());

        Set<Checksum> actualChecksums = computeChecksums(channel, digests, throughputLimit);
        compareChecksums(expectedChecksums, actualChecksums);
        return actualChecksums;
    }

    private void compareChecksums(Iterable<Checksum> expected, Iterable<Checksum> actual) throws FileCorruptedCacheException
    {
        Map<ChecksumType, Checksum> checksumByType = Maps.newHashMap();
        for (Checksum checksum: concat(expected, actual)) {
            Checksum otherChecksum = checksumByType.get(checksum.getType());
            if (otherChecksum != null && !otherChecksum.equals(checksum)) {
                throw new FileCorruptedCacheException(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(actual));
            }
            checksumByType.put(checksum.getType(), checksum);
        }
    }

    private Set<Checksum> computeChecksums(RepositoryChannel channel, Collection<MessageDigest> digests) throws IOException,
        InterruptedException
    {
        return computeChecksums(channel, digests, Double.POSITIVE_INFINITY);
    }

    /**
     * Compute the checksum for a file with a limit on how many bytes/second to
     * checksum.
     * @param file              the file to compute a checksum for.
     * @param digests           the digests to update with the file's content
     * @param throughputLimit   a limit on how many bytes/second that may be
     *                          checksummed.
     * @return                  the computed checksum.
     * @throws IOException
     * @throws InterruptedException
     */
    private Set<Checksum> computeChecksums(RepositoryChannel channel, Collection<MessageDigest> digests, double throughputLimit)
        throws IOException, InterruptedException
    {
        long start = System.currentTimeMillis();
        long pos = 0L;
        ByteBuffer buffer = ByteBuffer.allocate(KiB.toBytes(64));

        int rc;
        while ((rc = channel.read(buffer, pos)) > 0) {
            pos += rc;
            buffer.flip();
            digests.forEach(d -> d.update(buffer.asReadOnlyBuffer()));
            buffer.clear();
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            long adjust
                    = throughputAdjustment(throughputLimit,
                                           pos,
                                           System.currentTimeMillis() - start);
            if (adjust > 0) {
                Thread.sleep(adjust);
            }
        }

        Set<Checksum> checksums = digests.stream()
                .map(d -> new Checksum(ChecksumType.getChecksumType(d.getAlgorithm()), d.digest()))
                .collect(Collectors.toSet());

        LOGGER.debug("Computed checksum, length {}, checksum {} in {} ms{}", pos, checksums.toString(),
                   System.currentTimeMillis() - start, pos == 0 ? ""
                            : ", throughput " +
                              throughputAsString(pos, System.currentTimeMillis() - start) +
                              " MiB/s" +
                              (Double.isInfinite(throughputLimit)
                               ? ""
                               : " (limit " + BYTES.toMiB(throughputLimit) + " MiB/s)"));
        return checksums;
    }

    /**
     * Compute how much to sleep for current throughput not to exceed
     * <code>throughputLimit</code> given how many bytes read/written for a
     * certain amount of time.
     * <h3>Formula</h3>
     * <p><code>throughputLimit = numBytes/(elapsedTime + adjust)</code>
     * <p>gives:<p>
     * <code>adjust = numBytes/throughputLimit - elapsedTime</code>
     *
     * @param throughputLimit  max throughput (bytes/second). Must not be <= 0.
     * @param numBytes         no. of bytes read/written for <code>elapsedTime
     *                         </code> milliseconds. If 0, adjust will be 0.
     * @param elapsedTime      elapsed time (milliseconds) when <code>numBytes
     *                         </code> bytes were read/written.
     * @return                 how much to sleep (milliseconds) for throughput
     *                         not to exceed <code>throughputLimit</code>.
     *                         Guaranteed to be >= 0.
     */
    private long throughputAdjustment(double throughputLimit, long numBytes,
                                      long elapsedTime)
    {
        assert throughputLimit > 0 && numBytes >= 0 && elapsedTime >= 0;
        /**
         * Adjust is < 0 when numBytes/elapsedTime < throughputLimit
         * (-elapsedTime when throughputLimit is ∞). Adjust is 0 when numBytes
         * is 0.
         */
        long desiredDuration = (long) Math.ceil(MILLISECONDS_IN_SECOND *
                                                (numBytes / throughputLimit));
        long adjust = desiredDuration - elapsedTime;
        return Math.max(0, adjust);
    }

    /**
     * Return the string representation of throughput given the amount of bytes
     * read/written over a certain time period.
     * @param numBytes  no. of bytes read/written for <code>millis</code>
     *                  milliseconds.
     * @param millis    elapsed time (milliseconds) when <code>numBytes</code>
     *                  bytes were read/written. If 0 increment by 1 to avoid
     *                  printing Infinity or NaN.
     * @return          throughput in (MiB/s) as the string representation of a
     *                  floating point number. Neither NaN or Infinity will be
     *                  printed due to the incrementing of <code>millis</code>
     *                  to 1 if it has a value of 0.
     */
    private String throughputAsString(long numBytes, long millis)
    {
        return Double.toString(BYTES.toMiB((double) numBytes)
                        / (( millis == 0 ? 1 : millis ) / (double) MILLISECONDS_IN_SECOND));
    }
}
