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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.FileCorruptedCacheException;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Checksums;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.isEmpty;
import static org.dcache.pool.classic.ChecksumModule.PolicyFlag.*;
import static org.dcache.util.ByteUnit.BYTES;
import static org.dcache.util.ByteUnit.MiB;
import static org.dcache.util.ChecksumType.ADLER32;
import static org.dcache.util.ChecksumType.getChecksumType;

public class ChecksumModuleV1
    extends AbstractCellComponent
    implements CellCommandListener, ChecksumModule
{
    private final EnumSet<PolicyFlag> _policy = EnumSet.of(ON_TRANSFER, ENFORCE_CRC);

    private double _throughputLimit = Double.POSITIVE_INFINITY;
    private long _scrubPeriod = TimeUnit.HOURS.toMillis(24L);
    private ChecksumType _defaultChecksumType = ADLER32;

    private ChecksumScanner _scanner;

    public void setChecksumScanner(ChecksumScanner scanner)
    {
        _scanner = scanner;
    }

    public synchronized ChecksumType getDefaultChecksumType()
    {
        return _defaultChecksumType;
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
        pw.println("csm set checksumtype " + _defaultChecksumType);
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

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        pw.println("          Checksum type : " + _defaultChecksumType);
        pw.print(" Checkum calculation on : transfer ");
        for (PolicyFlag flag: _policy) {
            switch (flag) {
            case ON_READ:
                pw.print("read ");
                break;
            case ON_WRITE:
                pw.print("write ");
                break;
            case ON_FLUSH:
                pw.print("flush ");
                break;
            case ON_RESTORE:
                pw.print("restore ");
                break;
            case ENFORCE_CRC:
                pw.print("enforceCRC ");
                break;
            case GET_CRC_FROM_HSM:
                pw.print("getcrcfromhsm ");
                break;
            case SCRUB:
                pw.print("scrub(");
                pw.print("limit=" + (Double.isInfinite(_throughputLimit) ? "off" : BYTES.toMiB(_throughputLimit)));
                pw.print(",");
                pw.print("period=" + TimeUnit.MILLISECONDS.toHours(_scrubPeriod));
                pw.print(") ");
            }
        }
        pw.println("");
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
        public String call() throws Exception
        {
            return getPolicies();
        }
    }

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

                if (hasPolicy(SCRUB)) {
                    _scanner.startScrubber();
                } else {
                    _scanner.stopScrubber();
                }

                return verbose ? getPolicies() : "";
            }
        }
    }

    @Command(name = "csm set checksumtype",
            description = "Sets the default checksum type to compute and store for new files.\n\n" +
                    "Unless the client has specified a checksum of a different type, the default " +
                    "checksum defines the checksum that is computed for each new file and stored " +
                    "in the name space.")
    public class SetChecksumTypeCommand implements Callable<String>
    {
        @Argument(valueSpec = "adler32|md5")
        String type;

        @Override
        public String call() throws IllegalArgumentException
        {
            synchronized (ChecksumModuleV1.this) {
                _defaultChecksumType = getChecksumType(type);
                return "New checksumtype : "+ _defaultChecksumType;
            }
        }
    }

    private synchronized String getPolicy(PolicyFlag flag)
    {
        return hasPolicy(flag) ? "on" : "off";
    }

    @Override
    public synchronized boolean hasPolicy(PolicyFlag flag)
    {
        return _policy.contains(flag);
    }

    @Override
    public ChecksumFactory getPreferredChecksumFactory(ReplicaDescriptor handle)
            throws NoSuchAlgorithmException, CacheException
    {
        List<Checksum> existingChecksumsByPreference = Checksums.preferrredOrder().sortedCopy(handle.getChecksums());
        return ChecksumFactory.getFactory(existingChecksumsByPreference, getDefaultChecksumType());
    }

    @Override
    public void enforcePostTransferPolicy(
            ReplicaDescriptor handle, Iterable<Checksum> actualChecksums)
            throws CacheException, NoSuchAlgorithmException, IOException, InterruptedException
    {
        Iterable<Checksum> expectedChecksums = handle.getChecksums();
        if (hasPolicy(ON_WRITE)
                || (hasPolicy(ENFORCE_CRC) && isEmpty(expectedChecksums) && isEmpty(actualChecksums))) {
            ChecksumFactory factory = ChecksumFactory.getFactory(
                    concat(expectedChecksums, actualChecksums), getDefaultChecksumType());
            try (RepositoryChannel channel = handle.createChannel()) {
                actualChecksums
                        = concat(actualChecksums,
                                 Collections.singleton(factory.computeChecksum(channel)));
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
    public void enforcePostRestorePolicy(ReplicaDescriptor handle)
            throws CacheException, NoSuchAlgorithmException, IOException, InterruptedException
    {
        if (hasPolicy(ON_RESTORE)) {
            handle.addChecksums(verifyChecksum(handle));
        }
    }

    @Override
    public Iterable<Checksum> verifyChecksum(ReplicaDescriptor handle)
            throws NoSuchAlgorithmException, IOException, InterruptedException, CacheException
    {
        try (RepositoryChannel channel = handle.createChannel()) {
            return verifyChecksum(channel, handle.getChecksums());
        }
    }

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
        ChecksumFactory factory = ChecksumFactory.getFactory(expectedChecksums, getDefaultChecksumType());
        Iterable<Checksum> actualChecksums = Collections.singleton(factory.computeChecksum(channel, throughputLimit));
        compareChecksums(expectedChecksums, actualChecksums);
        return actualChecksums;
    }

    private void compareChecksums(Iterable<Checksum> expected, Iterable<Checksum> actual) throws CacheException
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
}
