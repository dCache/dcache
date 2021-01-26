package org.dcache.ftp.door;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.EnumSet;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.GFtpProtocolInfo;

import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Checksums;
import org.dcache.util.PortRange;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicies;

import static org.dcache.util.ByteUnit.MiB;

/**
 * A transfer that calculates a new checksum value of a file.
 */
public class ChecksumCalculatingTransfer extends Transfer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ChecksumCalculatingTransfer.class);

    private final ChecksumType desiredType;
    private final PortRange portRange;
    private final InetAddress localAddress;

    private long calculated;

    public ChecksumCalculatingTransfer(PnfsHandler pnfs, Subject subject,
            Restriction namespaceRestriction, FsPath path, ChecksumType type,
            InetAddress localAddress, PortRange range)
    {
        super(pnfs, subject, namespaceRestriction, path);
        desiredType = type;
        portRange = range;
        this.localAddress = localAddress;
    }

    public Checksum calculateChecksum() throws CacheException,
            InterruptedException, IOException, NoSuchAlgorithmException
    {
        boolean success = false;

        setAdditionalAttributes(EnumSet.of(FileAttribute.CHECKSUM));
        readNameSpaceEntry(false);
        Checksum existingChecksum = Checksums.preferrredOrder().max(getFileAttributes().getChecksums());
        MessageDigest verifyingDigest = existingChecksum.getType().createMessageDigest();
        MessageDigest desiredDigest = desiredType.createMessageDigest();

        try (ServerSocketChannel ssc = ServerSocketChannel.open()) {
            portRange.bind(ssc.socket(), localAddress);
            LOGGER.debug("calculating checksum using port {}", ssc.getLocalAddress());
            setProtocolInfo(new GFtpProtocolInfo("GFtp", 1, 0,
                    (InetSocketAddress) ssc.getLocalAddress(), 1, 1, 1,
                    MiB.toBytes(1), 0, getFileAttributes().getSize()));
            try {
                selectPoolAndStartMover(TransferRetryPolicies.tryOncePolicy());

                try (SocketChannel s = ssc.accept()) {
                    ByteBuffer buffer = ByteBuffer.allocate(MiB.toBytes(1));

                    while (s.read(buffer) > -1) {
                        advanceCalculated(buffer.position());
                        buffer.flip();
                        desiredDigest.update(buffer);
                        buffer.rewind();
                        verifyingDigest.update(buffer);
                        buffer.clear();
                    }
                }

                if (!waitForMover(30_000)) {
                    throw new TimeoutCacheException("copy: wait for DoorTransferFinishedMessage expired");
                }

                success = true;

                if (getCalculated() != getFileAttributes().getSize()) {
                    throw new CacheException("File size mismatch: " + getCalculated() + " != " + getFileAttributes().getSize());
                }

                Checksum verifyingChecksum = new Checksum(verifyingDigest);
                if (!existingChecksum.equals(verifyingChecksum)) {
                    throw new CacheException("Corrupt data: " + verifyingChecksum + " != " + existingChecksum);
                }
                return new Checksum(desiredDigest);
            } finally {
                if (!success) {
                    killMover(0, "killed by failure");
                }
            }
        }
    }

    private synchronized void advanceCalculated(long count)
    {
        calculated += count;
    }

    private synchronized long getCalculated()
    {
        return calculated;
    }

    public String getReply()
    {
        long now = System.currentTimeMillis();

        // Format taken from globus_i_gfs_control.c:955
        return "113-Status Marker\r\n"
                + " Timestamp: " + (now/1000) + "."  + (now%1000)/100 + "\r\n"
                + " Bytes Processed: " + getCalculated() + "\r\n"
                + "113 End.";
    }
}
