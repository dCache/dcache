package diskCacheV111.util;

import java.util.*;
import java.util.concurrent.TimeUnit;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;

import diskCacheV111.vehicles.PnfsFlagMessage;

import org.dcache.pool.classic.ChecksumModuleV1;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ChecksumFactory
{
    public abstract ChecksumType getType();
    public abstract MessageDigest create();
    public abstract Checksum create(byte [] digest);
    public abstract Checksum create(String stringDigest);
    public abstract Checksum find(Set<Checksum> checksums);
    public abstract Checksum computeChecksum(File file)
        throws IOException, InterruptedException;

    /**
     * Compute the checksum for a file with a limit on how many bytes/second to
     * checksum.
     * @param file              the file to compute a checksum for.
     * @param throughputLimit   a limit on how many bytes/second that may be
     *                          checksummed.
     * @return                  the computed checksum.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract Checksum computeChecksum(File file, double throughputLimit)
        throws IOException, InterruptedException;

    public static ChecksumFactory getFactory(ChecksumType type)
        throws NoSuchAlgorithmException
    {
	return new GenericIdChecksumFactory(type);
    }

    public static void main( String [] args ) throws Exception {
       System.out.println("Getting MD4 first time");
       ChecksumFactory.getFactory(ChecksumType.MD4_TYPE);
       System.out.println("Getting MD4 second time");
       ChecksumFactory.getFactory(ChecksumType.MD5_TYPE);
    }
}

class GenericIdChecksumFactory extends ChecksumFactory
{
    private final static Logger _log =
        LoggerFactory.getLogger(GenericIdChecksumFactory.class);

    private final static long BYTES_IN_MEBIBYTE = 1024 * 1024;

    private ChecksumType _type;

    public GenericIdChecksumFactory(ChecksumType type)
        throws NoSuchAlgorithmException
    {
        _type = type;
	if (_type != ChecksumType.MD5_TYPE && _type != ChecksumType.ADLER32) {
            // we know we support the above too; check the rest
            MessageDigest.getInstance(_type.getName());
        }
    }

    public ChecksumType getType()
    {
        return _type;
    }

    public MessageDigest create()
    {
        try {
            if (_type == ChecksumType.ADLER32) {
              return new Adler32();
            }

            return MessageDigest.getInstance(_type.getName());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("This is a bug in ChecksumFactory", e);
        }
    }

    public Checksum create(byte[] digest)
    {
	return new Checksum(_type, digest);
    }

    public Checksum create(String digest)
    {
        return new Checksum(_type, digest);
    }

    public Checksum find(Set<Checksum> checksums)
    {
        for (Checksum checksum: checksums) {
            if (checksum.getType() == _type) {
                return checksum;
            }
        }
        return null;
    }

    @Override
    public Checksum computeChecksum(File file) throws IOException,
        InterruptedException
    {
        return computeChecksum(file, Double.POSITIVE_INFINITY);
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
        assert throughputLimit > 0;
        /**
         * Adjust is -elapsedTime when throughputLimit is ∞. Adjust is 0 when
         * numBytes is 0.
         */
        long adjust = (long) Math.ceil(1000 * (numBytes / throughputLimit)
                                       - elapsedTime);
        return Math.max(0, adjust);
    }

    /**
     * Return the string representation of throughput given the amount of bytes
     * read/written over a certain time period.
     * @param numBytes  no. of bytes read/written for <code>millis</code>
     *                  milliseconds.
     * @param millis    elapsed time (milliseconds) when <code>numBytes</code>
     *                  bytes were read/written.
     * @return          throughput (MiB/s) as the string representation of a
     *                  floating point number or the string "Infinity".
     */
    private String throughputAsString(long numBytes, long millis)
    {
        if (millis <= 0) {
            return "Infinity";
        } else {
            return String.valueOf((numBytes / BYTES_IN_MEBIBYTE) /
                                  (millis / 1000.0));
        }
     }

    @Override
    public Checksum computeChecksum(File file, double throughputLimit)
        throws IOException, InterruptedException
    {
        long start = System.currentTimeMillis();
        MessageDigest digest = create();
        byte [] buffer = new byte[64 * 1024];
        long sum = 0L;
        FileInputStream in = new FileInputStream(file);
        try {
            int rc;
            while ((rc = in.read(buffer, 0, buffer.length)) > 0) {
                sum += rc;
                digest.update(buffer, 0, rc);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                long adjust =
                    throughputAdjustment(throughputLimit,
                                         sum,
                                         System.currentTimeMillis() - start);
                if (adjust > 0) {
                    Thread.sleep(adjust);
                }
            }
        } finally {
            in.close();
        }
        Checksum checksum = create(digest.digest());
        _log.debug("Computed checksum for {}, length {}, checksum {} in {} ms, throughput {} MiB/s (limit {} MiB/s)",
                   new Object[] {
                       file,
                       sum,
                       checksum,
                       System.currentTimeMillis() - start,
                       throughputAsString(sum,
                                          System.currentTimeMillis() - start),
                       throughputLimit / BYTES_IN_MEBIBYTE });
        return checksum;
    }
}
