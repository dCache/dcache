package diskCacheV111.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

import static org.dcache.util.ByteUnit.BYTES;
import static org.dcache.util.ByteUnit.KiB;

public abstract class ChecksumFactory
{
    public abstract ChecksumType getType();
    public abstract MessageDigest create();
    public abstract Checksum create(byte [] digest);
    public abstract Checksum create(String stringDigest);
    public abstract Checksum find(Set<Checksum> checksums);
    public abstract Checksum computeChecksum(RepositoryChannel channel)
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
    public abstract Checksum computeChecksum(RepositoryChannel file, double throughputLimit)
        throws IOException, InterruptedException;

    public static ChecksumFactory getFactory(ChecksumType type)
        throws NoSuchAlgorithmException
    {
	return new GenericIdChecksumFactory(type);
    }

    /**
     * Returns a ChecksumFactory for the first supported checksum.
     *
     * @param preferredChecksums Ordered list of checksums
     * @param defaultType Default type used when none of the preferred types are supported
     */
    public static ChecksumFactory getFactory(Iterable<Checksum> preferredChecksums, ChecksumType defaultType)
            throws NoSuchAlgorithmException
    {
        for (Checksum checksum : preferredChecksums) {
            try {
                return getFactory(checksum.getType());
            } catch (NoSuchAlgorithmException ignored) {
            }
        }
        return getFactory(defaultType);
    }

    public static ChecksumFactory getFactoryFor(Checksum checksum)
            throws NoSuchAlgorithmException
    {
        return getFactory(checksum.getType());
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
    private static final Logger _log =
        LoggerFactory.getLogger(GenericIdChecksumFactory.class);

    private static final long MILLISECONDS_IN_SECOND = 1000;

    private final ChecksumType _type;

    public GenericIdChecksumFactory(ChecksumType type)
        throws NoSuchAlgorithmException
    {
        _type = type;
	if (_type != ChecksumType.MD5_TYPE && _type != ChecksumType.ADLER32) {
            // we know we support the above too; check the rest
            MessageDigest.getInstance(_type.getName());
        }
    }

    @Override
    public ChecksumType getType()
    {
        return _type;
    }

    @Override
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

    @Override
    public Checksum create(byte[] digest)
    {
	return new Checksum(_type, digest);
    }

    @Override
    public Checksum create(String digest)
    {
        return new Checksum(_type, digest);
    }

    @Override
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
    public Checksum computeChecksum(RepositoryChannel channel) throws IOException,
        InterruptedException
    {
        return computeChecksum(channel, Double.POSITIVE_INFINITY);
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

    @Override
    public Checksum computeChecksum(RepositoryChannel channel, double throughputLimit)
        throws IOException, InterruptedException
    {
        long start = System.currentTimeMillis();
        MessageDigest digest = create();
        long pos = 0L;
        ByteBuffer buffer = ByteBuffer.allocate(KiB.toBytes(64));

        int rc;
        while ((rc = channel.read(buffer, pos)) > 0) {
            pos += rc;
            buffer.flip();
            digest.update(buffer);
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

        Checksum checksum = create(digest.digest());

        _log.debug("Computed checksum, length {}, checksum {} in {} ms{}", pos, checksum,
                   System.currentTimeMillis() - start, pos == 0 ? ""
                            : ", throughput " +
                              throughputAsString(pos, System.currentTimeMillis() - start) +
                              " MiB/s" +
                              (Double.isInfinite(throughputLimit)
                               ? ""
                               : " (limit " + BYTES.toMiB(throughputLimit) + " MiB/s)"));
        return checksum;
    }
}
