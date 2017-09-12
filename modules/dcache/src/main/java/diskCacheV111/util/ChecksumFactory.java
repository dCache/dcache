package diskCacheV111.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;


public abstract class ChecksumFactory
{
    public abstract ChecksumType getType();
    public abstract MessageDigest create();
    public abstract Checksum create(byte [] digest);
    public abstract Checksum create(String stringDigest);
    public abstract Checksum find(Set<Checksum> checksums);

    public static ChecksumFactory getFactory(ChecksumType type)
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
            return getFactory(checksum.getType());
        }
        return getFactory(defaultType);
    }

    public static ChecksumFactory getFactoryFor(Checksum checksum)
    {
        return getFactory(checksum.getType());
    }
}

class GenericIdChecksumFactory extends ChecksumFactory
{
    private static final Logger _log =
        LoggerFactory.getLogger(GenericIdChecksumFactory.class);

    private final ChecksumType _type;

    public GenericIdChecksumFactory(ChecksumType type)
    {
        _type = type;
	if (_type != ChecksumType.MD5_TYPE && _type != ChecksumType.ADLER32) {
            try {
                // we know we support the above too; check the rest
                MessageDigest.getInstance(_type.getName());
            } catch (NoSuchAlgorithmException e) {
                // If this happens, it's actually a bug in our software.  It is
                // not legitimate for us not to support a ChecksumType.
                throw new RuntimeException("Inconsistent checksum support: {}" + e.getMessage());
            }
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
}
