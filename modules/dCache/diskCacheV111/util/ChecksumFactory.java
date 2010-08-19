package diskCacheV111.util;

import java.util.*;

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

    public Checksum computeChecksum(File file)
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
            }
        } finally {
            in.close();
        }
        Checksum checksum = create(digest.digest());
        _log.debug("Computed checksum for {}, length {}, checksum {} in {} ms",
                   new Object[] { file, sum, checksum, System.currentTimeMillis() - start });
        return checksum;
    }
}
