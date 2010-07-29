package diskCacheV111.util;

import  java.util.*;

import  dmg.cells.nucleus.CellEndpoint;
import  dmg.cells.nucleus.CellPath;
import  dmg.cells.nucleus.CellMessage;
import  dmg.cells.nucleus.NoRouteToCellException;
import  diskCacheV111.vehicles.PnfsFlagMessage;

import  java.security.MessageDigest ;
import  java.security.NoSuchAlgorithmException ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

public abstract class ChecksumFactory {

    private final static Logger _log = LoggerFactory.getLogger(ChecksumFactory.class);

    public abstract ChecksumType getType();
    public abstract MessageDigest create();
    public abstract Checksum  create(byte [] digest);
    public abstract Checksum  create(String stringDigest);

    // these 2 methods should be p_impled for the storeChecksum
    public abstract Checksum  createFromPersistentState( CellEndpoint endpoint, PnfsId pnfsId );

    public static ChecksumFactory getFactory(ChecksumType type)  throws NoSuchAlgorithmException {
	return new GenericIdChecksumFactory(type);
    }

    public static final String [] getTypes( CellEndpoint endpoint,PnfsId pnfsId ){
       try {
           int []intTypes = ChecksumPersistence.getPersistenceMgr().listChecksumTypes(endpoint,pnfsId);

           if ( intTypes == null )
              return null;

           Vector<String> stringTypes = new Vector<String>(1);
           for ( int i = 0; i < intTypes.length; ++i){
               try {
                   stringTypes.add(ChecksumType.getChecksumType(intTypes[i]).getName());
               } catch (IllegalArgumentException e) {
                   _log.warn("Unknown checksum type: " + intTypes[i]);
               }
           }
           if ( stringTypes.size() > 0 ){
              return stringTypes.toArray(new String[0]);
           }

       } catch ( Exception ex){ _log.error(ex.toString()); }

       return null;
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

    public Checksum createFromPersistentState(CellEndpoint endpoint, PnfsId pnfsId)
    {
        try {
           String checksumValue =
               ChecksumPersistence.getPersistenceMgr().retrieve(endpoint,pnfsId,_type);
           if ( checksumValue != null )
              return create(checksumValue);
        } catch ( Exception e){
          _log.error(e.toString());
        }
	return null ;
    }
}
