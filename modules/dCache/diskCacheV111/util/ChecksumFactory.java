/*
 * @(#)Checksum.java	1.2 03/11/10
 *
 * Copyright 1996-2006 dcache.org All Rights Reserved.
 *
 * This software is the proprietary information of dCache.org
 * Use is subject to license terms.
 */

package diskCacheV111.util;

/**
 *
 * @author  Andrii Baranovski
 * @version 1.2, 03/11/10
 * @see     Preferences
 * @since   1.4
*/

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

public abstract class ChecksumFactory {

    private final static Logger _log = LoggerFactory.getLogger(ChecksumFactory.class);

    protected static String[] _types = { "ADLER32","MD5","MD4" };

    protected String _stringType;

    public String getType(){ return _stringType; }

    public abstract Checksum  create();
    public abstract Checksum  create(byte [] digest);
    public abstract Checksum  create(String stringDigest);

    // these 2 methods should be p_impled for the storeChecksum
    public abstract Checksum  createFromPersistentState( CellEndpoint endpoint, PnfsId pnfsId );

    public static ChecksumFactory getFactory(String type)  throws NoSuchAlgorithmException {
	return new GenericIdChecksumFactory(type.toUpperCase());
    }

    public static final String [] getTypes() {
       return _types;
    }

    public static final String [] getTypes( CellEndpoint endpoint,PnfsId pnfsId ){
       try {
           int []intTypes = ChecksumPersistence.getPersistenceMgr().listChecksumTypes(endpoint,pnfsId);

           if ( intTypes == null )
              return null;

           Vector<String> stringTypes = new Vector<String>(1);
           for ( int i = 0; i < intTypes.length; ++i){
              if ( intTypes[i] > 0 && intTypes[i] <= _types.length )
                stringTypes.add(_types[intTypes[i]-1]);
           }
           if ( stringTypes.size() > 0 ){
              return stringTypes.toArray(new String[1]);
           }

       } catch ( Exception ex){ _log.error(ex.toString()); }

       return null;
    }

    public static int mapStringTypeToId(String type) throws NoSuchAlgorithmException{

      int intType = 1;

      for ( int i = 0; i < _types.length; ++i, ++intType)
        if ( _types[i].equals(type) )
           return intType;

      throw new NoSuchAlgorithmException("type "+type+" is not supported");
   }

   public static String mapIdTypeToString(int type) throws NoSuchAlgorithmException{

      if ( type > getTypes().length || type < 1 )
          throw new NoSuchAlgorithmException("type "+Integer.toString(type)+" is not supported");

      return getTypes()[type - 1];
   }

    public static void main( String [] args ) throws Exception {
       System.out.println("Getting MD4 first time");
       ChecksumFactory.getFactory("MD4");
       System.out.println("Getting MD4 second time");
       ChecksumFactory.getFactory("MD4");
    }
}

class GenericIdChecksumFactory extends ChecksumFactory
{
    private final static Logger _log =
        LoggerFactory.getLogger(GenericIdChecksumFactory.class);

    private int _type;

    public GenericIdChecksumFactory(String type) throws NoSuchAlgorithmException {
        _type = mapStringTypeToId(type);
        _stringType = type;

	if ( _type != Checksum.MD5 && _type != Checksum.ADLER32 ) {  // we know we support these two
            MessageDigest.getInstance(type);
        }
    }

    public Checksum  create() {
	try {
            if ( _type == Checksum.ADLER32 )
              return new Checksum( new Adler32() );

            MessageDigest md = (MessageDigest)MessageDigest.getInstance(_stringType);

	    return new Checksum(md);
	} catch ( NoSuchAlgorithmException ex){
	}
	assert(false);
	return null;
    }

    public Checksum  create(byte [] digest){
	return new Checksum(_type,digest);
    }
    public Checksum create(String stringDigest){
        return new Checksum(Integer.toString(_type)+":"+stringDigest);
    }

    public Checksum  createFromPersistentState( CellEndpoint endpoint,  PnfsId pnfsId )
    {

        try {
           String checksumValue = ChecksumPersistence.getPersistenceMgr().retrieve(endpoint,pnfsId,_type);
           if ( checksumValue != null )
              return create(checksumValue);
        } catch ( Exception e){
          _log.error(e.toString());
        }

/*
	try{
	    PnfsFlagMessage flag =
		new PnfsFlagMessage(pnfsId,"c","get") ;
	    flag.setReplyRequired(true) ;
	    CellMessage msg = new CellMessage( new CellPath("PnfsManager") , flag ) ;
	    if( ( msg = cell.sendAndWait( msg , 60000L ) ) == null )return null ;

	    Object obj = msg.getMessageObject() ;
	    if( obj instanceof PnfsFlagMessage ){
		PnfsFlagMessage flags = (PnfsFlagMessage)obj ;
		if( flags.getValue() == null ){
		    cell.esay("getChecksumFromPnfs : No crc available for "+pnfsId);
		    return null ;
		}
		return new Checksum( flags.getValue().toString() ) ; // assume this is the right type
	    }
	}catch(Exception ee ){
	    ee.printStackTrace();
	}
*/
	return null ;
    }

}
