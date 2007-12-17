/*
 * @(#)Checksum.java    1.2 03/11/10
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
import  java.io.NotSerializableException;

import  dmg.cells.nucleus.CellAdapter;
import  dmg.cells.nucleus.CellPath;
import  dmg.cells.nucleus.CellMessage;
import  dmg.cells.nucleus.NoRouteToCellException;
import  diskCacheV111.vehicles.PnfsGetChecksumMessage;
import  diskCacheV111.vehicles.PnfsGetChecksumAllMessage;
import  diskCacheV111.vehicles.PnfsSetChecksumMessage;

import  java.security.MessageDigest ;
import  java.security.NoSuchAlgorithmException ;

import java.io.RandomAccessFile;

public abstract class ChecksumPersistence {
  public abstract void store(CellAdapter cell,PnfsId pnfsId, diskCacheV111.util.Checksum value,boolean overwrite) throws Exception;
  public abstract String retrieve(CellAdapter cell, PnfsId pnfsId,int type) throws Exception ;
  public abstract int[] listChecksumTypes(CellAdapter cell, PnfsId pnfsId) throws Exception;

  public static ChecksumPersistence getPersistenceMgr(){
    return new ChecksumPersistencePnfsImpl();
  }
}

// not really exception safe 
class ChecksumPersistenceImpl extends ChecksumPersistence {

  private static final String basePath = System.getProperty("CHECKSUM_DB","/tmp");

  private String getdbFileName(PnfsId pnfsId,int type){
     return basePath+"/"+pnfsId.toString()+"_"+Integer.toString(type);
  }

  public void store(CellAdapter cell, PnfsId pnfsId, diskCacheV111.util.Checksum value,boolean overwrite) throws Exception {

    RandomAccessFile raf = new RandomAccessFile(getdbFileName(pnfsId,value.getType()),"rw");
    raf.write(value.toHexString().getBytes());
    raf.close();

  }

  public String retrieve(CellAdapter cell, PnfsId pnfsId,int type) throws Exception
  {
     String fileNamePath = getdbFileName(pnfsId,type);
     RandomAccessFile raf = new RandomAccessFile(fileNamePath,"r");

     long numRead = raf.length();

     byte [] digest = new byte[(int)numRead];

   
     numRead = raf.read(digest);

     raf.close();

     if ( numRead <= 0 )
        throw new Exception("Checksum value for "+fileNamePath);

     cell.say("Read "+(int)numRead+" bytes");

     return new String(digest);
  }
  public int[] listChecksumTypes(CellAdapter cell, PnfsId pnfsId) throws Exception { return null; }
}

class ChecksumPersistencePnfsImpl extends ChecksumPersistence {

  public void store(CellAdapter cell, PnfsId pnfsId, diskCacheV111.util.Checksum value,boolean overwrite) throws Exception {
         PnfsSetChecksumMessage flag =
            new PnfsSetChecksumMessage(pnfsId, value.getType(), value.toHexString() ) ;
         flag.setReplyRequired(false) ;

         cell.sendMessage(
            new CellMessage(
               new CellPath("PnfsManager") ,
               flag
            )
         );
  }

  public String retrieve(CellAdapter cell, PnfsId pnfsId,int type) throws Exception
  {
            PnfsGetChecksumMessage flag =
                new PnfsGetChecksumMessage(pnfsId,type) ;
            flag.setReplyRequired(true) ;
            CellMessage msg = new CellMessage( new CellPath("PnfsManager") , flag ) ;
            if( ( msg = cell.sendAndWait( msg , 60000L ) ) == null )return null ;

            Object obj = msg.getMessageObject() ;
            if( obj instanceof PnfsGetChecksumMessage){
                PnfsGetChecksumMessage flags = (PnfsGetChecksumMessage)obj ;
                if( flags.getValue() == null ){
                    cell.esay("getChecksumFromPnfs : No crc available for "+pnfsId);
                    return null ;
                }
                return flags.getValue() ; // assume this is the right type
            } 

            throw new Exception("Got message of unrecognized type. Expected PnfsGetChecksumMessage");
  }

  public int[] listChecksumTypes(CellAdapter cell, PnfsId pnfsId) throws Exception
  {
           PnfsGetChecksumAllMessage flag =
                new PnfsGetChecksumAllMessage(pnfsId) ;
            flag.setReplyRequired(true) ;
            CellMessage msg = new CellMessage( new CellPath("PnfsManager") , flag ) ;
            if( ( msg = cell.sendAndWait( msg , 60000L ) ) == null )return null ;

            Object obj = msg.getMessageObject() ;
            if( obj instanceof PnfsGetChecksumAllMessage){
                PnfsGetChecksumAllMessage flags = (PnfsGetChecksumAllMessage)obj ;
                if( flags.getValue() == null ){
                    cell.esay("getChecksumFromPnfs : No checksum available for "+pnfsId);
                    return null ;
                }
                return flags.getValue() ; // assume this is the right type
            }

            throw new Exception("Got message of unrecognized type. Expected PnfsGetChecksumAllMessage");
  }
}
