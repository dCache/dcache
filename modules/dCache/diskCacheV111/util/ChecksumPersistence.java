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

import  dmg.cells.nucleus.CellEndpoint;
import  dmg.cells.nucleus.CellPath;
import  dmg.cells.nucleus.CellMessage;
import  dmg.cells.nucleus.NoRouteToCellException;
import  diskCacheV111.vehicles.PnfsGetChecksumMessage;
import  diskCacheV111.vehicles.PnfsGetChecksumAllMessage;
import  diskCacheV111.vehicles.PnfsSetChecksumMessage;

import  java.security.MessageDigest ;
import  java.security.NoSuchAlgorithmException ;

import java.io.RandomAccessFile;

import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

public abstract class ChecksumPersistence {
  public abstract void store(CellEndpoint endpoint,PnfsId pnfsId, Checksum value) throws Exception;
  public abstract String retrieve(CellEndpoint endpoint, PnfsId pnfsId,ChecksumType type) throws Exception ;
  public abstract int[] listChecksumTypes(CellEndpoint endpoint, PnfsId pnfsId) throws Exception;

  public static ChecksumPersistence getPersistenceMgr(){
    return new ChecksumPersistencePnfsImpl();
  }
}

class ChecksumPersistencePnfsImpl extends ChecksumPersistence {

  public void store(CellEndpoint endpoint, PnfsId pnfsId, Checksum value) throws Exception {
         PnfsSetChecksumMessage flag =
             new PnfsSetChecksumMessage(pnfsId, value.getType().getType(), value.getValue() ) ;
         flag.setReplyRequired(true);

         endpoint.sendAndWait(new CellMessage(new CellPath("PnfsManager"),
                                              flag), 60000L);
  }

  public String retrieve(CellEndpoint endpoint, PnfsId pnfsId, ChecksumType type) throws Exception
  {
            PnfsGetChecksumMessage flag =
                new PnfsGetChecksumMessage(pnfsId,type.getType()) ;
            flag.setReplyRequired(true) ;
            CellMessage msg = new CellMessage( new CellPath("PnfsManager") , flag ) ;
            msg = endpoint.sendAndWait( msg , 60000L );
            if( msg == null )return null ;

            Object obj = msg.getMessageObject() ;
            if( obj instanceof PnfsGetChecksumMessage){
                PnfsGetChecksumMessage flags = (PnfsGetChecksumMessage)obj ;
                if( flags.getValue() == null ){
                    return null ;
                }
                return flags.getValue() ; // assume this is the right type
            }

            throw new Exception("Got message of unrecognized type. Expected PnfsGetChecksumMessage, but got: " + obj);
  }

  public int[] listChecksumTypes(CellEndpoint endpoint, PnfsId pnfsId) throws Exception
  {
           PnfsGetChecksumAllMessage flag =
                new PnfsGetChecksumAllMessage(pnfsId) ;
            flag.setReplyRequired(true) ;
            CellMessage msg = new CellMessage( new CellPath("PnfsManager") , flag ) ;
            msg = endpoint.sendAndWait( msg , 60000L );
            if( msg == null )return null ;

            Object obj = msg.getMessageObject() ;
            if( obj instanceof PnfsGetChecksumAllMessage){
                PnfsGetChecksumAllMessage flags = (PnfsGetChecksumAllMessage)obj ;
                if( flags.getValue() == null ){
                    return new int[0];
                }
                return flags.getValue() ; // assume this is the right type
            }

            throw new Exception("Got message of unrecognized type. Expected PnfsGetChecksumAllMessage, but got: " + obj);
  }
}
