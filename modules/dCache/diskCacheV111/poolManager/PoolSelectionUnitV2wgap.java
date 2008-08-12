// $Id: PoolSelectionUnitV2wgap.java,v 1.0 2008-08-05 14:03:54 catalind Exp $
package diskCacheV111.poolManager;

import java.net.*; 
import java.io.* ;
import java.util.*;
import diskCacheV111.repository.CacheRepositoryEntryInfo;
import java.util.concurrent.atomic.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.concurrent.locks.*; 
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import dmg.util.Args;
import dmg.util.CommandSyntaxException;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.pools.PoolCostInfo; 


public class PoolSelectionUnitV2wgap extends PoolSelectionUnitV2 {

    private static final String __version = "$Id: PoolSelectionUnitV2wgap.java,v 1.0 2008-08-05 14:03:54 catalind Exp $";
    private static final Logger _logPoolSelection = Logger.getLogger("logger.org.dcache.poolselection."+PoolSelectionUnitV2wgap.class.getName());

    private static final long _timeout                 = 2 * 60 * 1000L ;
    private static final long _TO_GetPoolRepository    = 2 * 60 * 1000L;
    private static final long _TO_GetPoolTags          = 2 * 60 * 1000L;
    private static final long _TO_MessageQueueUpdate   =     15 * 1000L; // 15 seconds
    private static final long _TO_GetStorageInfo       = _timeout;
    private static final long _TO_GetCacheLocationList = _timeout;
    private static final long _TO_GetPoolGroup         = 2*60 * 1000L;
    private static final long _TO_GetPoolList          = 2*60 * 1000L;
    private static final long _TO_GetFreeSpace         = 2*60 * 1000L;
    private static final long _TO_SendObject           = 2*60 * 1000L;

    private  static CostModulePoolInfoTable _costTable = null;
    private  static Object _costTableLock = new Object();
    private static CellAdapter _cell4SelectionWithGap = null; 


    public PoolSelectionUnitV2wgap() {
	  super (); 
	  
	  // this creates a new entity 
          _cell4SelectionWithGap = new CellAdapter ("SelectorWithGap", "", false); 
    }


    public PoolPreferenceLevel[] match(DirectionType type, String storeUnitName,
            String dCacheUnitName, String netUnitName, String protocolUnitName,
            StorageInfo storageInfo, String linkGroupName) {

          PoolPreferenceLevel[] result = 
              super.match (type, storeUnitName, dCacheUnitName, netUnitName, 
			   protocolUnitName, storageInfo, linkGroupName); 

          synchronized (_costTableLock) { 
	      try { 
                  getCostTable(_cell4SelectionWithGap);
              } catch (Exception e) { 
                  // throw new IllegalArgumentException( "CostTable is not defined (null pointer)"); 
		  // don't throw anything - do default 
		  dsay (_cell4SelectionWithGap, "failed getting a cost table"); 
                  //System.out.println ("failed getting a cost table"); 
              } 

              if ( _costTable != null ) {
	          for( int prio = 0 ; prio < result.length ; prio++ ) { 
	              List<String> resultList = new ArrayList<String>(); 
                      List<String> poolList = result[prio].getPoolList() ;
	      
	             for (String poolName : poolList) { 
			 if (_costTable.getPoolCostInfoByName(poolName) != null) { 
		            PoolCostInfo.PoolSpaceInfo plSpace = _costTable.getPoolCostInfoByName(poolName).getSpaceInfo (); 
		  
			    dsay (_cell4SelectionWithGap, poolName + "> checking: " + plSpace.getGap () + " " + plSpace.getFreeSpace ()); 
			    //System.out.println (poolName + "> checking: " + plSpace.getGap () + " " + plSpace.getFreeSpace ());
	                    if (plSpace.getGap () < plSpace.getFreeSpace ()) { 
			      dsay (_cell4SelectionWithGap, poolName + "> included on level " + prio); 
			      //System.out.println (poolName + "> included on level " + prio);
	                      resultList.add (poolName); 
	                    } 
                         } else { 
			   dsay (_cell4SelectionWithGap, "missing data for " + poolName);
                           //System.out.println ("missing data for " + poolName);
                         } 
	             } 
		     // We don't want to return empty lists
                     result[prio] = new PoolPreferenceLevel(resultList, result[prio].getTag());
                     if (resultList.size () < 1) {
			 dsay (_cell4SelectionWithGap, "list empty, returning original"); 
                         //System.out.println ("list empty, returning original"); 
		     } 
	          } 
              } else { 
                  throw new IllegalArgumentException( "CostTable is not defined (null pointer)");
              }
          } 
          return result;
    }

   protected void dsay( CellAdapter cell, String s ){
      cell.say("DEBUG: " +s) ;
   }

   private void getCostTable(CellAdapter cell)
           throws InterruptedException,
           NoRouteToCellException,
           NotSerializableException {

       synchronized (_costTableLock) {

           if (_costTable == null ||
               System.currentTimeMillis() > _costTable.getTimestamp() + 240 * 1000) {

               String command = new String("xcm ls");

               CellMessage cellMessage = new CellMessage(
                       new CellPath("PoolManager"), command);
               CellMessage reply = null;

               dsay(cell, "gtCstTble(): sendMessage, " + " command=[" + command +
                    "]\n" + "message=" + cellMessage);

               reply = cell.sendAndWait(cellMessage, _TO_GetFreeSpace);

               dsay(cell, "DEBUG: Cst tble reply arrived");

               if (reply == null ||
                   !(reply.getMessageObject() instanceof CostModulePoolInfoTable)) {

                   throw new IllegalArgumentException(
                           "received null pinter or wrong object type from PoolManager in getCostTable");
               }

               Object obj = reply.getMessageObject();
               if ( obj == null ) {
                   throw new IllegalArgumentException(
                           "received null pinter from getCostTable from PoolManager");
               } else {
                   _costTable = (CostModulePoolInfoTable) obj;
               }
           }
       }
   } 

} 



