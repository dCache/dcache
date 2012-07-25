// $Id: PoolSelectionUnitV2wgap.java,v 1.0 2008-08-05 14:03:54 catalind Exp $
package diskCacheV111.poolManager;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import java.io.NotSerializableException;
import java.io.PrintWriter ;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.util.Args;
import dmg.util.CommandSyntaxException;

import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.pools.PoolV2Mode;


import dmg.cells.nucleus.* ;
import dmg.util.* ;
import diskCacheV111.vehicles.* ;
import diskCacheV111.pools.* ;
import diskCacheV111.pools.PoolCostInfo.NamedPoolQueueInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolSelectionUnitV2wgap extends PoolSelectionUnitV2 {

    private static final Logger _log =
        LoggerFactory.getLogger(PoolSelectionUnitV2wgap.class);

    private static final String __version = "$Id: PoolSelectionUnitV2wgap.java,v 1.0 2008-08-05 14:03:54 catalind Exp $";
    private static final Logger _logPoolSelection = LoggerFactory.getLogger("logger.org.dcache.poolselection."+PoolSelectionUnitV2wgap.class.getName());

    private static final long _TO_GetFreeSpace         = 2*60 * 1000L;
    private static final long _TO_GetGapSpace          = 3600 * 1000L;


    private  static CostModulePoolInfoTable _costTable;
    private  static Object _costTableLock = new Object();
    private static CellAdapter _cell4SelectionWithGap;


    public PoolSelectionUnitV2wgap() {
	  super ();

	  // this creates a new entity
          _cell4SelectionWithGap = new CellAdapter ("SelectorWithGap", "", false);
    }


    @Override
    public PoolPreferenceLevel[] match(DirectionType type, String netUnitName, String protocolUnitName,
            StorageInfo storageInfo, String linkGroupName) {

          PoolPreferenceLevel[] result =
              super.match (type,  netUnitName,
			   protocolUnitName, storageInfo, linkGroupName);

          if (type == DirectionType.WRITE) {

            synchronized (_costTableLock) {
	       try {
                  getCostTable(_cell4SelectionWithGap);
               } catch (Exception e) {
                  // throw new IllegalArgumentException( "CostTable is not defined (null pointer)");
		  // don't throw anything - do default
		  _log.debug("failed getting a cost table");
               }

               if ( _costTable != null ) {
	          for( int prio = 0 ; prio < result.length ; prio++ ) {
	              List<String> resultList = new ArrayList<String>();
                      List<String> poolList = result[prio].getPoolList() ;

	             for (String poolName : poolList) {
			 if (_costTable.getPoolCostInfoByName(poolName) != null) {
		            PoolCostInfo.PoolSpaceInfo plSpace = _costTable.getPoolCostInfoByName(poolName).getSpaceInfo ();

			    _log.debug(poolName + "> checking: " + plSpace.getGap () + " " + plSpace.getFreeSpace ());
	                    if (plSpace.getGap () < plSpace.getFreeSpace ()) {
			      _log.debug(poolName + "> included on level " + prio);
	                      resultList.add (poolName);
	                    }
                         } else {
			   _log.debug("missing data for " + poolName);
                         }
	             }
		     // the result can be even empty
                     result[prio] = new PoolPreferenceLevel(resultList, result[prio].getTag());
	          }
               } else {
                  throw new IllegalArgumentException( "CostTable is not defined (null pointer)");
               }
            }

          }
          return result;
    }

   private void getCostTable(CellAdapter cell)
           throws InterruptedException,
           NoRouteToCellException
   {

       synchronized (_costTableLock) {

	   /*
            * The gap parameter is not supossed to change so often
            *    - this check was changed to 1 hours = 3600 * 1000
            */
           if (_costTable == null ||
               System.currentTimeMillis() > _costTable.getTimestamp() + _TO_GetGapSpace) {

               String command = new String("xcm ls");

               CellMessage cellMessage = new CellMessage(
                       new CellPath("PoolManager"), command);
               CellMessage reply = null;

               _log.debug("gtCstTble(): sendMessage, " + " command=[" + command +
                    "]\n" + "message=" + cellMessage);

               reply = cell.sendAndWait(cellMessage, _TO_GetFreeSpace);

               _log.debug("DEBUG: Cst tble reply arrived");

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



