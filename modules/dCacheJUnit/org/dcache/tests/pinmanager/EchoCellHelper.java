// $Id: EchoCell.java,v 1.42 2007-10-25 15:02:43 timur Exp $
//

/*
 * EchoCell.java
 *
 * Created on March 05, 2008, 12:54 PM
 */

package org.dcache.tests.pinmanager;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.CellVersion;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PinManagerMessage;
import diskCacheV111.vehicles.PinManagerPinMessage;
import diskCacheV111.vehicles.PinManagerUnpinMessage;
import diskCacheV111.vehicles.PinManagerExtendLifetimeMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.PnfsId;
import java.util.Set;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.dcache.cells.Option;
import org.dcache.cells.AbstractCell;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;


/**
 *   <pre>
 *   This cell performs "pinning and unpining service on behalf of other
 *   services cenralized pin management supports:
 *    pining/unpinning of the same resources by multiple requestors,
 *     synchronization of pinning and unpinning
 *     lifetimes for pins
 *
 * PINNING
 * 1) when pin for a file exists and another request arrives
 *  no action is taken, the database pinrequest record is created
 * 2) if pin does not exist new pinrequest record is created
 *       Pnfs flag is not set anymore
 *       the file is staged if nessesary and pinned in a read pool
 *       with EchoCell, as an owner, and lifetime
 *
 *
 * UNPINNING
 *  1)if pin request expires / canseled and other pin requests
 *  for the same file exist, no action is taken, other then removal
 *  of the database  pin request record
 *  2) if last pin request is removed then the file is unpinned
 * which means sending of the "set sticky to false message" is send to all
 * locations,
 * the pnfs flag is removed
 * database  pin request record is removed
 *
 *
 * @author timur
 */
public class EchoCellHelper extends AbstractCell  {

    /*
     * Creates a new instance of EchoCell
     */
    public EchoCellHelper(String name , String argString) throws Exception {
        super(name, argString);

        doInit();
    }


    public void stop() {
        kill();
    }




    public void messageArrived( final CellMessage cellMessage ) {
        Object o = cellMessage.getMessageObject();
        if(!(o instanceof Message )) {
            super.messageArrived(cellMessage);
            return;
        }
        Message message = (Message)o ;
        try {
            info("processMessage: Message  arrived:"+o.getClass().getName()+" : "+o +" from "+
                   cellMessage.getSourcePath());
            if(message instanceof PnfsGetStorageInfoMessage) {
                PnfsGetStorageInfoMessage pnfsGetStorageInfo =
                    (PnfsGetStorageInfoMessage) message;
                pnfsGetStorageInfo.setStorageInfo(new GenericStorageInfo());

            } else if (message instanceof PnfsGetCacheLocationsMessage) {
                PnfsGetCacheLocationsMessage pnfsGetCacheLocations =
                    (PnfsGetCacheLocationsMessage) message;
                List locations = new ArrayList(1);
                locations.add(getCellName());
                pnfsGetCacheLocations.setCacheLocations(locations);
            } else if (message instanceof PoolMgrSelectReadPoolMsg) {
                PoolMgrSelectReadPoolMsg selectReadPool =
                    (PoolMgrSelectReadPoolMsg) message;
                selectReadPool.setPoolName(getCellName());

            }
            else {
                super.messageArrived(cellMessage);
            }
            // return;
        } catch(Throwable t) {
            error(t);
            message.setFailed(-1,t);
        }
         if(  message.getReplyRequired()  ) {
            try {
                message.setReply();
                info("Reverting derection "+cellMessage);
                cellMessage.revertDirection();
                info("Sending reply "+cellMessage);
                sendMessage(cellMessage);
            } catch (Exception e) {
                error("Can't reply message : "+e);
            }
        } else {
            info("reply is not required, finished processing");
        }
   }


    public void exceptionArrived(ExceptionEvent ee) {
        error("Exception Arrived: "+ee);
        error(ee.getException().toString());
        super.exceptionArrived(ee);
    }





}
