/*
 * SelectPoolCompanion.java
 *
 * Created on August 10, 2006, 3:18 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessageAnswerable;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMgrGetPoolByLink;
import diskCacheV111.vehicles.EnstoreStorageInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class SelectPoolCompanion implements CellMessageAnswerable{

    private static final Logger _log =
        LoggerFactory.getLogger(SelectPoolCompanion.class);

    private static final int ASKING_FOR_POOL = 2;
    private static final int RECEIVED_POOL = 3;
    private Manager manager;
    CellMessage selectPoolMsg;
    PoolMgrSelectPoolMsg selectPool;
    String poolManager;
    private int state =ASKING_FOR_POOL;
    private String linkName;
    /** Creates a new instance of SelectPoolCompanion */
    public SelectPoolCompanion(Manager manager,String linkName,CellMessage selectPoolMsg) {
        this.manager = manager;
        this.selectPoolMsg = selectPoolMsg;
        this.selectPool = (PoolMgrSelectPoolMsg)selectPoolMsg.getMessageObject();
        this.poolManager = manager.getPoolManager();
        this.linkName = linkName;
    }

    public static final String getStateString(int state) {
        switch(state) {
            case ASKING_FOR_POOL:
                return "ASKING_FOR_POOL";
            case RECEIVED_POOL:
                return "RECEIVED_POOL";
           default:
                return "UNKNOWN";
        }
    }

    private void returnFailure(Object errorObject) {
        _log.warn("retirning failure: ");
        if(errorObject != null) {
            if(errorObject instanceof Throwable  ) {
                _log.warn(errorObject.toString(), (Throwable) errorObject);
            }
            else {
                _log.warn(errorObject.toString());
            }
        }
        selectPool.setReply(1,errorObject);
        selectPoolMsg.revertDirection();
        try{
            manager.sendMessage(selectPoolMsg);
        }
        catch(Exception e) {
            _log.warn(e.toString(), e);
        }
    }

    private void returnFailure(String error) {
        _log.warn("returning failure: "+error);

        selectPool.setReply(1,error);
        selectPoolMsg.revertDirection();
        try{
            manager.sendMessage(selectPoolMsg);
        }
        catch(Exception e) {
            _log.warn(e.toString(), e);
        }
    }

    public void exceptionArrived(CellMessage request, Exception exception) {
        returnFailure(exception);
    }

    public void answerTimedOut(CellMessage request) {
        selectPool.setReply(1,"timeout in SelectPoolCompanion");
        selectPoolMsg.revertDirection();
        try{
            manager.sendMessage(selectPoolMsg);
        }
        catch(Exception e) {
            _log.warn(e.toString(), e);
        }
    }

    public void answerArrived(CellMessage request, CellMessage answer) {
         int current_state = state;
        _log.info("answerArrived, state="+getStateString(current_state));
        Object o = answer.getMessageObject();
        if(o instanceof Message) {
            Message message = (Message)answer.getMessageObject() ;
            if( message instanceof PoolMgrGetPoolByLink  &&
                current_state == ASKING_FOR_POOL) {
                state=RECEIVED_POOL;
                PoolMgrGetPoolByLink get_pool_bylink_msg =
                (PoolMgrGetPoolByLink)message;
                poolArrived(get_pool_bylink_msg);
                return;
            } else {
                _log.warn("ignoring unexpected message : "+message);
                //callbacks.ReserveSpaceFailed("unexpected message arrived:"+message);
                return ;
            }
        }
        else {
            _log.warn(" got unknown object. ignoring "+
            " : "+o);
        }

    }

    private void poolArrived(PoolMgrGetPoolByLink get_pool_bylink_msg) {
        if(get_pool_bylink_msg.getPoolName() == null ||
                get_pool_bylink_msg.getReturnCode() != 0) {
            _log.warn("pool manager could not get pool name");
            if(get_pool_bylink_msg.getErrorObject() != null  ) {
               _log.warn(get_pool_bylink_msg.getErrorObject().toString());
                returnFailure(get_pool_bylink_msg.getErrorObject() );
            } else {
                returnFailure("pool manager could not get pool name, no more info available" );
            }
            return;
        }
        selectPool.setPoolName(get_pool_bylink_msg.getPoolName());
        selectPool.setReply();
        selectPoolMsg.revertDirection();
        try{
            manager.sendMessage(selectPoolMsg);
        }
        catch(Exception e) {
            _log.warn(e.toString(), e);
        }

    }

    private void askForPoolByLink() {
        _log.info("askForPoolByLink, linkName="+linkName);
        try {
            PoolMgrGetPoolByLink getPoolByLink = new
                    PoolMgrGetPoolByLink(linkName);
            getPoolByLink.setFileSize(selectPool.getFileSize());
            state = ASKING_FOR_POOL;
            manager.sendMessage(new CellMessage(new CellPath(poolManager),getPoolByLink),
                this,1000*60*3);

        } catch ( Exception e) {
            returnFailure(e);
        }
        //String space = manager.getSpace(spaceId);

    }

    public static final void selectPool(Manager manager,String linkName,CellMessage selectPoolMsg) throws Exception {
        SelectPoolCompanion companion = new SelectPoolCompanion(manager,linkName,selectPoolMsg);
        companion.askForPoolByLink();

    }

}
