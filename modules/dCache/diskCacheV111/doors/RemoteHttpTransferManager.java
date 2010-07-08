/*
 * RemoteHttpTransferManager.java
 *
 * Created on June 4, 2003, 10:31 AM
 */

package diskCacheV111.doors;

import dmg.cells.nucleus.*;
import dmg.cells.network.*;
import dmg.util.*;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsFile;

import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolSetStickyMessage;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.RemoteHttpTransferManagerMessage;

import java.io.PrintWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author  timur
 */
public class RemoteHttpTransferManager extends CellAdapter {

    private final static Logger _log =
        LoggerFactory.getLogger(RemoteHttpTransferManager.class);

    private HashMap longIdToMessageMap = new HashMap();
    private HashSet activeTransfersIDs = new HashSet();
    private HashMap activeTransfersIDsToHandlerMap = new HashMap();
    private CellPath _pnfsPath;
    private CellPath _poolMgrPath;
    private String[] _hosts;
    private int __poolManagerTimeout = 10;
    private int poolTimeout        = 5 * 60 ;
    private CellNucleus  _nucleus ;
    private Args _args;
    private int moverTimeout = 24*60*60;

    /** Creates a new instance of RemoteHttpTransferManager */
    public RemoteHttpTransferManager(String cellName, String argString) throws Exception
    {
        super(cellName,argString,false);
       _nucleus  = getNucleus() ;
        _args = new Args(argString);
       _pnfsPath = new CellPath ( "PnfsManager" );
        _poolMgrPath     = new CellPath ( "PoolManager" ) ;
        InetAddress[] addresses = InetAddress.getAllByName(
            InetAddress.getLocalHost().getHostName());
        _hosts = new String[addresses.length];
         for(int i = 0; i<addresses.length; ++i)
         {
             _hosts[i] = addresses[i].getHostName();
         }

         String tmpstr = _args.getOpt ("pool_manager_timeout");
         if(tmpstr != null)
         {
            __poolManagerTimeout =Integer.parseInt (tmpstr);
         }


         tmpstr = _args.getOpt ("pool_timeout");
         if(tmpstr != null)
         {
            poolTimeout =Integer.parseInt (tmpstr);
         }

         tmpstr = _args.getOpt ("mover_timeout");
         if(tmpstr != null)
         {
            moverTimeout =Integer.parseInt (tmpstr);
         }

         tmpstr = _args.getOpt ("max_transfers");
         if(tmpstr != null)
         {
            max_transfers =Integer.parseInt (tmpstr);
         }

         useInterpreter (true);
        getNucleus ().export ();
        start() ;


    }

    // transfers ls
    // queue ls
    // kill active
    // remove queued



    public String hh_set_max_transfers = "<#max transfers>" ;
    public String ac_set_max_transfers_$_1( Args args )throws CommandException
    {
       int max_transfs = Integer.parseInt(args.argv(0)) ;
       if(max_transfs <= 0)
       {
           return "Error, max transfers number should be greater then 0 ";
       }
       setMax_transfers(max_transfs);
       return "set maximum number of active transfers to "+max_transfs;
    }

    public String hh_set_mover_timeout = "<#seconds>" ;
    public String ac_set_mover_timeout_$_1( Args args )throws CommandException
    {
       int timeout = Integer.parseInt(args.argv(0)) ;
       if(timeout <= 0)
       {
           return "Error, mover timeout should be greater then 0 ";
       }
       moverTimeout = timeout;
       return "set mover timeout to "+timeout+ " seconds";
    }

    public String hh_set_pool_timeout = "<#seconds>" ;
    public String ac_set_pool_timeout_$_1( Args args )throws CommandException
    {
       int timeout = Integer.parseInt(args.argv(0)) ;
       if(timeout <= 0)
       {
           return "Error, pool timeout should be greater then 0 ";
       }
       poolTimeout = timeout;
       return "set pool timeout to "+timeout+ " seconds";
    }

    public String hh_set_pool_manager_timeout = "<#seconds>" ;
    public String ac_set_pool_manager_timeout_$_1( Args args )throws CommandException
    {
       int timeout = Integer.parseInt(args.argv(0)) ;
       if(timeout <= 0)
       {
           return "Error, pool manger timeout should be greater then 0 ";
       }
       __poolManagerTimeout = timeout;
       return "set pool manager timeout to "+timeout+ " seconds";
    }

    public String hh_ls = "[-l] [<#transferId>]" ;
    public String ac_ls_$_0_1( Args args )throws CommandException
    {
       boolean long_format = args.getOpt("l") != null;
       Long id = null;
       if(args.argc() >0)
       {
           id = Long.valueOf(args.argv(0));
       }
       if(id != null)
       {
           synchronized(activeTransfersIDs)
           {
               if(!activeTransfersIDs.contains(id))
               {
                   return "ID not found : "+id;
               }
               RemoteHttpTransferHandler transferHandler =
                (RemoteHttpTransferHandler)
                    activeTransfersIDsToHandlerMap.get(id);
               return " transfer id="+id+" : "+
                    transferHandler.toString(long_format);
           }
       }
       StringBuffer sb =  new StringBuffer();
       synchronized(activeTransfersIDs)
       {
           if(activeTransfersIDs.isEmpty())
           {
               return "No Active Transfers";
           }
           sb.append("  Active Transfers ");
           Iterator iter = activeTransfersIDs.iterator();
           while(iter.hasNext())
           {
               id = (Long) iter.next();
               RemoteHttpTransferHandler transferHandler =
                (RemoteHttpTransferHandler)
                    activeTransfersIDsToHandlerMap.get(id);
               sb.append("\n#").append(id);
               sb.append(" ").append( transferHandler.toString(long_format));
           }

       }
       return sb.toString();
    }

    public String hh_queue = "[-l]" ;
    public String ac_queue_$_0( Args args )throws CommandException
    {
       boolean long_format = args.getOpt("l") != null;
       synchronized(queue)
       {
           int size =queue.size();
           if( size == 0)
           {
               return "Queue is empty";
           }
           StringBuffer sb = new StringBuffer();
           for(int i=0;i<size;++i)
           {
              sb.append("\n#").append(i);
              CellMessage cellMessage = (CellMessage) queue.get(i);
              RemoteHttpTransferManagerMessage transfer_request =
                (RemoteHttpTransferManagerMessage)
                cellMessage.getMessageObject();
               if(transfer_request.isStore())
               {
                   sb.append(" store src=");
                   sb.append(transfer_request.getHttpUrl());
                   sb.append(" dest=");
                   sb.append(transfer_request.getPnfsPath());
               }
               else
               {
                   sb.append("restore src=");
                   sb.append(transfer_request.getPnfsPath());
                   sb.append(" dest=");
                   sb.append(transfer_request.getHttpUrl());
               }

               if(!long_format)
               {
                   continue;
               }
               sb.append("\n    uid=").append(transfer_request.getUid());
               sb.append(" gid=").append(transfer_request.getGid());
               sb.append(" try#").append(transfer_request.getNumberOfPerformedRetries());

           }
           return sb.toString();
       }
    }

   public void getInfo( PrintWriter printWriter )
   {
       StringBuffer sb = new StringBuffer();
       sb.append("    RemoteHttpTransferManager\n");
       sb.append("---------------------------------\n");
       sb.append("Name   : ").
        append(_nucleus.getCellName());
       sb.append("\nnumber of active transfers : ").
        append(num_transfers);
       synchronized(queue)
       {
       sb.append("\nnumber of queuedrequests : ").
        append(queue.size());
       }
       sb.append("\nmax number of active transfers  : ").
        append(getMax_transfers());
       sb.append("\nPoolManager timeout : ").
        append(__poolManagerTimeout).append(" seconds");
       sb.append("\nPool timeout  : ").
        append(poolTimeout).append(" seconds");
       sb.append("\nMover timeout  : ").
        append(moverTimeout).append(" seconds");
       sb.append("\nnext id  : ").
        append(nextMessageID);


       printWriter.println( sb.toString()) ;
   }


    public void messageArrived( CellMessage cellMessage )
    {
        Object object = cellMessage.getMessageObject();
        _log.info("Message messageArrived ["+object.getClass()+"]="+object.toString());
        _log.info("Message messageArrived source = "+cellMessage.getSourceAddress());
        if (object instanceof DoorTransferFinishedMessage)
        {

            DoorTransferFinishedMessage reply =
            (DoorTransferFinishedMessage)object ;
            long id = reply.getId();
            Object o = null;
            synchronized(longIdToMessageMap)
            {
                o = longIdToMessageMap.get(Long.valueOf(id));
                if(o != null && o instanceof Long)
                {
                    longIdToMessageMap.put(o,reply);
                }
                else
                {
                    _log.warn("DoorTransferFinishedMessage with unknown id ="+id+
                        " or unknown sync object in longIdToMessageMap " +o);
                    return;
                }
            }

            synchronized(o)
            {
                o.notify();
            }
            return;
        }
        else if(object instanceof RemoteHttpTransferManagerMessage)
        {
                if(new_transfer())
                {
                    _nucleus.newThread(new RemoteHttpTransferManager.RemoteHttpTransferHandler(
            cellMessage)).start() ;
                }
                else
                {
                    putOnQueue(cellMessage);
                }
            //new Thread().start();
            return;
        }
        super.messageArrived (cellMessage);


    }
    public void returnError(CellMessage cellMessage,String errormsg)
    {
        RemoteHttpTransferManagerMessage transfer_request =
        (RemoteHttpTransferManagerMessage)(cellMessage.getMessageObject());
        transfer_request.setReturnCode(1);
        transfer_request.setDescription(errormsg);

        try
        {
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        }
        catch(Exception e)
        {
            _log.warn(e.toString(), e);
        }
    }

    private class RemoteHttpTransferHandler implements Runnable
    {


        private CellMessage cellMessage;
        private RemoteHttpTransferManagerMessage transfer_request;
        private boolean requeue;
        private String state ="Pending";
        private PnfsId          pnfsId;
        private StorageInfo     storageInfo;
        private String pool;
        private boolean store;
        public RemoteHttpTransferHandler(CellMessage cellMessage)
        {
             this.cellMessage = cellMessage;
        }

        public synchronized String toString(boolean long_format)
        {
            if(transfer_request == null)
            {
                return state;
            }

           StringBuffer sb = new StringBuffer();
           if(store)
           {
               sb.append("store src=");
               sb.append(transfer_request.getHttpUrl());
               sb.append(" dest=");
               sb.append(transfer_request.getPnfsPath());
           }
           else
           {
               sb.append("restore src=");
               sb.append(transfer_request.getPnfsPath());
               sb.append(" dest=");
               sb.append(transfer_request.getHttpUrl());
           }
           if(!long_format)
           {
               return sb.toString();
           }
           sb.append("\n   ").append(state);
           sb.append("\n    uid=").append(transfer_request.getUid());
           sb.append(" gid=").append(transfer_request.getGid());
           sb.append(" try#").append(transfer_request.getNumberOfPerformedRetries());
           if(pnfsId != null)
           {
                sb.append("\n   pnfsId=").append(pnfsId);
           }
           if(storageInfo != null)
           {
                sb.append("\n  storageInfo=").append(storageInfo);
           }
           if(pool != null)
           {
               sb.append("\n   pool=").append(pool);
           }
           return sb.toString();

        }

        public synchronized String getState()
        {
            return state;
        }

        public synchronized void setState(String state)
        {
            this.state = state;
        }

        public String getHttpUrl()
        {
            RemoteHttpTransferManagerMessage req =
              transfer_request;
            if(req != null)
            {
                return req.getHttpUrl();
            }
            return "unknown";
        }

        public String getPnfsPath()
        {
            RemoteHttpTransferManagerMessage req =
              transfer_request;
            if(req != null)
            {
                return req.getPnfsPath();
            }
            return "unknown";
        }

        public boolean isStore()
        {
            RemoteHttpTransferManagerMessage req =
              transfer_request;
            if(req != null)
            {
                return req.isStore();
            }
            return false;
        }

        public  PnfsId  getPnfsId()
        {
            return pnfsId;
        }
        public StorageInfo getStorageInfo()
        {
            return storageInfo;
        }

        public String getPool()
        {
            return pool;
        }

        public void run()
        {
            while(cellMessage != null)
            {
                requeue = false;
                try
                {
                     synchronized(this)
                     {
                       this.transfer_request=(RemoteHttpTransferManagerMessage)
                            cellMessage.getMessageObject();
                            state ="Pending";
                            pnfsId = null;
                            storageInfo = null;
                            pool = null;
                            store = transfer_request.isStore();

                     }

                    if(store)
                    {
                        getFromRemoteHttpUrl(transfer_request.getUid(),
                            transfer_request.getGid(),
                            transfer_request.getBufferSize(),
                            transfer_request.getHttpUrl(),
                            transfer_request.getPnfsPath());
                            transfer_request.setReturnCode(0);
                        transfer_request.setDescription("file "+
                            transfer_request.getPnfsPath() +
                            " has been retrieved from "+
                            transfer_request.getPnfsPath());
                    }
                    else
                    {
                        transfer_request.setReturnCode(1);
                        transfer_request.setDescription(
                        "RemoteHttpTransfer restore is not supported");
                    }
                }
                catch(Exception e)
                {
                    int number_or_retries = transfer_request.getNumberOfRetries();
                    if(number_or_retries >0)
                    {
                        transfer_request.setNumberOfRetries(number_or_retries -1);
                        requeue = true;
                    }
                    else
                    {
                        transfer_request.setReturnCode(1);
                        transfer_request.setDescription("getFromRemoteHttpUrl failed:"+
                            e.getMessage());
                    }
                }
                finally
                {
                    finish_transfer();
                    transfer_request.increaseNumberOfPerformedRetries();
                    if(requeue)
                    {
                        _log.info("putting on queue for retry:"+cellMessage);
                        putOnQueue(cellMessage);
                    }
                    else
                    {
                        try
                        {
                            cellMessage.revertDirection();
                            sendMessage(cellMessage);
                        }
                        catch(Exception e)
                        {
                            _log.warn(e.toString(), e);
                        }
                    }
                }

                cellMessage = nextFromQueue();
            }
        }


           private void getFromRemoteHttpUrl(int uid, int gid,int buffer_size,
            String remoteTURL,
            String pnfsFilePath)
            throws java.io.IOException
        {
                long id = getNextMessageID();
                synchronized(activeTransfersIDs)
                {
                    Long longId = Long.valueOf(id);
                    activeTransfersIDs.add(longId);
                    activeTransfersIDsToHandlerMap.put(longId,this);
                }
                setState("checking user permissions");
                PnfsHandler pnfs_handler = new PnfsHandler( RemoteHttpTransferManager.this, _pnfsPath );
                int last_slash_pos = pnfsFilePath.lastIndexOf('/');

                if(last_slash_pos == -1)
                {
                    throw new java.io.IOException(
                    "pnfsFilePath is not absolute:"+pnfsFilePath);
                }

                String parentDir = pnfsFilePath.substring(0,last_slash_pos);
                PnfsCreateEntryMessage pnfsEntry = null;
                try
                {
                    PnfsGetStorageInfoMessage parent_info =
                        pnfs_handler.getStorageInfoByPath(parentDir);
                    diskCacheV111.util.FileMetaData parent_data =
                        parent_info.getMetaData();
                    boolean can_write = (parent_data.getUid() == uid) &&
                    parent_data.getUserPermissions().canWrite() &&
                    parent_data.getUserPermissions().canExecute();

                    can_write |= (parent_data.getGid() == gid ) &&
                    parent_data.getGroupPermissions().canWrite() &&
                    parent_data.getGroupPermissions().canExecute();

                    can_write |= parent_data.getWorldPermissions().canWrite() &&
                    parent_data.getWorldPermissions().canExecute();

                    if(!can_write)
                    {
                        throw new java.io.IOException(
                            "user has no premission to write to the path: "+
                            parentDir);
                    }


                    setState("creating pnfs error");
                    pnfsEntry =
                        pnfs_handler.createPnfsEntry(pnfsFilePath, uid,
                        gid,0644);
                    pnfsId       = pnfsEntry.getPnfsId() ;
                    storageInfo  = pnfsEntry.getStorageInfo() ;
                    RemoteHttpDataTransferProtocolInfo protocol_info =
                    new RemoteHttpDataTransferProtocolInfo("RemoteHttpDataTransfer",1,1,_hosts,0,
                       buffer_size,remoteTURL);
                    Thread current = Thread.currentThread();
                    setState("waiting for a read pool");
                    pool = askForReadWritePool(pnfsId,storageInfo,protocol_info,true);

                    Object sync = Long.valueOf(id);
                    synchronized(longIdToMessageMap)
                    {
                        // it only seems to not make any sence
                        // I need to get back exactly the object
                        // I am waiting on futher in the code
                        longIdToMessageMap.put(sync,sync);
                    }

                    setState("wating for a mover to complete a transfer");
                    askForFile(pool,pnfsId,storageInfo,protocol_info,true,id);
                    Object o;
                    synchronized(sync)
                    {
                        try
                        {
                            sync.wait(moverTimeout*1000); //24 hours
                        }
                        catch(InterruptedException ie)
                        {
                        }
                        synchronized(longIdToMessageMap)
                        {
                            o =longIdToMessageMap.remove(sync);
                        }
                    }


                    if(o == null || o instanceof Long)
                    {
                        _log.warn(" getFromRemoteHttpUrl: wait expired ");
                        //cleanup, interrupt trunsfer and return error
                        throw new java.io.IOException("getFromRemoteHttpUrl failed");
                    }

                    if(o instanceof DoorTransferFinishedMessage)
                    {
                        DoorTransferFinishedMessage finished =
                        (DoorTransferFinishedMessage) o;
                        if(finished.getReturnCode() == 0)
                        {
                            setState("success");
                            //success
                            _log.info("transfer finished successfully");
                            return;
                        }
                        else
                        {
                            throw new CacheException(
                            "Transer failed with error code "+
                            finished.getReturnCode()+
                            "reason: "+
                            finished.getErrorObject());
                        }

                    }

                }
                catch(Exception e)
                {
                    setState("error :" +e.toString());
                    _log.warn(e.toString(), e);
                    if(pnfsEntry != null)
                    {

                        _log.info("pnfsEntry != null, trying to delete created pnfs entry");
                        try
                        {
                            pnfs_handler.deletePnfsEntry(pnfsFilePath);
                        }
                        catch(Exception e1)
                        {
                            _log.warn(e1.toString(), e1);
                        }
                    }
                    String message = e.getMessage();
                    throw new java.io.IOException(" getFromRemoteHttpUrl() exception "+
                    message == null?e.toString():message);
                }
                finally
                {
                    synchronized(longIdToMessageMap)
                    {
                        Long longId = Long.valueOf(id);
                        activeTransfersIDs.remove(longId);
                        activeTransfersIDsToHandlerMap.remove(longId);
                        pnfsId = null;
                        storageInfo = null;

                    }
                }
                throw new java.io.IOException("unknown failure");

        }

        public void putToRemoteTURL(int uid, int gid, String pnfsFilePath,String RemoteTURL)
        throws java.io.IOException
        {
            throw new java.io.IOException("not implemented");
        }

        public String toString()
        {
            return toString(false);
        }
    }

    private String askForReadWritePool( PnfsId       pnfsId ,
    StorageInfo  storageInfo ,
    ProtocolInfo protocolInfo ,
    boolean      isWrite       ) throws CacheException {

        //
        // ask for a pool
        //
        PoolMgrSelectPoolMsg request =
        isWrite ?
        (PoolMgrSelectPoolMsg)
        new PoolMgrSelectWritePoolMsg(
        pnfsId,
        storageInfo,
        protocolInfo ,
        0L                 )
        :
            (PoolMgrSelectPoolMsg)
            new PoolMgrSelectReadPoolMsg(
            pnfsId  ,
            storageInfo,
            protocolInfo ,
            0L                 );

            _log.info("PoolMgrSelectPoolMsg: " + request );
            CellMessage reply;
            try
            {
                reply =
                sendAndWait(
                new CellMessage(  _poolMgrPath, request ) ,
                __poolManagerTimeout*1000
                );
            }
            catch(Exception e)
            {
                _log.warn(e.toString(), e);
                throw new CacheException(e.toString());
            }

            _log.info("CellMessage (reply): " + reply );
            if( reply == null )
                throw new
                CacheException("PoolMgrSelectReadPoolMsg timed out") ;

            Object replyObject = reply.getMessageObject();

            if( ! ( replyObject instanceof  PoolMgrSelectPoolMsg ) )
                throw new
                CacheException( "Not a PoolMgrSelectPoolMsg : "+
                replyObject.getClass().getName() ) ;

            request =  (PoolMgrSelectPoolMsg)replyObject;

            _log.info("poolManagerReply = "+request);

            if( request.getReturnCode() != 0 )
                throw new
                CacheException( "Pool manager error: "+
                request.getErrorObject() ) ;

            String pool = request.getPoolName();
            _log.info("Positive reply from pool "+pool);

            return pool ;

    }

    private void askForFile( String       pool ,
    PnfsId       pnfsId ,
    StorageInfo  storageInfo ,
    ProtocolInfo protocolInfo ,
    boolean      isWrite,
    long id) throws CacheException {

        _log.info("Trying pool "+pool+" for "+(isWrite?"Write":"Read"));

        PoolIoFileMessage poolMessage ;
        if( isWrite )
        {
            poolMessage =         new PoolAcceptFileMessage(
                pool,
                pnfsId,
                protocolInfo ,
                storageInfo     );
        }
        else
        {
            poolMessage =        new PoolDeliverFileMessage(
                pool,
                pnfsId ,
                protocolInfo ,
                storageInfo     );
        }
            poolMessage.setId( id ) ;

            CellMessage reply;
            try
            {
                reply= sendAndWait(new CellMessage(
                 new CellPath(pool) ,
                poolMessage
                )  ,
                poolTimeout*1000
                ) ;
            }
            catch(Exception e)
            {
                _log.warn(e.toString(), e);
                throw new CacheException(e.toString());
            }

            if( reply == null)
                throw new
                CacheException( "Pool request timed out : "+pool ) ;

            Object replyObject = reply.getMessageObject();

            if( ! ( replyObject instanceof PoolIoFileMessage ) )
                throw new
                CacheException( "Illegal Object received : "+
                replyObject.getClass().getName());

            PoolIoFileMessage poolReply = (PoolIoFileMessage)replyObject;

            if (poolReply.getReturnCode() != 0)
                throw new
                CacheException( "Pool error: "+poolReply.getErrorObject() ) ;

            _log.info("Pool "+pool+" will deliver file "+pnfsId);

    }

    protected static long   nextMessageID = 10000 ;

    private static synchronized long getNextMessageID()
    {
        if(nextMessageID == Long.MAX_VALUE)
        {
            nextMessageID = 10000;
            return Long.MAX_VALUE;
        }
        return nextMessageID++;
    }

    private int max_transfers = 30;
    private int num_transfers = 0;

    /** Getter for property max_transfers.
     * @return Value of property max_transfers.
     */
    public int getMax_transfers() {
        return max_transfers;
    }

    /** Setter for property max_transfers.
     * @param max_transfers New value of property max_transfers.
     */
    public void setMax_transfers(int max_transfers)
    {
        this.max_transfers = max_transfers;
        synchronized(queue)
        {
            while(queue.size() >0)
            {
                if(!new_transfer())
                {
                    break;
                }
                CellMessage nextMessage =
                (CellMessage) queue.remove(0);
                 _nucleus.newThread(
                    new RemoteHttpTransferManager.RemoteHttpTransferHandler(
                    nextMessage)).start() ;
            }
        }
    }

    private synchronized boolean new_transfer()
    {
        _log.info("new_transfer() num_transfers = "+num_transfers+
        " max_transfers="+max_transfers);
        if(num_transfers == max_transfers)
        {
            _log.info("new_transfer() returns false");
             return false;
        }
        _log.info("new_transfer() INCREMENT and return true");
        num_transfers++;
        return true;
    }

    private synchronized int active_transfers()
    {
        return num_transfers;
    }
    private synchronized void finish_transfer()
    {
        _log.info("finish_transfer() num_transfers = "+num_transfers+" DECREMENT");
        num_transfers--;
    }

    private ArrayList queue = new ArrayList();

    private synchronized void putOnQueue(CellMessage request)
    {
        queue.add(request);
    }

    private synchronized CellMessage nextFromQueue()
    {

        if(queue.size() >0)
        {
            if(new_transfer())
            {
                return (CellMessage)queue.remove(0);
            }
        }
        return null;
    }


}


