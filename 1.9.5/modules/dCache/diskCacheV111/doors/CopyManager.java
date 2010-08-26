/*
 * CopyManager.java
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
import diskCacheV111.vehicles.DCapClientProtocolInfo;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.CopyManagerMessage;
import diskCacheV111.vehicles.DCapClientPortAvailableMessage;
import diskCacheV111.vehicles.DCapProtocolInfo;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
/**
 *
 * @author  timur
 */
public class CopyManager extends CellAdapter {
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
    private int  buffer_size=256*1024;
    private int tcp_buffer_size =256*1024;
    protected String poolManager ="PoolManager";
    private String poolProxy ;


    /** Creates a new instance of CopyManager */
    public CopyManager(String cellName, String argString) throws Exception
    {
        super(cellName,argString,false);
       _nucleus  = getNucleus() ;
        _args = new Args(argString);
       _pnfsPath = new CellPath ( "PnfsManager" );
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
         if(_args.getOpt("poolManager") != null) {
                 poolManager = _args.getOpt("poolManager");
         }
         _poolMgrPath     = new CellPath ( poolManager ) ;

         poolProxy = _args.getOpt("poolProxy");
            say("Pool Proxy "+( poolProxy == null ? "not set" : ( "set to "+poolProxy ) ) );

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
               CopyHandler transferHandler =
                (CopyHandler)
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
               CopyHandler transferHandler =
                (CopyHandler)
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
              CopyManagerMessage transfer_request =
                (CopyManagerMessage)
                cellMessage.getMessageObject();
               sb.append(" store src=");
               sb.append(transfer_request.getSrcPnfsPath());
               sb.append(" dest=");
               sb.append(transfer_request.getDstPnfsPath());

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
       sb.append("    CopyManager\n");
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
        sb.append("\nPoolManager  : ").
                append(poolManager);
       sb.append("\nPoolManager timeout : ").
        append(__poolManagerTimeout).append(" seconds");
       sb.append("\nPool timeout  : ").
        append(poolTimeout).append(" seconds");
       sb.append("\nMover timeout  : ").
        append(moverTimeout).append(" seconds");
       sb.append("\nnext id  : ").
        append(nextMessageID);
        sb.append("\nPool Proxy : ").
                append(poolProxy == null ? "not set" : ( "set to "+poolProxy ));

       printWriter.println( sb.toString()) ;
   }


    public void notifyHandler(long handlerId,Object message)
    {
        say("CopyManager.notifyHandler()");

        synchronized(activeTransfersIDsToHandlerMap)
        {
            CopyHandler handler =
                (CopyHandler)
                activeTransfersIDsToHandlerMap.get(Long.valueOf(handlerId));
            if(handler != null)
            {
                handler.messageNotify(message);
            }
            else
            {
                esay("message arived for unknown handler id ="+handlerId+
                " message = "+message);
            }
        }
    }
    public void messageArrived( CellMessage cellMessage )
    {
        Object object = cellMessage.getMessageObject();
        say("Message messageArrived ["+object.getClass()+"]="+object.toString());
        say("Message messageArrived source = "+cellMessage.getSourceAddress());
        if (object instanceof DoorTransferFinishedMessage)
        {

            long id = ((DoorTransferFinishedMessage)object ).getId();
            notifyHandler(id,object);

            return;
        }
        else if(object instanceof DCapClientPortAvailableMessage)
        {
            long id = ((DCapClientPortAvailableMessage)object).getId();
            notifyHandler(id,object);

            return;
        }
        else if(object instanceof CopyManagerMessage)
        {
                if(new_transfer())
                {
                    _nucleus.newThread(new CopyManager.CopyHandler(
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
        CopyManagerMessage transfer_request =
        (CopyManagerMessage)(cellMessage.getMessageObject());
        transfer_request.setReturnCode(1);
        transfer_request.setDescription(errormsg);

        try
        {
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        }
        catch(Exception e)
        {
            esay(e);
        }
    }

    PnfsHandler pnfs_handler;
    public PnfsHandler getPnfsHandler()
    {
        if(pnfs_handler == null)
        {
            pnfs_handler = new PnfsHandler( CopyManager.this,
                    _pnfsPath );
        }
        return pnfs_handler;
    }


    private class CopyHandler implements Runnable
    {


        private CellMessage cellMessage;
        private CopyManagerMessage transfer_request;
        private boolean requeue;
        private String state ="Pending";
        private PnfsId          srcPnfsId;
        private PnfsId          dstPnfsId;
        private StorageInfo     srcStorageInfo;
        private StorageInfo     dstStorageInfo;
        private String srcPool;
        private String dstPool;
        private InetAddress mover_address;
        private int mover_port;
        private Object incommingMessage;
        private boolean notified = false;
        private Object sync = new Object();
        public void messageNotify(Object message)
        {
            say("CopyHandler.messageNotify("+message+")");
            synchronized(sync)
            {
                incommingMessage = message;
                notified = true;
                sync.notify();
            }
        }

        public Object messageWait(long timeout)
        {
            synchronized(sync)
            {
                if(notified)
                {
                    notified = false;
                    return incommingMessage;
                }
                incommingMessage = null;
                try
                {
                    sync.wait(timeout);
                }
                catch(InterruptedException ie)
                {
                }
                say("CopyHandler.messageWait returns "+incommingMessage);
                notified = false;
                return incommingMessage;
            }
        }

        public CopyHandler(CellMessage cellMessage)
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
           sb.append("store src=");
           sb.append(transfer_request.getSrcPnfsPath());
           sb.append(" dest=");
           sb.append(transfer_request.getDstPnfsPath());
           if(!long_format)
           {
               return sb.toString();
           }
           sb.append("\n   ").append(state);
           sb.append("\n    uid=").append(transfer_request.getUid());
           sb.append(" gid=").append(transfer_request.getGid());
           sb.append(" try#").append(transfer_request.getNumberOfPerformedRetries());
           if(srcPnfsId != null)
           {
                sb.append("\n   srcPnfsId=").append(srcPnfsId);
           }
           if(dstPnfsId != null)
           {
                sb.append("\n   dstPnfsId=").append(dstPnfsId);
           }
           if(srcStorageInfo != null)
           {
                sb.append("\n  srcStorageInfo=").append(srcStorageInfo);
           }
           if(dstStorageInfo != null)
           {
                sb.append("\n  dstStorageInfo=").append(dstStorageInfo);
           }
           if(srcPool != null)
           {
               sb.append("\n   srcPool=").append(srcPool);
           }
           if(dstPool != null)
           {
               sb.append("\n   dstPool=").append(dstPool);
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

        public String getSrcPnfsPath()
        {
            CopyManagerMessage req =
              transfer_request;
            if(req != null)
            {
                return req.getSrcPnfsPath();
            }
            return "unknown";
        }

        public String getDstPnfsPath()
        {
            CopyManagerMessage req =
              transfer_request;
            if(req != null)
            {
                return req.getDstPnfsPath();
            }
            return "unknown";
        }


        public  PnfsId  getSrcPnfsId()
        {
            return srcPnfsId;
        }

        public  PnfsId  getDstPnfsId()
        {
            return srcPnfsId;
        }

        public StorageInfo getSrcStorageInfo()
        {
            return srcStorageInfo;
        }
        public StorageInfo getDstStorageInfo()
        {
            return dstStorageInfo;
        }

        public String getSrcPool()
        {
            return srcPool;
        }

        public String getDstPool()
        {
            return dstPool;
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

                       this.transfer_request=(CopyManagerMessage)
                            cellMessage.getMessageObject();
                            say("starting  processing transfer message with id "+transfer_request.getId());
                            state ="Pending";
                            srcPnfsId = null;
                            dstPnfsId = null;
                            srcStorageInfo = null;
                            dstStorageInfo = null;
                            srcPool = null;
                            dstPool = null;

                     }

                       copy(
                        transfer_request.getUid(),
                        transfer_request.getGid(),
                        transfer_request.getSrcPnfsPath(),
                        transfer_request.getDstPnfsPath());

                        transfer_request.setReturnCode(0);

                        transfer_request.setDescription("file "+
                        transfer_request.getDstPnfsPath() +
                        " has been copied from "+
                        transfer_request.getSrcPnfsPath());
                }
                catch(Exception e)
                {
                    int number_or_retries = transfer_request.getNumberOfRetries()-1;
                    transfer_request.setNumberOfRetries(number_or_retries);

                    if(number_or_retries >0)
                    {
                        requeue = true;
                    }
                    else
                    {
                        transfer_request.setReturnCode(1);
                        transfer_request.setDescription("copy failed:"+
                            e.getMessage());
                    }
                }
                finally
                {
                    finish_transfer();
                    transfer_request.increaseNumberOfPerformedRetries();
                    if(requeue)
                    {
                        say("putting on queue for retry:"+cellMessage);
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
                            esay(e);
                        }
                    }
                }

                cellMessage = nextFromQueue();
            }
        }

        private void copy(
            int uid,
            int gid,
            String srcPnfsFilePath,
            String dstPnfsFilePath)
            throws Exception
        {
                // if source and dest pools are the same,
                // the second message with the same id will
                // ignored as DUP, so we might need two ids

                long id = getNextMessageID();
                long srcId =id;
                synchronized(activeTransfersIDs)
                {
                    Long longId = Long.valueOf(id);
                    activeTransfersIDs.add(longId);
                    activeTransfersIDsToHandlerMap.put(longId,this);
                }
                setState("checking user permissions");
                PnfsHandler pnfs_handler = getPnfsHandler();
                PnfsCreateEntryMessage dstPnfsEntry = null;
                try
                {
                    //
                    // first get source file info,
                    // check that it exists, we can read it etc.
                    //
                    PnfsGetStorageInfoMessage srcPnfsEntry =
                        getSrcPnfsInfo(uid,gid,srcPnfsFilePath);
                    srcPnfsId = srcPnfsEntry.getPnfsId();
                    srcStorageInfo = srcPnfsEntry.getStorageInfo();
                    //
                    // second check that we have permissions
                    // to, and then create destination file entry
                    //
                    dstPnfsEntry =
                        createDestinationPnfsEntry(uid,gid,dstPnfsFilePath);
                    dstPnfsId = dstPnfsEntry.getPnfsId();
                    dstStorageInfo = dstPnfsEntry.getStorageInfo();
                    //
                    //
                    DCapClientProtocolInfo dst_protocol_info =
                    new DCapClientProtocolInfo(
                        "DCapClient",
                        1,1,_hosts,
                        _nucleus.getCellName(),
                        _nucleus.getCellDomainName(),
                        id,
                        buffer_size,
                        tcp_buffer_size
                        );
                    Thread current = Thread.currentThread();
                    setState("waiting for a write pool");

                    dstPool = askForReadWritePool(dstPnfsId,
                        dstPnfsFilePath,
                        dstStorageInfo,dst_protocol_info,true);

                    setState("wating for a write mover to give us "+
                        " a listening port");

                    askForFile(dstPool,
                        dstPnfsId,
                        dstStorageInfo,
                        dst_protocol_info,
                        true,id);

                    say("copy is calling messageWait");
                    Object o = messageWait(moverTimeout*1000);
                    say("messageWait returned "+ o);

                    if(o == null )
                    {
                        esay("copy: wait expired ");
                        //cleanup, interrupt trunsfer and return error
                        throw new java.io.IOException(
                            "copy failed: wait for port from write mover expired");
                    }

                    if(!(o instanceof DCapClientPortAvailableMessage))
                    {
                        esay("copy: unexpected message arrived " +o);
                        throw new java.io.IOException(
                            "copy failed: received unexpected message while waiting "+
                            "for port from write mover " + o);
                    }

                    DCapClientPortAvailableMessage deleg_req =
                        (DCapClientPortAvailableMessage)o;

                   DCapProtocolInfo src_protocol_info =
                       new DCapProtocolInfo(
                            "DCap" , 3 , 0 ,
                            deleg_req.getHost(),
                            deleg_req.getPort() ) ;
                   src_protocol_info.setSessionId( getSessionNextId());
                   setState("waiting for a read pool");
                   srcPool = askForReadWritePool(srcPnfsId,
                        srcPnfsFilePath,
                        srcStorageInfo,
                        src_protocol_info,
                        false);
                    if(srcPool.equals(dstPool ) )
                    {
                         // if source and dest pools are the same,
                        // the second message with the same id will
                        // ignored as DUP, so we might need two ids

                        srcId = getNextMessageID();
                        Long longId = srcId;
                        activeTransfersIDs.add(longId);
                        activeTransfersIDsToHandlerMap.put(longId,this);

                    }

                    askForFile(srcPool,
                    srcPnfsId,
                    srcStorageInfo,
                    src_protocol_info,
                    false,srcId);

                    boolean srcDone = false;
                    boolean dstDone = false;
                    while(true)
                    {
                        o = messageWait(moverTimeout*1000);
                        say("messageWait returned "+ o);
                        if(o == null )
                        {
                            esay("copy: wait for DoorTransferFinishedMessage expired ");
                            //cleanup, interrupt trunsfer and return error
                            throw new java.io.IOException(
                                "copy: wait for DoorTransferFinishedMessage expired");
                        }

                        if(o instanceof DoorTransferFinishedMessage)
                        {
                            DoorTransferFinishedMessage finished =
                            (DoorTransferFinishedMessage) o;
                            if(finished.getReturnCode() == 0)
                            {
                                if(finished.getPnfsId().equals(srcPnfsId))
                                {
                                    say("src mover reported success ");
                                    srcDone = true;
                                }

                                if(finished.getPnfsId().equals(dstPnfsId))
                                {
                                    say("dst mover reported success ");
                                    dstDone = true;
                                }

                                if(srcDone && dstDone)
                                {
                                    setState("success");
                                    //success
                                    say("transfer finished successfully");
                                    return;
                                }
                            }
                            else
                            {
                                throw new CacheException(
                                "Transer failed with error code "+
                                finished.getReturnCode()+
                                " reason: "+
                                finished.getErrorObject());
                            }

                        }
                        else
                        {
                            esay("copy: unexpected message arrived " +o);
                            throw new java.io.IOException(
                                "copy failed: received unexpected message while waiting "+
                                "for DoorTransferFinishedMessage from write  and read movers " + o);
                        }
                    }
                }
                catch(Exception e)
                {
                    if(dstPnfsEntry != null)
                    {
                        say("pnfsEntry != null, trying to delete created pnfs entry");
                        try
                        {
                            pnfs_handler = getPnfsHandler();
                            pnfs_handler.deletePnfsEntry(dstPnfsFilePath);
                        }
                        catch(Exception e1)
                        {
                            esay(e1);
                        }
                    }

                    setState("error :" +e.toString());
                    esay(e);
                    throw new java.io.IOException(e.toString());
                }
                finally
                {
                    synchronized(longIdToMessageMap)
                    {
                        Long longId = Long.valueOf(id);
                        activeTransfersIDs.remove(longId);
                        activeTransfersIDsToHandlerMap.remove(longId);
                        if(srcPool.equals(dstPool ) ) {
                            longId = srcId;
                            activeTransfersIDs.remove(longId);
                            activeTransfersIDsToHandlerMap.remove(longId);

                        }
                    }
                }
        }

        PnfsCreateEntryMessage createDestinationPnfsEntry(int uid,int gid,
            String dstPnfsFilePath)
            throws Exception
        {
            PnfsHandler pnfs_handler = getPnfsHandler();
            int last_slash_pos = dstPnfsFilePath.lastIndexOf('/');

            if(last_slash_pos == -1)
            {
                throw new java.io.IOException(
                "pnfsFilePath is not absolute:"+dstPnfsFilePath);
            }
            String parentDir = dstPnfsFilePath.substring(0,last_slash_pos);
            PnfsCreateEntryMessage pnfsEntry = null;

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
                    "user has no permission to write to directory "+parentDir );
            }
            return    pnfs_handler.createPnfsEntry(dstPnfsFilePath, uid,
                gid,0644);


        }

        private PnfsGetStorageInfoMessage getSrcPnfsInfo(int uid, int gid,
            String srcPnfsFilePath)
            throws Exception
        {
            PnfsHandler pnfs_handler = getPnfsHandler();

            PnfsGetStorageInfoMessage info =
                pnfs_handler.getStorageInfoByPath(srcPnfsFilePath);

            diskCacheV111.util.FileMetaData metadata =
                info.getMetaData();

            boolean can_read = (metadata.getUid() == uid) &&
                metadata.getUserPermissions().canRead();

            can_read |= (metadata.getGid() == gid ) &&
                metadata.getGroupPermissions().canRead();

            can_read |= metadata.getWorldPermissions().canRead();

            if(!can_read)
            {
                throw new java.io.IOException("user has no permission to read this file");
            }

            return info;
        }

    }
    private String askForReadWritePool( PnfsId       pnfsId ,
    String pnfsPath,
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
        request.setPnfsPath(pnfsPath);

            say("PoolMgrSelectPoolMsg: " + request.toString() );
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
                esay(e);
                throw new CacheException(e.toString());
            }

            say("CellMessage (reply): " + reply);
            if( reply == null )
                throw new
                CacheException("PoolMgrSelectReadPoolMsg timed out") ;

            Object replyObject = reply.getMessageObject();

            if( ! ( replyObject instanceof  PoolMgrSelectPoolMsg ) )
                throw new
                CacheException( "Not a PoolMgrSelectPoolMsg : "+
                replyObject.getClass().getName() ) ;

            request =  (PoolMgrSelectPoolMsg)replyObject;

            say("poolManagerReply = "+request);

            if( request.getReturnCode() != 0 )
                throw new
                CacheException( "Pool manager error: "+
                request.getErrorObject() ) ;

            String pool = request.getPoolName();
            say("Positive reply from pool "+pool);

            return pool ;

    }

    private void askForFile( String       pool ,
    PnfsId       pnfsId ,
    StorageInfo  storageInfo ,
    ProtocolInfo protocolInfo ,
    boolean      isWrite,
    long id) throws CacheException {

        say("Trying pool "+pool+" for "+(isWrite?"Write":"Read"));

        PoolIoFileMessage poolMessage ;
        if( isWrite )
        {
            poolMessage =         new PoolAcceptFileMessage(
                pool,
                pnfsId.toString() ,
                protocolInfo ,
                storageInfo     );
        }
        else
        {
            poolMessage =        new PoolDeliverFileMessage(
                pool,
                pnfsId.toString() ,
                protocolInfo ,
                storageInfo     );
        }
            poolMessage.setId( id ) ;

           CellPath poolCellPath;
            if( poolProxy == null ){
                    poolCellPath = new CellPath(pool);
            }else{
                    poolCellPath = new CellPath(poolProxy);
                    poolCellPath.add(pool);
            }
            CellMessage reply;
            try
            {
                reply= sendAndWait(new CellMessage(
                 poolCellPath ,
                 poolMessage
                )  ,
                poolTimeout*1000
                ) ;
            }
            catch(Exception e)
            {
                esay(e);
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

            say("Pool "+pool+" will deliver file "+pnfsId);

    }

    private static long   nextMessageID = 10000 ;

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
                    new CopyManager.CopyHandler(
                    nextMessage)).start() ;
            }
        }
    }

    private synchronized boolean new_transfer()
    {
        say("new_transfer() num_transfers = "+num_transfers+
        " max_transfers="+max_transfers);
        if(num_transfers == max_transfers)
        {
            say("new_transfer() returns false");
             return false;
        }
        say("new_transfer() INCREMENT and return true");
        num_transfers++;
        return true;
    }

    private synchronized int active_transfers()
    {
        return num_transfers;
    }
    private synchronized void finish_transfer()
    {
        say("finish_transfer() num_transfers = "+num_transfers+" DECREMENT");
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
   private int                nextSessionId      = 100 ;
   private synchronized int getSessionNextId(){ return nextSessionId++ ; }


}


