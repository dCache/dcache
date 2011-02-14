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
import java.util.Map;
import java.util.Iterator;
import java.util.EnumSet;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import org.dcache.cells.AbstractCell;
import static org.dcache.namespace.FileAttribute.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author  timur
 */
public class CopyManager extends AbstractCell
{
    private final static Logger _log =
        LoggerFactory.getLogger(CopyManager.class);

    private final Map<Long,CopyHandler> activeTransfers =
        new ConcurrentHashMap<Long,CopyHandler>();
    private final Queue<CellMessage> queue = new ArrayDeque<CellMessage>();

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
    private String poolProxy;
    private PnfsHandler _pnfs;

    private int nextSessionId = 100;
    private static long nextMessageID = 10000;
    private int _maxTransfers = 30;
    private int _numTransfers = 0;

    /** Creates a new instance of CopyManager */
    public CopyManager(String cellName, String argString)
        throws InterruptedException, ExecutionException
    {
        super(cellName, argString);
        doInit();
    }

    @Override
    protected void init()
        throws Exception
    {
        _nucleus  = getNucleus();
        _args = getArgs();
        _pnfsPath = new CellPath ("PnfsManager");
        InetAddress[] addresses =
            InetAddress.getAllByName(InetAddress.getLocalHost().getHostName());
        _hosts = new String[addresses.length];
        for (int i = 0; i < addresses.length; ++i) {
            _hosts[i] = addresses[i].getHostName();
        }

        String tmpstr = _args.getOpt ("pool_manager_timeout");
        if (tmpstr != null) {
            __poolManagerTimeout =Integer.parseInt (tmpstr);
        }


        tmpstr = _args.getOpt ("pool_timeout");
        if (tmpstr != null) {
            poolTimeout =Integer.parseInt (tmpstr);
        }

        tmpstr = _args.getOpt ("mover_timeout");
        if (tmpstr != null) {
            moverTimeout =Integer.parseInt (tmpstr);
        }

        tmpstr = _args.getOpt ("max_transfers");
        if (tmpstr != null) {
            _maxTransfers = Integer.parseInt(tmpstr);
        }
        if (_args.getOpt("poolManager") != null) {
            poolManager = _args.getOpt("poolManager");
        }
        _poolMgrPath = new CellPath(poolManager);

        poolProxy = _args.getOpt("poolProxy");

        _pnfs = new PnfsHandler(this, _pnfsPath);

        _log.info("Pool Proxy {}",
                  (poolProxy == null ? "not set" : ("set to " + poolProxy)));
    }

    // transfers ls
    // queue ls
    // kill active
    // remove queued



    public final static String hh_set_max_transfers = "<#max transfers>";
    public String ac_set_max_transfers_$_1( Args args )throws CommandException
    {
       int max = Integer.parseInt(args.argv(0));
       if (max <= 0)
       {
           return "Error, max transfers number should be greater then 0 ";
       }
       setMaxTransfers(max);
       return "set maximum number of active transfers to " + max;
    }

    public final static String hh_set_mover_timeout = "<#seconds>";
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

    public final static String hh_set_pool_timeout = "<#seconds>";
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

    public final static String hh_set_pool_manager_timeout = "<#seconds>";
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

    public final static String hh_ls = "[-l] [<#transferId>]";
    public String ac_ls_$_0_1( Args args )throws CommandException
    {
       boolean long_format = args.getOpt("l") != null;
       if(args.argc() >0)
       {
           long id = Long.parseLong(args.argv(0));
           CopyHandler transferHandler = activeTransfers.get(id);
           if (transferHandler == null) {
               return "ID not found : "+id;
           }
           return " transfer id="+id+" : "+
               transferHandler.toString(long_format);
       }
       StringBuilder sb =  new StringBuilder();
       if (activeTransfers.isEmpty()) {
           return "No Active Transfers";
       }
       sb.append("  Active Transfers ");
       for (Map.Entry<Long,CopyHandler> entry: activeTransfers.entrySet()) {
           sb.append("\n#").append(entry.getKey());
           sb.append(" ").append(entry.getValue().toString(long_format));
       }
       return sb.toString();
    }

    public final static String hh_queue = "[-l]";
    public String ac_queue_$_0(Args args)throws CommandException
    {
        boolean long_format = args.getOpt("l") != null;
        StringBuilder sb = new StringBuilder();
        synchronized (queue) {
           if (queue.isEmpty()) {
               return "Queue is empty";
           }

           int i = 0;
           for (CellMessage envelope: queue) {
               sb.append("\n#").append(i++);
               CopyManagerMessage request =
                   (CopyManagerMessage) envelope.getMessageObject();
               sb.append(" store src=");
               sb.append(request.getSrcPnfsPath());
               sb.append(" dest=");
               sb.append(request.getDstPnfsPath());

               if (!long_format) {
                   continue;
               }
               sb.append("\n    uid=").append(request.getUid());
               sb.append(" gid=").append(request.getGid());
               sb.append(" try#").append(request.getNumberOfPerformedRetries());
           }
       }
       return sb.toString();
    }

    @Override
    public void getInfo(PrintWriter printWriter)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("    CopyManager\n");
        sb.append("---------------------------------\n");
        sb.append("Name   : ").
            append(_nucleus.getCellName());
        sb.append("\nnumber of active transfers : ").
            append(_numTransfers);
        synchronized(queue) {
            sb.append("\nnumber of queuedrequests : ").append(queue.size());
        }
        sb.append("\nmax number of active transfers  : ").
            append(getMaxTransfers());
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

        printWriter.println(sb);
    }


    public void notifyHandler(long handlerId,Object message)
    {
        _log.info("CopyManager.notifyHandler()");

        CopyHandler handler = activeTransfers.get(handlerId);
        if (handler != null) {
            handler.messageNotify(message);
        } else {
            _log.warn("message arived for unknown handler id = {} message = {}",
                      handlerId, message);
        }
    }

    public void messageArrived(DoorTransferFinishedMessage message)
    {
        notifyHandler(message.getId(), message);
    }

    public void messageArrived(DCapClientPortAvailableMessage message)
    {
        notifyHandler(message.getId(), message);
    }

    public void messageArrived(CellMessage envelope, CopyManagerMessage message)
    {
        if (newTransfer()) {
            new Thread(new CopyManager.CopyHandler(envelope)).start();
        } else {
            putOnQueue(envelope);
        }
    }

    public void returnError(CellMessage envelope, String errormsg)
    {
        CopyManagerMessage transfer_request =
            (CopyManagerMessage)(envelope.getMessageObject());
        transfer_request.setReturnCode(1);
        transfer_request.setDescription(errormsg);

        try {
            envelope.revertDirection();
            sendMessage(envelope);
        } catch (NoRouteToCellException e) {
            _log.warn(e.toString());
        }
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
            _log.info("CopyHandler.messageNotify("+message+")");
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
                _log.info("CopyHandler.messageWait returns "+incommingMessage);
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

           StringBuilder sb = new StringBuilder();
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
                            _log.info("starting  processing transfer message with id "+transfer_request.getId());
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
                    finishTransfer();
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
                long srcId = id;
                activeTransfers.put(id, this);
                setState("checking user permissions");
                FileAttributes dstPnfsEntry = null;
                try
                {
                    //
                    // first get source file info,
                    // check that it exists, we can read it etc.
                    //
                    FileAttributes srcPnfsEntry =
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

                    dstPool = askForReadWritePool(
                        dstPnfsEntry,
                        dstPnfsFilePath,
                        dst_protocol_info,true);

                    setState("wating for a write mover to give us "+
                        " a listening port");

                    askForFile(dstPool,
                        dstPnfsId,
                        dstStorageInfo,
                        dst_protocol_info,
                        true,id);

                    _log.info("copy is calling messageWait");
                    Object o = messageWait(moverTimeout*1000);
                    _log.info("messageWait returned "+ o);

                    if(o == null )
                    {
                        _log.warn("copy: wait expired ");
                        //cleanup, interrupt trunsfer and return error
                        throw new java.io.IOException(
                            "copy failed: wait for port from write mover expired");
                    }

                    if(!(o instanceof DCapClientPortAvailableMessage))
                    {
                        _log.warn("copy: unexpected message arrived " +o);
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
                   srcPool = askForReadWritePool(
                        srcPnfsEntry,
                        srcPnfsFilePath,
                        src_protocol_info,
                        false);
                    if(srcPool.equals(dstPool ) )
                    {
                         // if source and dest pools are the same,
                        // the second message with the same id will
                        // ignored as DUP, so we might need two ids

                        srcId = getNextMessageID();
                        activeTransfers.put(srcId, this);
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
                        _log.info("messageWait returned "+ o);
                        if(o == null )
                        {
                            _log.warn("copy: wait for DoorTransferFinishedMessage expired ");
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
                                    _log.info("src mover reported success ");
                                    srcDone = true;
                                }

                                if(finished.getPnfsId().equals(dstPnfsId))
                                {
                                    _log.info("dst mover reported success ");
                                    dstDone = true;
                                }

                                if(srcDone && dstDone)
                                {
                                    setState("success");
                                    //success
                                    _log.info("transfer finished successfully");
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
                            _log.warn("copy: unexpected message arrived " +o);
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
                        _log.info("pnfsEntry != null, trying to delete created pnfs entry");
                        try
                        {
                            _pnfs.deletePnfsEntry(dstPnfsFilePath);
                        }
                        catch(Exception e1)
                        {
                            _log.warn(e1.toString(), e1);
                        }
                    }

                    setState("error :" +e.toString());
                    _log.warn(e.toString(), e);
                    throw new java.io.IOException(e.toString());
                }
                finally
                {
                    activeTransfers.remove(id);
                    activeTransfers.remove(srcId);
                }
        }

        FileAttributes createDestinationPnfsEntry(int uid,int gid,
                                                  String dstPnfsFilePath)
            throws Exception
        {
            int last_slash_pos = dstPnfsFilePath.lastIndexOf('/');

            if(last_slash_pos == -1)
            {
                throw new java.io.IOException(
                "pnfsFilePath is not absolute:"+dstPnfsFilePath);
            }
            String parentDir = dstPnfsFilePath.substring(0,last_slash_pos);
            PnfsCreateEntryMessage pnfsEntry = null;

             PnfsGetStorageInfoMessage parent_info =
                _pnfs.getStorageInfoByPath(parentDir);
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
            return  _pnfs.createPnfsEntry(dstPnfsFilePath, uid,
                                                gid,0644).getFileAttributes();
        }

        private FileAttributes getSrcPnfsInfo(int uid, int gid,
                                              String srcPnfsFilePath)
            throws Exception
        {
            EnumSet<FileAttribute> attributes =
                EnumSet.of(OWNER, OWNER_GROUP, MODE);
            attributes.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());
            FileAttributes info =
                _pnfs.getFileAttributes(srcPnfsFilePath, attributes);

            diskCacheV111.util.FileMetaData metadata =
                new diskCacheV111.util.FileMetaData(info);

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
    private String askForReadWritePool(FileAttributes fileAttributes,
                                       String pnfsPath,
                                       ProtocolInfo protocolInfo ,
                                       boolean isWrite)
        throws CacheException
    {
        //
        // ask for a pool
        //
        PoolMgrSelectPoolMsg request =
            isWrite ?
            (PoolMgrSelectPoolMsg)
            new PoolMgrSelectWritePoolMsg(fileAttributes,
                                          protocolInfo,
                                          0L)
            :
            (PoolMgrSelectPoolMsg)
            new PoolMgrSelectReadPoolMsg(fileAttributes,
                                         protocolInfo,
                                         0L);
        request.setPnfsPath(pnfsPath);

            _log.info("PoolMgrSelectPoolMsg: " + request.toString() );
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

            _log.info("CellMessage (reply): " + reply);
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
                pnfsId ,
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

    private static synchronized long getNextMessageID()
    {
        if(nextMessageID == Long.MAX_VALUE)
        {
            nextMessageID = 10000;
            return Long.MAX_VALUE;
        }
        return nextMessageID++;
    }

    /** Getter for property max_transfers.
     * @return Value of property max_transfers.
     */
    public int getMaxTransfers()
    {
        return _maxTransfers;
    }

    /** Setter for property max_transfers.
     * @param max_transfers New value of property max_transfers.
     */
    public void setMaxTransfers(int maxTransfers)
    {
        synchronized (queue)
        {
            _maxTransfers = maxTransfers;
            while (!queue.isEmpty())
            {
                if (!newTransfer())
                {
                    break;
                }
                CellMessage nextMessage = queue.remove();
                _nucleus.newThread(new CopyManager.CopyHandler(nextMessage)).start() ;
            }
        }
    }

    private synchronized boolean newTransfer()
    {
        _log.debug("newTransfer() numTransfers = {} maxTransfers = {}",
                   _numTransfers, _maxTransfers);
        if (_numTransfers == _maxTransfers) {
            _log.info("new_transfer() returns false");
             return false;
        }
        _log.info("new_transfer() INCREMENT and return true");
        _numTransfers++;
        return true;
    }

    private synchronized void finishTransfer()
    {
        _log.debug("finishTransfer() numTransfers = {} DECREMENT",
                   _numTransfers);
        _numTransfers--;
    }

    private synchronized void putOnQueue(CellMessage request)
    {
        queue.add(request);
    }

    private synchronized CellMessage nextFromQueue()
    {
        if (!queue.isEmpty())
        {
            if(newTransfer())
            {
                return (CellMessage) queue.remove();
            }
        }
        return null;
    }

    private synchronized int getSessionNextId()
    {
        return nextSessionId++;
    }
}


