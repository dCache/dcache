package diskCacheV111.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.URI;
import java.util.EnumSet;
import java.util.concurrent.Executor;

import diskCacheV111.doors.FTPTransactionLog;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.TransferCompleteMessage;
import diskCacheV111.vehicles.transferManager.TransferFailedMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

public class TransferManagerHandler extends AbstractMessageCallback<Message>
{
    private static final Logger log =
            LoggerFactory.getLogger(TransferManagerHandler.class);
    private final TransferManager manager;
    private TransferManagerMessage transferRequest;
    private CellPath requestor;
    private String pnfsPath;
    private transient String parentDir;
    boolean store;
    boolean created;
    private PnfsId pnfsId;
    private String pnfsIdString;
    private String remoteUrl;
    transient boolean locked;
    private String pool;
    private CellAddressCore poolAddress;
    private FTPTransactionLog tlog;
    private FileAttributes fileAttributes;
    public static final int INITIAL_STATE = 0;
    public static final int WAITING_FOR_PNFS_INFO_STATE = 1;
    public static final int RECEIVED_PNFS_INFO_STATE = 2;
    public static final int WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE = 5;
    public static final int RECEIVED_PNFS_ENTRY_CREATION_INFO_STATE = 6;
    public static final int WAITING_FOR_POOL_INFO_STATE = 7;
    public static final int RECEIVED_POOL_INFO_STATE = 8;
    public static final int WAITING_FIRST_POOL_REPLY_STATE = 9;
    public static final int RECEIVED_FIRST_POOL_REPLY_STATE = 10;
    public static final int WAITING_FOR_SPACE_INFO_STATE = 11;
    public static final int RECEIVED_SPACE_INFO_STATE = 12;
    public static final int WAITING_FOR_PNFS_ENTRY_DELETE = 13;
    public static final int RECEIVED_PNFS_ENTRY_DELETE = 14;
    public static final int WAITING_FOR_PNFS_CHECK_BEFORE_DELETE_STATE = 15;
    public static final int RECEIVED_PNFS_CHECK_BEFORE_DELETE_STATE = 16;
    public static final int SENT_ERROR_REPLY_STATE = -1;
    public static final int SENT_SUCCESS_REPLY_STATE = -2;
    public int state = INITIAL_STATE;
    private long id;
    private Integer moverId;
    private IpProtocolInfo protocol_info;
    private long creationTime;
    private long lifeTime;
    private Long credentialId;
    private transient int numberOfRetries;
    private transient int _replyCode;
    private transient Serializable _errorObject;
    private transient boolean _cancelTimer;
    private final transient DoorRequestInfoMessage info;
    private final transient PermissionHandler permissionHandler;
    private transient PoolMgrSelectReadPoolMsg.Context _readPoolSelectionContext;
    private final transient Executor executor;

    public TransferManagerHandler(TransferManager tManager,
                                  TransferManagerMessage message,
                                  CellPath requestor,
                                  Executor executor)
    {
        this.executor = executor;
        numberOfRetries = 0;
        creationTime = System.currentTimeMillis();
        manager = tManager;
        id = manager.getNextMessageID();
        message.setId(id);

        transferRequest = message;
        pnfsPath = transferRequest.getPnfsPath();
        store = transferRequest.isStore();
        remoteUrl = transferRequest.getRemoteURL();
        credentialId = transferRequest.getCredentialId();
        Subject subject = transferRequest.getSubject();

        info = new DoorRequestInfoMessage(manager.getCellName() + "@"
                + manager.getCellDomainName());
        info.setTransactionDuration(-creationTime);
        info.setSubject(subject);
        info.setPath(pnfsPath);
        info.setTimeQueued(-System.currentTimeMillis());
        this.requestor = requestor;
        try {
            info.setClient(new URI(transferRequest.getRemoteURL()).getHost());
        } catch (Exception e) {
        }

        try {
            if (manager.getLogRootName() != null) {
                tlog = new FTPTransactionLog(manager.getLogRootName());
                String user_info =
                        Subjects.getDn(transferRequest.getSubject())
                        + "(" + info.getUid() + "." + info.getGid() + ")";
                String rw = store ? "write" : "read";
                InetAddress remoteaddr =
                        InetAddress.getByName(new URI(transferRequest.getRemoteURL()).getHost());
                tlog.begin(user_info, "remotegsiftp", rw, transferRequest.getPnfsPath(), remoteaddr);
            }
        } catch (Exception e) {
            log.error("starting tlog failed :", e);
        }
        manager.addActiveTransfer(id, this);
        setState(INITIAL_STATE);
        permissionHandler =
                new ChainedPermissionHandler(
                new ACLPermissionHandler(),
                new PosixPermissionHandler());
    }

    public void handle()
    {
        log.debug("handling:  " + toString(true));
        int last_slash_pos = pnfsPath.lastIndexOf('/');
        if (last_slash_pos == -1) {
            transferRequest.setFailed(2,
                    new IOException("pnfsFilePath is not absolute:" + pnfsPath));
            return;
        }
        parentDir = pnfsPath.substring(0, last_slash_pos);
        PnfsGetFileAttributes message;
        if (store) {
            message = new PnfsCreateEntryMessage(pnfsPath);
            message.setSubject(transferRequest.getSubject());
            setState(WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE);
        } else {
            EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
            attributes.addAll(permissionHandler.getRequiredAttributes());
            attributes.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());
            message = new PnfsGetFileAttributes(pnfsPath, attributes);
            message.setSubject(transferRequest.getSubject());
            message.setAccessMask(EnumSet.of(AccessMask.READ_DATA));
            setState(WAITING_FOR_PNFS_INFO_STATE);
        }
        manager.persist(this);
        CellStub.addCallback(manager.getPnfsManagerStub().send(message), this, executor);
    }

    @Override
    public void success(Message message)
    {
            if (message instanceof PnfsCreateEntryMessage) {
                PnfsCreateEntryMessage create_msg =
                        (PnfsCreateEntryMessage) message;
                if (state == WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE) {
                    setState(RECEIVED_PNFS_ENTRY_CREATION_INFO_STATE);
                    createEntryResponseArrived(create_msg);
                    return;
                }
                log.error(this.toString() + " got unexpected PnfsCreateEntryMessage "
                        + " : " + create_msg + " ; Ignoring");
            } else if (message instanceof PnfsGetFileAttributes) {
                PnfsGetFileAttributes attributesMessage =
                        (PnfsGetFileAttributes) message;
                if (state == WAITING_FOR_PNFS_INFO_STATE) {
                    setState(RECEIVED_PNFS_INFO_STATE);
                    storageInfoArrived(attributesMessage);
                    return;
                }
                log.error(this.toString() + " got unexpected PnfsGetStorageInfoMessage "
                        + " : " + attributesMessage + " ; Ignoring");
            } else if (message instanceof PnfsMapPathMessage) {
                PnfsMapPathMessage mapMessage = (PnfsMapPathMessage) message;
                if (state == WAITING_FOR_PNFS_CHECK_BEFORE_DELETE_STATE) {
                    state = RECEIVED_PNFS_CHECK_BEFORE_DELETE_STATE;
                    deletePnfsEntry();
                    return;
                } else {
                    log.error(this.toString() + " got unexpected PnfsMapPathMessage "
                            + " : " + mapMessage + " ; Ignoring");
                }
            } else if (message instanceof PoolMgrSelectPoolMsg) {
                PoolMgrSelectPoolMsg select_pool_msg =
                        (PoolMgrSelectPoolMsg) message;
                if (state == WAITING_FOR_POOL_INFO_STATE) {
                    setState(RECEIVED_POOL_INFO_STATE);
                    poolInfoArrived(select_pool_msg);
                    return;
                }
                log.error(this.toString() + " got unexpected PoolMgrSelectPoolMsg "
                        + " : " + select_pool_msg + " ; Ignoring");
            } else if (message instanceof PoolIoFileMessage) {
                PoolIoFileMessage first_pool_reply =
                        (PoolIoFileMessage) message;
                if (state == WAITING_FIRST_POOL_REPLY_STATE) {
                    setState(RECEIVED_FIRST_POOL_REPLY_STATE);
                    poolFirstReplyArrived(first_pool_reply);
                    return;
                }
                log.error(this.toString() + " got unexpected PoolIoFileMessage "
                        + " : " + first_pool_reply + " ; Ignoring");
            } else if (message instanceof PnfsDeleteEntryMessage) {
                PnfsDeleteEntryMessage deleteReply = (PnfsDeleteEntryMessage) message;
                if (state == WAITING_FOR_PNFS_ENTRY_DELETE) {
                    setState(RECEIVED_PNFS_ENTRY_DELETE);
                    log.debug("Received PnfsDeleteEntryMessage, Deleted  : {}",
                            deleteReply.getPath());
                    sendErrorReply();
                }
            }
        manager.persist(this);
    }

    @Override
    public void failure(int rc, Object error)
    {
        switch (state) {
        case WAITING_FOR_PNFS_INFO_STATE:
            sendErrorReply(rc, "Failed to lookup file: " + error);
            break;

        case WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE:
            sendErrorReply(rc, "Failed to create namespace entry: " + error);
            break;

        case WAITING_FIRST_POOL_REPLY_STATE:
            // FIXME: in the case of an attempted read (pool pushing the file
            //        to some remote site), we can ask PoolManager for another
            //        pool.  For an attempted write (pool pulling the file)
            //        we must fail the transfer as we don't know if a mover
            //        was started.
            sendErrorReply(CacheException.SELECTED_POOL_FAILED,
                    "Failed while waiting for mover to start: " + error);
            break;

        case WAITING_FOR_PNFS_CHECK_BEFORE_DELETE_STATE:
            sendErrorReply(rc, "Pre-delete check failed: " + error);
            break;

        case WAITING_FOR_POOL_INFO_STATE:
            if (rc == CacheException.OUT_OF_DATE) {
                handle();
            } else {
                sendErrorReply(rc, "Failed to select pool: " + error);
            }
            break;

        case WAITING_FOR_PNFS_ENTRY_DELETE:
            log.warn("Delete attempt ({} of {}) failed: {}", numberOfRetries + 1,
                    manager.getMaxNumberOfDeleteRetries(), error);
            numberOfRetries++;
            if (numberOfRetries < manager.getMaxNumberOfDeleteRetries()) {
                deletePnfsEntry();
            } else {
                sendErrorReply(_replyCode, "Failed to delete file " +
                        "(" + error + "), triggered by: " + _errorObject);
            }
            break;

        default:
            /* The code should never get here, but we try to recover from bugs. */
            sendErrorReply(rc, "Failed in state " + state + ": " + error +
                    " [" + rc + "]");
            break;
        }
    }

    public void createEntryResponseArrived(PnfsCreateEntryMessage create)
    {
        created = true;
        manager.persist(this);

        fileAttributes = create.getFileAttributes();
        pnfsId = create.getPnfsId();
        pnfsIdString = pnfsId.toString();
        info.setPnfsId(pnfsId);
        info.setStorageInfo(create.getFileAttributes().getStorageInfo());
        selectPool();
    }

    public void storageInfoArrived(PnfsGetFileAttributes msg)
    {
        if (!store && tlog != null) {
            tlog.middle(msg.getFileAttributes().getSize());
        }
        //
        // Added by litvinse@fnal.gov
        //
        pnfsId = msg.getPnfsId();
        info.setPnfsId(pnfsId);
        info.setStorageInfo(msg.getFileAttributes().getStorageInfo());
        pnfsIdString = pnfsId.toString();
        manager.persist(this);
        if (store) {
            synchronized (manager.justRequestedIDs) {
                if (manager.justRequestedIDs.contains(msg.getPnfsId())) {
                    sendErrorReply(6, new CacheException("pnfs pnfsid: " + pnfsId.toString() + " file " + pnfsPath + "  is already there"));
                    return;
                }
                for (PnfsId pnfsid : manager.justRequestedIDs) {
                    log.debug("found pnfsid: {}", pnfsid);
                }
                manager.justRequestedIDs.add(pnfsId);
            }
        }

        if (fileAttributes == null) {
            fileAttributes =
                    msg.getFileAttributes();
        }

        log.debug("storageInfoArrived(uid={} gid={} pnfsid={} fileAttributes={}", info.getUid(), info.getGid(),
                  pnfsId, fileAttributes);
        selectPool();
    }

    public void selectPool()
    {
        protocol_info = manager.getProtocolInfo(transferRequest);
        PoolMgrSelectPoolMsg request = store
                ? new PoolMgrSelectWritePoolMsg(fileAttributes, protocol_info)
                : new PoolMgrSelectReadPoolMsg(fileAttributes, protocol_info, _readPoolSelectionContext);
        request.setPnfsPath(new FsPath(pnfsPath));
        request.setSubject(transferRequest.getSubject());
        log.debug("PoolMgrSelectPoolMsg: " + request);
        setState(WAITING_FOR_POOL_INFO_STATE);
        manager.persist(this);
        CellStub.addCallback(manager.getPoolManagerStub().send(request), this, executor);
    }

    public void poolInfoArrived(PoolMgrSelectPoolMsg pool_info)
    {
        log.debug("poolManagerReply = " + pool_info);

        if (pool_info instanceof PoolMgrSelectReadPoolMsg) {
            _readPoolSelectionContext =
                    ((PoolMgrSelectReadPoolMsg) pool_info).getContext();
        }

        setPool(pool_info.getPoolName());
        setPoolAddress(pool_info.getPoolAddress());
        fileAttributes = pool_info.getFileAttributes();
        manager.persist(this);
        log.debug("Positive reply from pool {}", pool);
        startMoverOnThePool();
    }

    public void startMoverOnThePool()
    {
        PoolIoFileMessage poolMessage = store
                ? new PoolAcceptFileMessage(
                pool,
                protocol_info,
                fileAttributes)
                : new PoolDeliverFileMessage(
                pool,
                protocol_info,
                fileAttributes);
        poolMessage.setPnfsPath(new FsPath(pnfsPath));
        poolMessage.setSubject(transferRequest.getSubject());
        if (manager.getIoQueueName() != null) {
            poolMessage.setIoQueueName(manager.getIoQueueName());
        }
        poolMessage.setInitiator(info.getTransaction());
        poolMessage.setId(id);
        setState(WAITING_FIRST_POOL_REPLY_STATE);
        manager.persist(this);
        CellPath poolCellPath;
        String poolProxy = manager.getPoolProxy();
        if (poolProxy == null) {
            poolCellPath = new CellPath(poolAddress);
        } else {
            poolCellPath = new CellPath(poolProxy);
            poolCellPath.add(poolAddress);
        }

        CellStub.addCallback(manager.getPoolStub().send(poolCellPath, poolMessage), this, executor);
    }

    public void poolFirstReplyArrived(PoolIoFileMessage poolMessage)
    {
        log.debug("poolReply = " + poolMessage);
        info.setTimeQueued(info.getTimeQueued() + System.currentTimeMillis());
        log.debug("Pool " + pool + " will deliver file " + pnfsId + " mover id is " + poolMessage.getMoverId());
        log.debug("Starting moverTimeout timer");
        manager.startTimer(id);
        setMoverId(poolMessage.getMoverId());
        manager.persist(this);

    }

    public void deletePnfsEntry()
    {
        if (state == RECEIVED_PNFS_CHECK_BEFORE_DELETE_STATE) {
            PnfsDeleteEntryMessage pnfsMsg = new PnfsDeleteEntryMessage(pnfsPath);
            setState(WAITING_FOR_PNFS_ENTRY_DELETE);
            manager.persist(this);
            pnfsMsg.setReplyRequired(true);
            CellStub.addCallback(manager.getPnfsManagerStub().send(pnfsMsg), this, executor);
        } else {
            PnfsMapPathMessage message = new PnfsMapPathMessage(pnfsPath);
            setState(WAITING_FOR_PNFS_CHECK_BEFORE_DELETE_STATE);
            CellStub.addCallback(manager.getPnfsManagerStub().send(message), this, executor);
        }
    }

    public void poolDoorMessageArrived(DoorTransferFinishedMessage doorMessage)
    {
        log.debug("poolDoorMessageArrived, doorMessage.getReturnCode()=" + doorMessage.getReturnCode());
        if (doorMessage.getReturnCode() != 0) {
            sendErrorReply(CacheException.THIRD_PARTY_TRANSFER_FAILED,
                    doorMessage.getErrorObject());
            return;
        }

        DoorTransferFinishedMessage finished = doorMessage;
        if (store && tlog != null) {
            tlog.middle(finished.getFileAttributes().getSize());
        }
        sendSuccessReply();
    }

    public void sendErrorReply(int replyCode,
            Serializable errorObject)
    {
        sendErrorReply(replyCode, errorObject, true);
    }

    public void sendErrorReply(int replyCode,
            Serializable errorObject,
            boolean cancelTimer)
    {

        _replyCode = replyCode;
        _errorObject = errorObject;
        _cancelTimer = cancelTimer;

        if (log.isDebugEnabled()) {
            log.debug("sending error reply {}:{} for {}", replyCode,
                    errorObject, toString(true));
        }

        if (store && created) {// Timur: I think this check  is not needed, we might not ever get storage info and pnfs id: && pnfsId != null && aMetadata != null && aMetadata.getFileSize() == 0) {
            if (state != WAITING_FOR_PNFS_ENTRY_DELETE && state != RECEIVED_PNFS_ENTRY_DELETE) {
                log.debug("deleting pnfs entry we created: {}", pnfsPath);
                deletePnfsEntry();
                return;
            }
        }


        if (tlog != null) {
            tlog.error("getFromRemoteGsiftpUrl failed: state = " + state
                               + " replyCode=" + replyCode + " errorObject="
                               + errorObject);
        }
        if (info.getTimeQueued() < 0) {
            info.setTimeQueued(info.getTimeQueued() + System
                    .currentTimeMillis());
        }
        if (info.getTransactionDuration() < 0) {
            info.setTransactionDuration(info
                    .getTransactionDuration() + System
                    .currentTimeMillis());
        }
        sendDoorRequestInfo(replyCode, errorObject.toString());

        setState(SENT_ERROR_REPLY_STATE, errorObject);
        manager.persist(this);

        if (cancelTimer) {
            manager.stopTimer(id);
        }



        if (store) {
            synchronized (manager.justRequestedIDs) {
                manager.justRequestedIDs.remove(pnfsId);
            }
        }
        manager.finishTransfer();
        try {
            TransferFailedMessage errorReply = new TransferFailedMessage(transferRequest, replyCode, errorObject);
            manager.sendMessage(new CellMessage(requestor, errorReply));
        } catch (Exception e) {
            log.error(e.toString());
            //can not do much more here!!!
        }
        Long longId = id;
        //this will allow the handler to be garbage collected
        // once we sent a response
        manager.removeActiveTransfer(longId);
    }

    public void sendErrorReply()
    {
        int replyCode = _replyCode;
        Serializable errorObject = _errorObject;
        boolean cancelTimer = _cancelTimer;

        if (log.isDebugEnabled()) {
            log.debug("sending error reply {}:{} for {}", replyCode,
                    errorObject, toString(true));
        }

        if (tlog != null) {
            tlog.error("getFromRemoteGsiftpUrl failed: state = " + state
                    + " replyCode=" + replyCode + " errorObject="
                    + errorObject);
        }
        if (info.getTimeQueued() < 0) {
            info.setTimeQueued(info.getTimeQueued() + System
                    .currentTimeMillis());
        }
        if (info.getTransactionDuration() < 0) {
            info.setTransactionDuration(info
                    .getTransactionDuration() + System
                    .currentTimeMillis());
        }
        sendDoorRequestInfo(replyCode, errorObject.toString());

        setState(SENT_ERROR_REPLY_STATE, errorObject);
        manager.persist(this);

        if (cancelTimer) {
            manager.stopTimer(id);
        }

        if (store) {
            synchronized (manager.justRequestedIDs) {
                manager.justRequestedIDs.remove(pnfsId);
            }
        }
        manager.finishTransfer();
        try {
            TransferFailedMessage errorReply = new TransferFailedMessage(transferRequest, replyCode, errorObject);
            manager.sendMessage(new CellMessage(requestor, errorReply));
        } catch (Exception e) {
            log.error(e.toString());
            //can not do much more here!!!
        }
        Long longId = id;
        //this will allow the handler to be garbage collected
        // once we sent a response
        manager.removeActiveTransfer(longId);
    }

    public void sendSuccessReply()
    {
        log.debug("sendSuccessReply for: " + toString(true));
        if (info.getTimeQueued() < 0) {
            info.setTimeQueued(info.getTimeQueued() + System
                    .currentTimeMillis());
        }
        if (info.getTransactionDuration() < 0) {
            info.setTransactionDuration(info
                    .getTransactionDuration() + System
                    .currentTimeMillis());
        }
        sendDoorRequestInfo(0, "");
        setState(SENT_SUCCESS_REPLY_STATE);
        manager.persist(this);
        manager.stopTimer(id);
        if (store) {
            synchronized (manager.justRequestedIDs) {
                manager.justRequestedIDs.remove(pnfsId);
            }
        }
        manager.finishTransfer();
        if (tlog != null) {
            tlog.success();
        }
        try {
            TransferCompleteMessage errorReply = new TransferCompleteMessage(transferRequest);
            manager.sendMessage(new CellMessage(requestor, errorReply));
        } catch (Exception e) {
            log.error(e.toString());
            //can not do much more here!!!
        }
        Long longId = id;
        //this will allow the handler to be garbage collected
        // once we sent a response
        manager.removeActiveTransfer(longId);
    }

    /**
     * Sends status information to the biling cell.
     */
    void sendDoorRequestInfo(int code, String msg)
    {
        try {
            info.setResult(code, msg);
            log.debug("Sending info: " + info);
            manager.getBillingStub().notify(info);
        } catch (NoRouteToCellException e) {
            log.error("Couldn't send billing info", e);
        }
    }

    public void cancel()
    {
        log.warn("the transfer is canceled by admin command, killing mover");
        if (moverId != null) {
            killMover(moverId);
        }
        sendErrorReply(24, new IOException("canceled"));
    }

    public void timeout()
    {
        log.error(" transfer timed out");
        if (moverId != null) {
            killMover(moverId);
        }
        sendErrorReply(24, new IOException("timed out while waiting for mover reply"), false);
    }

    public void cancel(CancelTransferMessage cancel)
    {
        log.warn("the transfer is canceled by " + cancel + ", killing mover");
        if (moverId != null) {
            killMover(moverId);
        }
        sendErrorReply(24, new IOException("canceled"));
    }

    public synchronized String toString(boolean long_format)
    {
        StringBuilder sb = new StringBuilder("id=");
        sb.append(id);
        if (!long_format) {
            if (store) {
                sb.append(" store src=");
                sb.append(transferRequest.getRemoteURL());
                sb.append(" dest=");
                sb.append(transferRequest.getPnfsPath());
            } else {
                sb.append(" restore src=");
                sb.append(transferRequest.getPnfsPath());
                sb.append(" dest=");
                sb.append(transferRequest.getRemoteURL());
            }
            return sb.toString();
        }
        sb.append("\n  state=").append(state);
        sb.append("\n  user=").append(transferRequest.getSubject());
        if (pnfsId != null) {
            sb.append("\n   pnfsId=").append(pnfsId);
        }
        if (fileAttributes != null) {
            sb.append("\n   fileAttributes=").append(fileAttributes);
        }
        if (pool != null) {
            sb.append("\n   pool=").append(pool);
            if (moverId != null) {
                sb.append("\n   moverId=").append(moverId);
            }
        }
        return sb.toString();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String getPool()
    {
        return pool;
    }

    public void setPool(String pool)
    {
        this.pool = pool;
    }

    public void setPoolAddress(CellAddressCore poolAddress)
    {
        this.poolAddress = poolAddress;
    }

    public void killMover(int moverId)
    {
        log.warn("sending mover kill to pool {} for moverId={}", pool, moverId);
        PoolMoverKillMessage killMessage = new PoolMoverKillMessage(pool, moverId);
        killMessage.setReplyRequired(false);
        try {
            manager.getPoolStub().notify(new CellPath(poolAddress), killMessage);
        } catch (NoRouteToCellException e) {
            log.error(e.toString());
        }
    }

    public void setState(int istate)
    {
        this.state = istate;
        TransferManagerHandlerState ts = new TransferManagerHandlerState(this, null);
        manager.persist(ts);
    }

    public void setState(int istate, Object errorObject)
    {
        this.state = istate;
        TransferManagerHandlerState ts = new TransferManagerHandlerState(this, errorObject);
        manager.persist(ts);
    }

    public void setMoverId(Integer moverid)
    {
        moverId = moverid;
    }

    public String getPnfsPath()
    {
        return pnfsPath;
    }

    public boolean getStore()
    {
        return store;
    }

    public boolean getCreated()
    {
        return created;
    }

    public boolean getLocked()
    {
        return locked;
    }

    public String getPnfsIdString()
    {
        return pnfsIdString;
    }

    public String getRemoteUrl()
    {
        return remoteUrl;
    }

    public int getState()
    {
        return state;
    }

    public long getId()
    {
        return id;
    }

    public Integer getMoverId()
    {
        return moverId;
    }

    public long getCreationTime()
    {
        return creationTime;
    }

    public long getLifeTime()
    {
        return lifeTime;
    }

    public Long getCredentialId()
    {
        return credentialId;
    }
}
