package diskCacheV111.services;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Executor;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileIsNewCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.Pool;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.transferManager.TransferCompleteMessage;
import diskCacheV111.vehicles.transferManager.TransferFailedMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferStatusQueryMessage;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageReply;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static org.dcache.namespace.FileAttribute.*;

public class TransferManagerHandler extends AbstractMessageCallback<Message>
{
    private static final Logger log =
            LoggerFactory.getLogger(TransferManagerHandler.class);

    private static final int MAXIMUM_POOL_SELECTION_ATTEMPTS = 10;
    private static final int MAXIMUM_MOVER_START_ATTEMPTS = 10;

    private final TransferManager manager;
    private final TransferManagerMessage transferRequest;
    private final CellPath requestor;
    private String pnfsPath;
    private transient String parentDir;
    boolean store;
    boolean created;
    private PnfsId pnfsId;
    private String pnfsIdString;
    private String remoteUrl;
    transient boolean locked;
    private Pool pool;
    private FileAttributes fileAttributes;
    public static final int INITIAL_STATE = 0;
    public static final int WAITING_FOR_PNFS_INFO_STATE = 1;
    public static final int RECEIVED_PNFS_INFO_STATE = 2;
    public static final int WAITING_FOR_CREATED_FILE_INFO_STATE = 3;
    public static final int RECEIVED_CREATED_FILE_INFO_STATE = 4;
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
    public static final int UNKNOWN_ID = -3;

    public static final Map<Integer,String> STATE_DESCRIPTION =
            ImmutableMap.<Integer,String>builder()
            .put(INITIAL_STATE, "initialising")
            .put(WAITING_FOR_PNFS_INFO_STATE, "querying file metadata")
            .put(RECEIVED_PNFS_INFO_STATE, "recieved file metadata")
            .put(WAITING_FOR_CREATED_FILE_INFO_STATE, "querying created file metadata")
            .put(RECEIVED_CREATED_FILE_INFO_STATE, "received created file metadata")
            .put(WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE, "creating namespace entry")
            .put(RECEIVED_PNFS_ENTRY_CREATION_INFO_STATE, "namespace entry created")
            .put(WAITING_FOR_POOL_INFO_STATE, "selecting pool")
            .put(RECEIVED_POOL_INFO_STATE, "pool selected")
            .put(WAITING_FIRST_POOL_REPLY_STATE, "waiting for transfer to start")
            .put(RECEIVED_FIRST_POOL_REPLY_STATE, "transfer has started")
            .put(WAITING_FOR_SPACE_INFO_STATE, "reserving space")
            .put(RECEIVED_SPACE_INFO_STATE, "space reserved")
            .put(WAITING_FOR_PNFS_ENTRY_DELETE, "requesting file deletion")
            .put(RECEIVED_PNFS_ENTRY_DELETE, "notified of file deletion")
            .put(WAITING_FOR_PNFS_CHECK_BEFORE_DELETE_STATE, "checking before file deletion")
            .put(RECEIVED_PNFS_CHECK_BEFORE_DELETE_STATE, "confirmed file deletion OK")
            .put(SENT_ERROR_REPLY_STATE, "transfer failed")
            .put(SENT_SUCCESS_REPLY_STATE, "transfer succeeded")
            .put(UNKNOWN_ID, "unknown transfer")
            .build();
    public int state = INITIAL_STATE;
    private long id;
    private Integer moverId;
    private IpProtocolInfo protocol_info;
    private long creationTime;
    private long lifeTime;
    private Long credentialId;
    private transient int numberOfRetries;
    private transient int numberOfPoolSelectionRetries;
    private transient int numberOfMoverStartRetries;
    private transient int _replyCode;
    private transient Serializable _errorObject;
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

        info = new DoorRequestInfoMessage(manager.getCellAddress());
        info.setTransactionDuration(-creationTime);
        info.setSubject(subject);
        info.setBillingPath(pnfsPath);
        info.setTransferPath(pnfsPath);
        info.setTimeQueued(-System.currentTimeMillis());
        this.requestor = requestor;
        try {
            info.setClient(new URI(transferRequest.getRemoteURL()).getHost());
        } catch (Exception e) {
        }

        manager.addActiveTransfer(id, this);
        setState(INITIAL_STATE);
        permissionHandler =
                new ChainedPermissionHandler(
                new ACLPermissionHandler(),
                new PosixPermissionHandler());
        pnfsId = transferRequest.getPnfsId();
    }

    public static String describeState(int state)
    {
        String description = STATE_DESCRIPTION.get(state);
        return description != null ? description : ("Unknown state: " + state);
    }

    public void handle()
    {
        log.debug("handling:  {}", toString(true));
        int last_slash_pos = pnfsPath.lastIndexOf('/');
        if (last_slash_pos == -1) {
            transferRequest.setFailed(2,
                    new IOException("pnfsFilePath is not absolute:" + pnfsPath));
            return;
        }
        parentDir = pnfsPath.substring(0, last_slash_pos);
        PnfsMessage message;
        if (store) {
            if (pnfsId == null) {
                message = new PnfsCreateEntryMessage(pnfsPath, FileAttributes.ofFileType(FileType.REGULAR));
                message.setSubject(transferRequest.getSubject());
                message.setRestriction(transferRequest.getRestriction());
                setState(WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE);
            } else {
                info.setPnfsId(pnfsId);
                pnfsIdString = pnfsId.toString();
                EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
                attributes.addAll(permissionHandler.getRequiredAttributes());
                attributes.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());
                message = new PnfsGetFileAttributes(pnfsId, attributes);
                message.setSubject(transferRequest.getSubject());
                message.setRestriction(transferRequest.getRestriction());
                message.setPnfsPath(pnfsPath);
                setState(WAITING_FOR_CREATED_FILE_INFO_STATE);
            }
        } else {
            EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
            attributes.addAll(permissionHandler.getRequiredAttributes());
            attributes.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());
            attributes.add(SIZE); // to determine if file is currently being uploaded
            message = pnfsId == null ? new PnfsGetFileAttributes(pnfsPath, attributes)
                    : new PnfsGetFileAttributes(pnfsId, attributes);
            message.setSubject(transferRequest.getSubject());
            message.setRestriction(transferRequest.getRestriction());
            message.setAccessMask(EnumSet.of(AccessMask.READ_DATA));
            message.setPnfsPath(pnfsPath);
            setState(WAITING_FOR_PNFS_INFO_STATE);
        }
        manager.persist(this);
        CellStub.addCallback(manager.getPnfsManagerStub().send(message), this, executor);
    }

    @Override
    public void success(Message message)
    {
        try {
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
                } else if (state == WAITING_FOR_PNFS_CHECK_BEFORE_DELETE_STATE) {
                    state = RECEIVED_PNFS_CHECK_BEFORE_DELETE_STATE;
                    deletePnfsEntry();
                    return;
                } else if (state == WAITING_FOR_CREATED_FILE_INFO_STATE) {
                    state = RECEIVED_CREATED_FILE_INFO_STATE;
                    getFileAttributesArrived(attributesMessage);
                    return;
                }

                log.error(this.toString() + " got unexpected PnfsGetStorageInfoMessage "
                        + " : " + attributesMessage + " ; Ignoring");
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
                            deleteReply.getPnfsPath());
                    sendErrorReply();
                }
            }
            manager.persist(this);
        } catch (RuntimeException e) {
            log.error("Bug detected in transfermanager, please report this to <support@dCache.org>", e);
            failure(1, "Bug detected: " + e);
        }
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

        case WAITING_FOR_CREATED_FILE_INFO_STATE:
            sendErrorReply(rc, "Failed to lookup created namespace entry: " + error);
            break;

        case WAITING_FIRST_POOL_REPLY_STATE:
            switch (rc) {
            case CacheException.OUT_OF_DATE:
            case CacheException.POOL_DISABLED:
            case CacheException.FILE_NOT_IN_REPOSITORY:
            case CacheException.CANNOT_CREATE_MOVER:
                log.debug("Pool {} reported rc={}; retrying pool selection",
                        pool.getAddress(), rc);
                retryPoolSelection(rc, error);
                break;

            case CacheException.TIMEOUT:
                if (store) {
                    /* We don't know if a mover was actually started; therefore,
                     * we must fail the request.
                     */
                    sendErrorReply(CacheException.SELECTED_POOL_FAILED,
                            "Timeout waiting for transfer to start on "
                            + pool.getAddress());
                    break;
                }

                // FALL THROUGH

            default:
                if (numberOfMoverStartRetries++ < MAXIMUM_MOVER_START_ATTEMPTS) {
                    log.debug("Pool {} reported rc={}; scheduling another attempt to start mover",
                            pool.getAddress(), rc);
                    executor.execute(() -> {
                                try {
                                    Thread.sleep(1_000);
                                    startMoverOnThePool();
                                } catch (InterruptedException e) {
                                    sendErrorReply(rc, "Interrupted while waiting"
                                            + " to resend start mover on "
                                            + pool.getAddress());
                                }
                            });
                } else {
                    log.debug("Too many attempts to start mover on pool {},"
                            + " retrying pool selection", pool.getAddress());
                    retryPoolSelection(rc, error);
                }
                break;
            }
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

    private void retryPoolSelection(int rc, Object error)
    {
        if (numberOfPoolSelectionRetries++ < MAXIMUM_POOL_SELECTION_ATTEMPTS) {
            numberOfMoverStartRetries = 0;
            selectPool();
        } else {
            sendErrorReply(rc, "Too many attempts to select pool; last pool "
                    + pool.getAddress() + " failed with " + error);
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
        if (create.getFileAttributes().isDefined(STORAGEINFO) && create.getFileAttributes().getStorageInfo().getKey("path") != null) {
            info.setBillingPath(create.getFileAttributes().getStorageInfo().getKey("path"));
        }

        selectPool();
    }

    public void getFileAttributesArrived(PnfsGetFileAttributes msg)
    {
        manager.persist(this);

        fileAttributes = msg.getFileAttributes();
        info.setStorageInfo(msg.getFileAttributes().getStorageInfo());
        if (msg.getFileAttributes().isDefined(STORAGEINFO) && msg.getFileAttributes().getStorageInfo().getKey("path") != null) {
            info.setBillingPath(msg.getFileAttributes().getStorageInfo().getKey("path"));
        }

        selectPool();
    }

    public void storageInfoArrived(PnfsGetFileAttributes msg)
    {
        if (!msg.getFileAttributes().isDefined(SIZE)) {
            sendErrorReply(CacheException.FILE_IS_NEW, new FileIsNewCacheException());
            return;
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
        request.setBillingPath(pnfsPath);
        request.setSubject(transferRequest.getSubject());
        log.debug("PoolMgrSelectPoolMsg: {}", request);
        setState(WAITING_FOR_POOL_INFO_STATE);
        manager.persist(this);
        CellStub.addCallback(manager.getPoolManagerStub().sendAsync(request), this, executor);
    }

    public void poolInfoArrived(PoolMgrSelectPoolMsg pool_info)
    {
        log.debug("poolManagerReply = {}", pool_info);

        if (pool_info instanceof PoolMgrSelectReadPoolMsg) {
            _readPoolSelectionContext =
                    ((PoolMgrSelectReadPoolMsg) pool_info).getContext();
        }

        setPool(pool_info.getPool());
        fileAttributes = pool_info.getFileAttributes();
        manager.persist(this);
        log.debug("Positive reply from pool {}", pool);
        startMoverOnThePool();
    }

    public void startMoverOnThePool()
    {
        PoolIoFileMessage poolMessage = store
                ? new PoolAcceptFileMessage(
                pool.getName(),
                protocol_info,
                fileAttributes,
                pool.getAssumption(),
                OptionalLong.empty())
                : new PoolDeliverFileMessage(
                pool.getName(),
                protocol_info,
                fileAttributes,
                pool.getAssumption());
        poolMessage.setBillingPath(info.getBillingPath());
        poolMessage.setTransferPath(info.getTransferPath());
        poolMessage.setSubject(transferRequest.getSubject());
        if (manager.getIoQueueName() != null) {
            poolMessage.setIoQueueName(manager.getIoQueueName());
        }
        poolMessage.setInitiator(info.getTransaction());
        poolMessage.setId(id);
        setState(WAITING_FIRST_POOL_REPLY_STATE);
        manager.persist(this);
        CellStub.addCallback(manager.getPoolManagerStub().startAsync(pool.getAddress(), poolMessage), this, executor);
    }

    public void poolFirstReplyArrived(PoolIoFileMessage poolMessage)
    {
        log.debug("poolReply = {}", poolMessage);
        info.setTimeQueued(info.getTimeQueued() + System.currentTimeMillis());
        log.debug("Pool {} will deliver file {} mover id is {}", pool, pnfsId, poolMessage.getMoverId());
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
            PnfsGetFileAttributes message = new PnfsGetFileAttributes(pnfsPath, EnumSet.noneOf(FileAttribute.class));
            setState(WAITING_FOR_PNFS_CHECK_BEFORE_DELETE_STATE);
            CellStub.addCallback(manager.getPnfsManagerStub().send(message), this, executor);
        }
    }

    public void poolDoorMessageArrived(DoorTransferFinishedMessage doorMessage)
    {
        log.debug("poolDoorMessageArrived, doorMessage.getReturnCode()={}", doorMessage.getReturnCode());
        if (doorMessage.getReturnCode() != 0) {
            sendErrorReply(CacheException.THIRD_PARTY_TRANSFER_FAILED,
                    doorMessage.getErrorObject());
            return;
        }
        sendSuccessReply();
    }

    private void sendErrorReply(int replyCode, Serializable errorObject)
    {
        _replyCode = replyCode;
        _errorObject = errorObject;

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
        manager.stopTimer(id);

        if (store) {
            synchronized (manager.justRequestedIDs) {
                manager.justRequestedIDs.remove(pnfsId);
            }
        }
        manager.finishTransfer();
        try {
            TransferFailedMessage errorReply = new TransferFailedMessage(transferRequest, replyCode, errorObject);
            manager.sendMessage(new CellMessage(requestor, errorReply));
        } catch (RuntimeException e) {
            log.error(e.toString());
            //can not do much more here!!!
        }
        //this will allow the handler to be garbage collected
        // once we sent a response
        manager.removeActiveTransfer(id);
    }

    private void sendErrorReply()
    {
        int replyCode = _replyCode;
        Serializable errorObject = _errorObject;

        if (log.isDebugEnabled()) {
            log.debug("sending error reply {}:{} for {}", replyCode,
                    errorObject, toString(true));
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
        manager.stopTimer(id);

        if (store) {
            synchronized (manager.justRequestedIDs) {
                manager.justRequestedIDs.remove(pnfsId);
            }
        }
        manager.finishTransfer();
        try {
            TransferFailedMessage errorReply = new TransferFailedMessage(transferRequest, replyCode, errorObject);
            manager.sendMessage(new CellMessage(requestor, errorReply));
        } catch (RuntimeException e) {
            log.error(e.toString());
            //can not do much more here!!!
        }
        //this will allow the handler to be garbage collected
        // once we sent a response
        manager.removeActiveTransfer(id);
    }

    public void sendSuccessReply()
    {
        log.debug("sendSuccessReply for: {}", toString(true));
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
        try {
            TransferCompleteMessage errorReply = new TransferCompleteMessage(transferRequest);
            manager.sendMessage(new CellMessage(requestor, errorReply));
        } catch (RuntimeException e) {
            log.error(e.toString());
            //can not do much more here!!!
        }
        //this will allow the handler to be garbage collected
        // once we sent a response
        manager.removeActiveTransfer(id);
    }

    /**
     * Sends status information to the biling cell.
     */
    void sendDoorRequestInfo(int code, String msg)
    {
        info.setResult(code, msg);
        log.debug("Sending info: {}", info);
        manager.getBillingStub().notify(info);
    }

    public void timeout()
    {
        if (moverId != null) {
            killMover(moverId, "timed out");
        }
        sendErrorReply(24, new IOException("timed out while waiting for mover reply"));
    }

    public void cancel(String explanation)
    {
        log.debug("transfer cancelled: {}", explanation);

        if (moverId != null) {
            killMover(moverId, explanation);
        }

        // FIXME: sending the reply here removes the TransferManagerHandler
        // from the set of active transfers.  This triggers an error
        // when the pool's DoorTransferFinishedMessage is received since
        // TransferManager now cannot find the corresponding
        // TransferManagerHandler message.
        sendErrorReply(24, new IOException("transfer cancelled: " + explanation));
    }

    public synchronized String toString(boolean isLongFormat)
    {
        String src = store ? transferRequest.getRemoteURL() : transferRequest.getPnfsPath();
        String dest = store ? transferRequest.getPnfsPath() : transferRequest.getRemoteURL();
        String siPath = fileAttributes == null ? null : fileAttributes.getStorageInfo().getKey("path");

        StringBuilder sb = new StringBuilder("id: ").append(id);

        if (isLongFormat) {
            sb.append('\n');
            sb.append("    Source: ").append(src).append('\n');
            sb.append("    Destination: ").append(dest).append('\n');
            if (store && siPath != null && !siPath.equals(dest)) {
                sb.append("    Final destination: ").append(siPath).append('\n');
            }
            sb.append("    State: ").append(describeState(state)).append('\n');
            sb.append("    User: ").append(Subjects.getDisplayName(transferRequest.getSubject())).append('\n');
            sb.append("    Restriction: ").append(transferRequest.getRestriction()).append('\n');
            if (pnfsId != null) {
                sb.append("    PNFS-ID: ").append(pnfsId).append('\n');
            }
            if (fileAttributes != null) {
                if (fileAttributes.isDefined(ACCESS_LATENCY)) {
                    sb.append("    Access latency: ").append(fileAttributes.getAccessLatency()).append('\n');
                }
                if (fileAttributes.isDefined(RETENTION_POLICY)) {
                    sb.append("    Retention policy: ").append(fileAttributes.getRetentionPolicy()).append('\n');
                }
                if (fileAttributes.isDefined(STORAGECLASS)) {
                    sb.append("    Storage class: ").append(fileAttributes.getStorageClass()).append('\n');
                }
                if (fileAttributes.isDefined(SIZE)) {
                    sb.append("    Size: ").append(fileAttributes.getSize()).append('\n');
                }
            }
            sb.append("    Pool: ").append(pool == null ? "not yet selected" : pool);
            if (moverId != null) {
                sb.append('\n');
                sb.append("    Mover: ").append(moverId);
            }
        } else {
            sb.append(' ').append(src).append(" --> ").append(dest);

            if (store && siPath != null && !siPath.equals(dest)) {
                sb.append(" [").append(siPath).append("]");
            }
        }

        return sb.toString();
    }

    public boolean isMoverActive()
    {
        return state == RECEIVED_FIRST_POOL_REPLY_STATE;
    }

    public Object appendInfo(final TransferStatusQueryMessage message)
    {
        message.setState(state);

        if (!isMoverActive()) {
            return message;
        }

        final MessageReply<TransferStatusQueryMessage> reply = new MessageReply<>();

        final ListenableFuture<IoJobInfo> future = manager.getPoolStub().
                send(new CellPath(pool.getAddress()), "mover ls -binary " + moverId,
                IoJobInfo.class, 30_000);
        Futures.addCallback(future, new FutureCallback<IoJobInfo>()
        {
            @Override
            public void onSuccess(IoJobInfo info)
            {
                message.setMoverInfo(info);
                reply.reply(message);
            }

            @Override
            public void onFailure(Throwable e)
            {
                reply.fail(message, CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                        "failed to query pool " + pool.getName() + ": " + e.getMessage());
            }
        }, MoreExecutors.directExecutor());

        return reply;
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public Pool getPool()
    {
        return pool;
    }

    public void setPool(Pool pool)
    {
        this.pool = pool;
    }

    public void killMover(int moverId, String explanation)
    {
        log.debug("sending mover kill to pool {} for moverId={}", pool, moverId);
        PoolMoverKillMessage killMessage = new PoolMoverKillMessage(pool.getName(), moverId,
                "killed by TransferManagerHandler: " + explanation);
        killMessage.setReplyRequired(false);
        manager.getPoolStub().notify(new CellPath(pool.getAddress()), killMessage);
    }

    public void setState(int istate)
    {
        this.state = istate;
    }

    public void setState(int istate, Object errorObject)
    {
        this.state = istate;
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
