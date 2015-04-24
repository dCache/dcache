// -*- c-basic-offset: 8; -*-
//______________________________________________________________________________
//
// Space Manager - cell that handles space reservation management in SRM
//                 essentially a layer on top of a database
//
// there are three essential tables:
//
//      +------------+  +--------+  +------------+
//      |srmlinkgroup|-<|srmspace|-<|srmspacefile|
//      +------------+  +--------+  +------------+
// srmlinkgroup contains field that caches sum(size-usedsize) of all space
// reservations belonging to the linkgroup. Field is called reservedspaceinbytes
//
// srmspace  contains fields that caches sum(size) of all files from srmspace
// that belong to this space reservation. Fields are usedspaceinbytes
//  (for files in state STORED) and allocatespaceinbytes
//  (for files in states TRANSFERRING)
//
// each time a space reservation is added/removed , reservedspaceinbytes in
// srmlinkgroup is updated
//
// each time a file is added/removed, usedspaceinbytes, allocatespaceinbytes and
// reservedspaceinbytes are updated depending on file state
//
//                                    Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________
package diskCacheV111.services.space;

import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.annotation.Transactional;

import javax.security.auth.Subject;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.services.space.message.GetLinkGroupNamesMessage;
import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.services.space.message.Release;
import diskCacheV111.services.space.message.Reserve;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.VOInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.auth.FQAN;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.Subjects;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.BoundedExecutor;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;

public final class SpaceManagerService
        extends AbstractCellComponent
        implements CellCommandListener,
        CellMessageReceiver,
        Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SpaceManagerService.class);

    private long expireSpaceReservationsPeriod;

    private Thread expireSpaceReservations;

    private boolean shouldDeleteStoredFileRecord;
    private boolean allowUnreservedUploadsToLinkGroups;
    private boolean shouldReturnFlushedSpaceToReservation;
    private boolean isSpaceManagerEnabled;

    private CellPath poolManager;
    private PnfsHandler pnfs;

    private SpaceManagerAuthorizationPolicy authorizationPolicy;

    private ExecutorService executor;

    private PoolMonitor poolMonitor;
    private SpaceManagerDatabase db;
    private LinkGroupLoader linkGroupLoader;
    private long perishedSpacePurgeDelay;
    private int threads;
    private volatile boolean isStopped;

    @Required
    public void setPoolManager(CellPath poolManager)
    {
        this.poolManager = poolManager;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        this.pnfs = pnfs;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor)
    {
        this.poolMonitor = poolMonitor;
    }

    @Required
    public void setSpaceManagerEnabled(boolean enabled)
    {
        this.isSpaceManagerEnabled = enabled;
    }

    @Required
    public void setExpireSpaceReservationsPeriod(long expireSpaceReservationsPeriod)
    {
        this.expireSpaceReservationsPeriod = expireSpaceReservationsPeriod;
    }

    @Required
    public void setAllowUnreservedUploadsToLinkGroups(boolean allowUnreservedUploadsToLinkGroups)
    {
        this.allowUnreservedUploadsToLinkGroups = allowUnreservedUploadsToLinkGroups;
    }

    @Required
    public void setShouldDeleteStoredFileRecord(boolean shouldDeleteStoredFileRecord)
    {
        this.shouldDeleteStoredFileRecord = shouldDeleteStoredFileRecord;
    }

    @Required
    public void setShouldReturnFlushedSpaceToReservation(boolean shouldReturnFlushedSpaceToReservation)
    {
        this.shouldReturnFlushedSpaceToReservation = shouldReturnFlushedSpaceToReservation;
    }

    @Required
    public void setMaxThreads(int threads)
    {
        this.threads = threads;
    }

    @Required
    public void setExecutor(ExecutorService executor)
    {
        this.executor = executor;
    }

    @Required
    public void setDatabase(SpaceManagerDatabase db)
    {
        this.db = db;
    }

    @Required
    public void setAuthorizationPolicy(SpaceManagerAuthorizationPolicy authorizationPolicy)
    {
        this.authorizationPolicy = authorizationPolicy;
    }

    @Required
    public void setLinkGroupLoader(LinkGroupLoader linkGroupLoader)
    {
        this.linkGroupLoader = linkGroupLoader;
    }

    @Required
    public void setPerishedSpacePurgeDelay(long millis)
    {
        this.perishedSpacePurgeDelay = millis;
    }

    public void start()
    {
        executor = new CDCExecutorServiceDecorator<>(new BoundedExecutor(executor, threads));
        (expireSpaceReservations = new Thread(this, "ExpireThreadReservations")).start();
    }

    public void stop() throws InterruptedException
    {
        try {
            isStopped = true;
            executor.shutdown();
            if (expireSpaceReservations != null) {
                expireSpaceReservations.interrupt();
                expireSpaceReservations.join();
            }
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } finally {
            for (Runnable runnable : executor.shutdownNow()) {
                notifyShutdown(((FibonacciBackoffMessageProcessor) runnable).getEnvelope());
            }
        }
    }


    @Override
    public void getInfo(PrintWriter printWriter)
    {
        printWriter.println("isSpaceManagerEnabled=" + isSpaceManagerEnabled);
        printWriter.println("expireSpaceReservationsPeriod="
                            + expireSpaceReservationsPeriod);
        printWriter.println("shouldDeleteStoredFileRecord="
                            + shouldDeleteStoredFileRecord);
        printWriter.println("allowUnreservedUploadsToLinkGroups="
                            + allowUnreservedUploadsToLinkGroups);
        printWriter.println("shouldReturnFlushedSpaceToReservation="
                            + shouldReturnFlushedSpaceToReservation);
    }

    private void expireSpaceReservations() throws DataAccessException
    {
        LOGGER.trace("expireSpaceReservations()...");

        /* Recover files from lost notifications. Space manager receives notifications
         * on file transfer finishing (DoorTransferFinished), name space deletion
         * (PnfsDeleteEntryNotification), pool location cleaning (PoolRemoveFiles),
         * and flush (PoolFileFlushed). These notifications are however lossy, and we
         * need to recover from lost messages in some way.
         *
         * We ought to do this with any kind of file reservation, but that would be rather
         * expensive. This code is currently limited to reservations in the TRANSFERRING
         * state: It is easy to miss the notification for such files if space manager
         * or some other critical component gets restarted while having active uploads.
         */
        SpaceManagerDatabase.FileCriterion oldTransfers = db.files()
                .whereStateIsIn(FileState.TRANSFERRING)
                .whereCreationTimeIsBefore(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
        final int maximumNumberFilesToLoadAtOnce = 1000;
        for (File file : db.get(oldTransfers, maximumNumberFilesToLoadAtOnce)) {
            try {
                EnumSet<FileAttribute> attributes =
                        EnumSet.of(FileAttribute.TYPE,
                                   FileAttribute.SIZE,
                                   FileAttribute.LOCATIONS,
                                   FileAttribute.STORAGEINFO,
                                   FileAttribute.ACCESS_LATENCY);
                FileAttributes fileAttributes = pnfs.getFileAttributes(file.getPnfsId(), attributes);
                if (fileAttributes.getFileType() != FileType.REGULAR) {
                    db.removeFile(file.getId());
                } else if (fileAttributes.getStorageInfo().isStored()) {
                    boolean isRemovable = !fileAttributes.getAccessLatency().equals(AccessLatency.ONLINE);
                    fileFlushed(file.getPnfsId(), fileAttributes.getSize(), isRemovable);
                } else if (!fileAttributes.getLocations().isEmpty()) {
                    transferFinished(file.getPnfsId(), fileAttributes.getSize());
                }
            } catch (FileNotFoundCacheException e) {
                db.removeFile(file.getId());
            } catch (TransientDataAccessException e) {
                LOGGER.warn("Transient data access failure while deleting expired file {}: {}",
                            file, e.getMessage());
            } catch (DataAccessException e) {
                LOGGER.error("Data access failure while deleting expired file {}: {}",
                             file, e.getMessage());
                break;
            } catch (TimeoutCacheException e) {
                LOGGER.error("Failed to lookup file {} in name space: {}", file.getPnfsId(), e.getMessage());
                break;
            } catch (CacheException e) {
                LOGGER.error("Failed to lookup file {} in name space: {}", file.getPnfsId(), e.getMessage());
            }
        }

        db.expire(db.spaces()
                          .whereStateIsIn(SpaceState.RESERVED)
                          .thatExpireBefore(System.currentTimeMillis()));
        db.remove(db.files()
                          .whereStateIsIn(FileState.STORED, FileState.FLUSHED)
                          .in(db.spaces()
                                      .whereStateIsIn(SpaceState.EXPIRED, SpaceState.RELEASED)
                                      .thatExpireBefore(
                                              System.currentTimeMillis() - perishedSpacePurgeDelay)));
        db.remove(db.spaces()
                          .whereStateIsIn(SpaceState.EXPIRED, SpaceState.RELEASED)
                          .thatHaveNoFiles());
    }

    private void getValidSpaceTokens(GetSpaceTokensMessage msg) throws DataAccessException
    {
        msg.setSpaceTokenSet(db.get(db.spaces().thatNeverExpire().whereStateIsIn(SpaceState.RESERVED), null));
    }

    private void getLinkGroups(GetLinkGroupsMessage msg) throws DataAccessException
    {
        msg.setLinkGroups(db.get(db.linkGroups()));
    }

    private void getLinkGroupNames(GetLinkGroupNamesMessage msg) throws DataAccessException
    {
        msg.setLinkGroupNames(newArrayList(transform(db.get(db.linkGroups()), LinkGroup::getName)));
    }

    /**
     * Returns true if message is of a type processed exclusively by SpaceManager
     */
    private boolean isSpaceManagerMessage(Message message)
    {
        return message instanceof Reserve
               || message instanceof GetSpaceTokensMessage
               || message instanceof GetLinkGroupsMessage
               || message instanceof GetLinkGroupNamesMessage
               || message instanceof Release
               || message instanceof GetSpaceMetaData
               || message instanceof GetSpaceTokens
               || message instanceof ExtendLifetime
               || message instanceof GetFileSpaceTokensMessage;
    }

    /**
     * Returns true if message is a notification to which SpaceManager subscribes
     */
    private boolean isNotificationMessage(Message message)
    {
        return message instanceof PoolFileFlushedMessage
               || message instanceof PnfsDeleteEntryNotificationMessage;
    }

    /**
     * Returns true if message is of a type that needs processing by SpaceManager even if
     * SpaceManager is not the intended final destination.
     */
    private boolean isInterceptedMessage(Message message)
    {
        return (message instanceof PoolMgrSelectWritePoolMsg && !message.isReply())
               || message instanceof DoorTransferFinishedMessage
               || (message instanceof PoolAcceptFileMessage && ((PoolAcceptFileMessage) message).getFileAttributes().getStorageInfo().getKey(
                "LinkGroupId") != null && (!message.isReply() || message.getReturnCode() != 0));
    }

    /**
     * Returns true if message should not be discarded during shutdown.
     */
    private boolean isImportantMessage(Message message)
    {
        return message.isReply() ||
               message instanceof PoolFileFlushedMessage ||
               message instanceof DoorTransferFinishedMessage;
    }

    public void messageArrived(final CellMessage envelope,
                               final Message message)
    {
        LOGGER.trace("messageArrived : type={} value={} from {}",
                     message.getClass().getName(), message, envelope.getSourcePath());

        if (!message.isReply()) {
            if (!isNotificationMessage(message) && !isSpaceManagerMessage(message)) {
                messageToForward(envelope, message);
            } else if (isSpaceManagerEnabled) {
                executor.execute(new FibonacciBackoffMessageProcessor(executor, envelope)
                {
                    @Override
                    public void process() throws DeadlockLoserDataAccessException
                    {
                        if (!isStopped || isImportantMessage(message)) {
                            processMessage(message);
                            if (message.getReplyRequired()) {
                                returnMessage(envelope);
                            }
                        } else {
                            notifyShutdown(envelope);
                        }
                    }
                });
            } else if (message.getReplyRequired()) {
                message.setReply(1, "Space manager is disabled in configuration");
                returnMessage(envelope);
            }
        }
    }

    public void messageToForward(final CellMessage envelope, final Message message)
    {
        LOGGER.trace("messageToForward: type={} value={} from {} going to {}",
                     message.getClass().getName(),
                     message,
                     envelope.getSourcePath(),
                     envelope.getDestinationPath());

        final boolean isEnRouteToDoor = message.isReply() || message instanceof DoorTransferFinishedMessage;
        if (!isEnRouteToDoor) {
            envelope.getDestinationPath().insert(poolManager);
        }

        if (envelope.nextDestination()) {
            if (isSpaceManagerEnabled && isInterceptedMessage(message)) {
                executor.execute(new FibonacciBackoffMessageProcessor(executor, envelope)
                {
                    @Override
                    public void process() throws DeadlockLoserDataAccessException
                    {
                        if (!isStopped || isImportantMessage(message)) {
                            processMessage(message);
                            if (message.getReturnCode() != 0 && !isEnRouteToDoor) {
                                envelope.revertDirection();
                            }
                        } else {
                            notifyShutdown(envelope);
                        }

                        forwardMessage(envelope, isEnRouteToDoor);
                    }
                });
            } else {
                forwardMessage(envelope, isEnRouteToDoor);
            }
        }
    }

    private void notifyShutdown(CellMessage envelope)
    {
        envelope.setMessageObject(new NoRouteToCellException(envelope, "Space manager is shutting down."));
        returnMessage(envelope);
    }

    private void returnMessage(CellMessage envelope)
    {
        envelope.revertDirection();
        sendMessage(envelope);
    }

    private void forwardMessage(CellMessage envelope, boolean isEnRouteToDoor)
    {
        sendMessage(envelope);
    }

    private void processMessage(Message message) throws DeadlockLoserDataAccessException
    {
        try {
            boolean isSuccessful = false;
            int attempts = 0;
            while (!isSuccessful) {
                try {
                    processMessageTransactionally(message);
                    isSuccessful = true;
                } catch (DeadlockLoserDataAccessException e) {
                    LOGGER.debug("Transaction lost deadlock race and will be retried: {}", e.getMessage());
                    throw e;
                } catch (TransientDataAccessException | RecoverableDataAccessException e) {
                    if (attempts >= 3) {
                        throw e;
                    }
                    LOGGER.warn("Retriable data access error: {}", e.toString());
                    attempts++;
                }
            }
        } catch (SpaceAuthorizationException e) {
            message.setFailedConditionally(CacheException.PERMISSION_DENIED, e);
        } catch (NoFreeSpaceException e) {
            message.setFailedConditionally(CacheException.RESOURCE, e);
        } catch (SpaceException e) {
            message.setFailedConditionally(CacheException.INVALID_ARGS, e);
        } catch (IllegalArgumentException e) {
            LOGGER.error("Message processing failed: {}", e.getMessage(), e);
            message.setFailedConditionally(CacheException.INVALID_ARGS, e.getMessage());
        } catch (DeadlockLoserDataAccessException e) {
            throw e;
        } catch (DataAccessException e) {
            LOGGER.error("Message processing failed: {}", e.toString());
            message.setFailedConditionally(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                           "Internal failure during space management");
        } catch (RuntimeException e) {
            LOGGER.error("Message processing failed: {}", e.getMessage(), e);
            message.setFailedConditionally(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                           "Internal failure during space management");
        }
    }

    @Transactional(rollbackFor = {SpaceException.class})
    private void processMessageTransactionally(Message message) throws SpaceException
    {
        if (message instanceof PoolMgrSelectWritePoolMsg) {
            selectPool((PoolMgrSelectWritePoolMsg) message);
        } else if (message instanceof PoolAcceptFileMessage) {
            PoolAcceptFileMessage poolRequest = (PoolAcceptFileMessage) message;
            if (message.isReply()) {
                transferStarted(poolRequest.getPnfsId(), poolRequest.getReturnCode() == 0);
            } else {
                transferStarting(poolRequest);
            }
        } else if (message instanceof DoorTransferFinishedMessage) {
            transferFinished((DoorTransferFinishedMessage) message);
        } else if (message instanceof Reserve) {
            reserveSpace((Reserve) message);
        } else if (message instanceof GetSpaceTokensMessage) {
            getValidSpaceTokens((GetSpaceTokensMessage) message);
        } else if (message instanceof GetLinkGroupsMessage) {
            getLinkGroups((GetLinkGroupsMessage) message);
        } else if (message instanceof GetLinkGroupNamesMessage) {
            getLinkGroupNames((GetLinkGroupNamesMessage) message);
        } else if (message instanceof Release) {
            releaseSpace((Release) message);
        } else if (message instanceof GetSpaceMetaData) {
            getSpaceMetaData((GetSpaceMetaData) message);
        } else if (message instanceof GetSpaceTokens) {
            getSpaceTokens((GetSpaceTokens) message);
        } else if (message instanceof ExtendLifetime) {
            extendLifetime((ExtendLifetime) message);
        } else if (message instanceof PoolFileFlushedMessage) {
            fileFlushed((PoolFileFlushedMessage) message);
        } else if (message instanceof GetFileSpaceTokensMessage) {
            getFileSpaceTokens((GetFileSpaceTokensMessage) message);
        } else if (message instanceof PnfsDeleteEntryNotificationMessage) {
            namespaceEntryDeleted((PnfsDeleteEntryNotificationMessage) message);
        } else {
            throw new RuntimeException(
                    "Unexpected " + message.getClass() + ": Please report this to support@dcache.org");
        }
    }

    @Override
    public void run()
    {
        try {
            while (true) {
                try {
                    expireSpaceReservations();
                } catch (DeadlockLoserDataAccessException e) {
                    LOGGER.debug("Expiration failed: {}", e.getMessage());
                } catch (TransientDataAccessException e) {
                    LOGGER.warn("Expiration failed: {}", e.getMessage());
                } catch (DataAccessException e) {
                    LOGGER.error("Expiration failed: {}", e.getMessage());
                } catch (Exception e) {
                    LOGGER.error("Expiration failed: {}", e.toString());
                }
                Thread.sleep(expireSpaceReservationsPeriod);
            }
        } catch (InterruptedException e) {
            LOGGER.trace("Expiration thread has terminated.");
        }
    }

    private void releaseSpace(Release release)
            throws DataAccessException, SpaceException
    {
        LOGGER.trace("releaseSpace({})", release);

        long spaceToken = release.getSpaceToken();
        Long spaceToReleaseInBytes = release.getReleaseSizeInBytes();
        if (spaceToReleaseInBytes != null) {
            throw new UnsupportedOperationException("partial release is not supported yet");
        }

        Space space = db.selectSpaceForUpdate(spaceToken);
        if (space.getState() == SpaceState.RELEASED) {
            /* Stupid way to signal that it isn't found, but there is no other way at the moment. */
            throw new EmptyResultDataAccessException("Space reservation " + spaceToken + " was already released.", 1);
        }
        Subject subject = release.getSubject();
        authorizationPolicy.checkReleasePermission(subject, space);
        space.setState(SpaceState.RELEASED);
        db.updateSpace(space);
    }

    private void reserveSpace(Reserve reserve)
            throws DataAccessException, SpaceException
    {
        Space space = reserveSpace(reserve.getSubject(),
                                   reserve.getSizeInBytes(),
                                   reserve.getAccessLatency(),
                                   reserve.getRetentionPolicy(),
                                   reserve.getLifetime(),
                                   reserve.getDescription());
        reserve.setSpaceToken(space.getId());
    }

    private void transferStarting(PoolAcceptFileMessage message) throws DataAccessException, SpaceException
    {
        LOGGER.trace("transferStarting({})", message);
        PnfsId pnfsId = checkNotNull(message.getPnfsId());
        FileAttributes fileAttributes = message.getFileAttributes();
        VOInfo owner = getVoInfo(message.getSubject());
        long sizeInBytes = message.getPreallocated();
        long spaceId;
        String spaceToken = fileAttributes.getStorageInfo().getKey("SpaceToken");
        if (spaceToken != null) {
            spaceId = Long.parseLong(spaceToken);
        } else {
            LOGGER.trace("transferStarting: file is not found, no prior reservations for this file");

            long lifetime = 1000 * 60 * 60;
            String description = null;

            long linkGroupId = Long.parseLong(fileAttributes.getStorageInfo().getKey("LinkGroupId"));
            Space space = db.insertSpace(owner.getVoGroup(),
                                         owner.getVoRole(),
                                         fileAttributes.getRetentionPolicy(),
                                         fileAttributes.getAccessLatency(),
                                         linkGroupId,
                                         sizeInBytes,
                                         lifetime,
                                         description,
                                         SpaceState.RESERVED,
                                         0,
                                         0);
            spaceId = space.getId();
        }
        db.insertFile(spaceId,
                      owner.getVoGroup(),
                      owner.getVoRole(),
                      sizeInBytes,
                      pnfsId,
                      FileState.TRANSFERRING);
    }

    private void transferStarted(PnfsId pnfsId, boolean success)
            throws DataAccessException
    {
        LOGGER.trace("transferStarted({},{})", pnfsId, success);
        if (!success) {
            db.remove(db.files().wherePnfsIdIs(pnfsId).whereStateIsIn(FileState.TRANSFERRING));

            /* TODO: If we also created the reservation, we should
             * release it at this point, but at the moment we cannot
             * know who created it. It will eventually expire
             * automatically.
             */
        }
    }

    private void transferFinished(DoorTransferFinishedMessage finished)
            throws DataAccessException
    {
        if (finished.getReturnCode() == CacheException.FILE_NOT_FOUND) {
            /* File is gone from name space so may as well get rid of it right away.
             */
            fileRemoved(finished.getPnfsId());
        } else {
            transferFinished(finished.getPnfsId(), finished.getFileAttributes().getSize());
        }
    }

    @Transactional
    private void transferFinished(PnfsId pnfsId, long size)
            throws DataAccessException
    {
        LOGGER.trace("transferFinished({})", pnfsId);
        File f;
        try {
            f = db.selectFileForUpdate(pnfsId);
        } catch (EmptyResultDataAccessException e) {
            LOGGER.trace("failed to find file {}: {}", pnfsId,
                         e.getMessage());
            return;
        }
        if (f.getState() != FileState.TRANSFERRING) {
            LOGGER.trace("transferFinished({}): file state={}",
                         pnfsId, f.getState());
        } else if (shouldDeleteStoredFileRecord) {
            LOGGER.trace("file transferred, deleting file record");
            db.removeFile(f.getId());
        } else {
            f.setSizeInBytes(size);
            f.setState(FileState.STORED);
            db.updateFile(f);
        }
    }

    private void fileFlushed(PoolFileFlushedMessage fileFlushed)
            throws DataAccessException
    {
        FileAttributes fileAttributes = fileFlushed.getFileAttributes();
        boolean isRemovable = !fileAttributes.getAccessLatency().equals(AccessLatency.ONLINE);
        fileFlushed(fileFlushed.getPnfsId(), fileAttributes.getSize(), isRemovable);
    }

    @Transactional
    private void fileFlushed(PnfsId pnfsId, long size, boolean isRemovable)
            throws DataAccessException
    {
        LOGGER.trace("fileFlushed({})", pnfsId);
        File f;
        try {
            f = db.selectFileForUpdate(pnfsId);
        } catch (EmptyResultDataAccessException e) {
            LOGGER.trace("failed to find file {}: {}", pnfsId, e.getMessage());
            return;
        }
        if (shouldDeleteStoredFileRecord) {
            /* A file must have been stored for it to be flushed. If we didn't do
             * it during DoorTransferFinished, we do it now.
             */
            db.removeFile(f.getId());
        } else if (f.getState() != FileState.FLUSHED) {
            if (shouldReturnFlushedSpaceToReservation && isRemovable) {
                f.setSizeInBytes(size);
                f.setState(FileState.FLUSHED);
                db.updateFile(f);
            } else if (f.getState() == FileState.TRANSFERRING) {
                /* A file must have been stored for it to be flushed. If we didn't do
                 * it during DoorTransferFinished, we do it now.
                 */
                f.setSizeInBytes(size);
                f.setState(FileState.STORED);
                db.updateFile(f);
            }
        }
    }

    @Transactional
    private void fileRemoved(PnfsId pnfsId)
    {
        LOGGER.trace("fileRemoved({})", pnfsId);
        db.remove(db.files().wherePnfsIdIs(pnfsId));
    }

    private Space reserveSpace(Subject subject,
                               long sizeInBytes,
                               AccessLatency latency,
                               RetentionPolicy policy,
                               long lifetime,
                               String description)
            throws DataAccessException, SpaceException
    {
        LOGGER.trace("reserveSpace(subject={}, sz={}, latency={}, policy={}, lifetime={}, description={})",
                     subject.getPrincipals(), sizeInBytes, latency, policy, lifetime, description);
        List<LinkGroup> linkGroups =
                db.get(db.linkGroups()
                               .allowsAccessLatency(latency)
                               .allowsRetentionPolicy(policy)
                               .hasAvailable(sizeInBytes)
                               .whereUpdateTimeAfter(linkGroupLoader.getLatestUpdateTime()))
                        .stream()
                        .sorted(Comparator.comparing(LinkGroup::getAvailableSpace).reversed())
                        .collect(toList());

        if (linkGroups.isEmpty()) {
            LOGGER.warn("Failed to find matching linkgroup for reservation request.");
            throw new NoFreeSpaceException("No space available.");
        }

        for (LinkGroup lg : linkGroups) {
            try {
                VOInfo voInfo = authorizationPolicy.checkReservePermission(subject, lg);
                if (latency == null) {
                    /* If a specific latency was not requested, we prefer nearline for
                     * custodial and online for other retention policies, but fall
                     * back to whatever is allowed by the link group if necessary.
                     */
                    if (policy == RetentionPolicy.CUSTODIAL) {
                        if (lg.isNearlineAllowed()) {
                            latency = AccessLatency.NEARLINE;
                        } else if (lg.isOnlineAllowed()) {
                            latency = AccessLatency.ONLINE;
                        } else {
                            continue;
                        }
                    } else {
                        if (lg.isOnlineAllowed()) {
                            latency = AccessLatency.ONLINE;
                        } else if (lg.isNearlineAllowed()) {
                            latency = AccessLatency.NEARLINE;
                        } else {
                            continue;
                        }
                    }
                }
                LOGGER.trace("Chose linkgroup {}", lg);
                return db.insertSpace(voInfo.getVoGroup(),
                                      voInfo.getVoRole(),
                                      policy,
                                      latency,
                                      lg.getId(),
                                      sizeInBytes,
                                      lifetime,
                                      description,
                                      SpaceState.RESERVED,
                                      0,
                                      0);
            } catch (SpaceAuthorizationException ignored) {
            }
        }
        LOGGER.warn("Failed to find linkgroup where user is authorized to reserve space.");
        throw new SpaceAuthorizationException("Failed to find LinkGroup where user is authorized to reserve space.");
    }

    private LinkGroup findLinkGroupForWrite(Subject subject, ProtocolInfo protocolInfo,
                                            FileAttributes fileAttributes, long size)
            throws DataAccessException
    {
        List<LinkGroup> linkGroups =
                db.get(db.linkGroups()
                               .allowsAccessLatency(fileAttributes.getAccessLatency())
                               .allowsRetentionPolicy(fileAttributes.getRetentionPolicy())
                               .hasAvailable(size)
                               .whereUpdateTimeAfter(linkGroupLoader.getLatestUpdateTime()))
                        .stream()
                        .sorted(Comparator.comparing(LinkGroup::getAvailableSpace).reversed())
                        .collect(toList());

        List<String> linkGroupNames = new ArrayList<>();
        for (LinkGroup linkGroup : linkGroups) {
            try {
                authorizationPolicy.checkReservePermission(subject, linkGroup);
                linkGroupNames.add(linkGroup.getName());
            } catch (SpaceAuthorizationException ignored) {
            }
        }
        linkGroupNames = findLinkGroupForWrite(protocolInfo, fileAttributes, linkGroupNames);
        LOGGER.trace("Found {} linkgroups protocolInfo={}, fileAttributes={}",
                     linkGroups.size(), protocolInfo, fileAttributes);

        if (!linkGroupNames.isEmpty()) {
            String linkGroupName = linkGroupNames.get(0);
            for (LinkGroup lg : linkGroups) {
                if (lg.getName().equals(linkGroupName)) {
                    return lg;
                }
            }
        }
        return null;
    }

    private List<String> findLinkGroupForWrite(ProtocolInfo protocolInfo, FileAttributes fileAttributes,
                                               Collection<String> linkGroups)
    {
        String protocol = protocolInfo.getProtocol() + '/' + protocolInfo.getMajorVersion();
        String hostName =
                (protocolInfo instanceof IpProtocolInfo)
                ? ((IpProtocolInfo) protocolInfo).getSocketAddress().getAddress().getHostAddress()
                : null;

        List<String> outputLinkGroups = new ArrayList<>(linkGroups.size());
        for (String linkGroup : linkGroups) {
            PoolPreferenceLevel[] level =
                    poolMonitor.getPoolSelectionUnit().match(PoolSelectionUnit.DirectionType.WRITE,
                                                             hostName,
                                                             protocol,
                                                             fileAttributes,
                                                             linkGroup);
            if (level.length > 0) {
                outputLinkGroups.add(linkGroup);
            }
        }
        return outputLinkGroups;
    }

    private VOInfo getVoInfo(Subject subject)
    {
        String effectiveGroup;
        String effectiveRole;
        String primaryFqan = Subjects.getPrimaryFqan(subject);
        String username = Subjects.getUserName(subject);
        if (primaryFqan != null) {
            FQAN fqan = new FQAN(primaryFqan);
            effectiveGroup = fqan.getGroup();
            effectiveRole = fqan.getRole();
        } else if (username != null) {
            effectiveGroup = Subjects.getUserName(subject);
            effectiveRole = null;
        } else {
            effectiveGroup = Long.toString(Subjects.getPrimaryGid(subject));
            effectiveRole = null;
        }
        return new VOInfo(effectiveGroup, effectiveRole);
    }

    /**
     * Called upon intercepting PoolMgrSelectWritePoolMsg requests.
     * <p>
     * Injects the link group name into the request message. Also adds SpaceToken and
     * LinkGroup flags to StorageInfo. These are accessed when space manager intercepts
     * the subsequent PoolAcceptFileMessage.
     */
    private void selectPool(PoolMgrSelectWritePoolMsg selectWritePool) throws DataAccessException, SpaceException
    {
        LOGGER.trace("selectPool({})", selectWritePool);
        FileAttributes fileAttributes = selectWritePool.getFileAttributes();
        String defaultSpaceToken = fileAttributes.getStorageInfo().getMap().get("writeToken");
        Subject subject = selectWritePool.getSubject();

        // REVISIT: look at moving this into Subjects.
        boolean hasIdentity = subject.getPrincipals().stream().anyMatch(p ->
                p instanceof FQANPrincipal ||
                p instanceof UserNamePrincipal ||
                p instanceof GidPrincipal);

        if (defaultSpaceToken != null) {
            LOGGER.trace("selectPool: file is not " +
                         "found, found default space " +
                         "token, calling insertFile()");
            Space space;
            try {
                space = db.getSpace(Long.parseLong(defaultSpaceToken));
            } catch (EmptyResultDataAccessException | NumberFormatException e) {
                throw new IllegalArgumentException("No such space reservation: " + defaultSpaceToken);
            }
            LinkGroup linkGroup = db.getLinkGroup(space.getLinkGroupId());
            String linkGroupName = linkGroup.getName();
            selectWritePool.setLinkGroup(linkGroupName);

            StorageInfo storageInfo = selectWritePool.getStorageInfo();
            storageInfo.setKey("SpaceToken", Long.toString(space.getId()));
            storageInfo.setKey("LinkGroupId", Long.toString(linkGroup.getId()));
            if (!fileAttributes.isDefined(FileAttribute.ACCESS_LATENCY)) {
                fileAttributes.setAccessLatency(space.getAccessLatency());
            } else if (fileAttributes.getAccessLatency() != space.getAccessLatency()) {
                throw new SpaceException("Access latency conflicts with access latency defined by space reservation.");
            }
            if (!fileAttributes.isDefined(FileAttribute.RETENTION_POLICY)) {
                fileAttributes.setRetentionPolicy(space.getRetentionPolicy());
            } else if (fileAttributes.getRetentionPolicy() != space.getRetentionPolicy()) {
                throw new SpaceException(
                        "Retention policy conflicts with retention policy defined by space reservation.");
            }

            if (space.getDescription() != null) {
                storageInfo.setKey("SpaceTokenDescription", space.getDescription());
            }
            LOGGER.trace("selectPool: found linkGroup = {}, " +
                         "forwarding message", linkGroupName);
        } else if (allowUnreservedUploadsToLinkGroups && hasIdentity) {
            LOGGER.trace("selectPool: file is " +
                         "not found, no prior " +
                         "reservations for this file");

            LinkGroup linkGroup =
                    findLinkGroupForWrite(subject, selectWritePool
                            .getProtocolInfo(), fileAttributes, selectWritePool.getPreallocated());
            if (linkGroup != null) {
                String linkGroupName = linkGroup.getName();
                selectWritePool.setLinkGroup(linkGroupName);
                fileAttributes.getStorageInfo().setKey("LinkGroupId", Long.toString(linkGroup.getId()));
                LOGGER.trace("selectPool: found linkGroup = {}, " +
                             "forwarding message", linkGroupName);
            } else {
                LOGGER.trace("selectPool: did not find linkGroup that can " +
                             "hold this file, processing file without space reservation.");
            }
        } else {
            LOGGER.trace("selectPool: file is " +
                         "not found, no prior " +
                         "reservations for this file " +
                         "allowUnreservedUploadsToLinkGroups={} " +
                         "subject={}",
                         allowUnreservedUploadsToLinkGroups,
                         subject.getPrincipals());
        }
    }

    private void namespaceEntryDeleted(PnfsDeleteEntryNotificationMessage msg) throws DataAccessException
    {
        fileRemoved(msg.getPnfsId());
    }

    private void getSpaceMetaData(GetSpaceMetaData gsmd) throws IllegalArgumentException
    {
        String[] tokens = gsmd.getSpaceTokens();
        if (tokens == null) {
            throw new IllegalArgumentException("null space tokens");
        }
        Space[] spaces = new Space[tokens.length];
        for (int i = 0; i < spaces.length; ++i) {
            try {
                Space space = db.getSpace(Long.parseLong(tokens[i]));
                // Expiration of space reservations is a background activity and is not immediate.
                // S2 tests however expect the state to be accurate at any point, hence we report
                // the state as EXPIRED even when the actual state has not been updated in the
                // database yet. See usecase.CheckGarbageSpaceCollector (S2).
                if (space.getState().equals(SpaceState.RESERVED)) {
                    Long expirationTime = space.getExpirationTime();
                    if (expirationTime != null && expirationTime <= System.currentTimeMillis()) {
                        space.setState(SpaceState.EXPIRED);
                    }
                }
                spaces[i] = space;
            } catch (NumberFormatException ignored) {
            } catch (EmptyResultDataAccessException e) {
                LOGGER.error("failed to find space reservation {}: {}",
                             tokens[i], e.getMessage());
            }
        }
        gsmd.setSpaces(spaces);
    }

    private void getSpaceTokens(GetSpaceTokens gst) throws DataAccessException
    {
        String description = gst.getDescription();
        Subject subject = gst.getSubject();
        Set<Long> spaces = new HashSet<>();
        if (description == null) {
            for (String s : Subjects.getFqans(subject)) {
                if (s != null) {
                    FQAN fqan = new FQAN(s);
                    String role = fqan.getRole();
                    SpaceManagerDatabase.SpaceCriterion criterion =
                            db.spaces()
                                    .whereStateIsIn(SpaceState.RESERVED)
                                    .whereGroupIs(fqan.getGroup());
                    if (!isNullOrEmpty(role)) {
                        criterion.whereRoleIs(role);
                    }
                    spaces.addAll(db.getSpaceTokensOf(criterion));
                }
            }
            spaces.addAll(db.getSpaceTokensOf(
                    db.spaces()
                            .whereStateIsIn(SpaceState.RESERVED)
                            .whereGroupIs(Subjects.getUserName(subject))));
        } else {
            spaces.addAll(db.getSpaceTokensOf(
                    db.spaces()
                            .whereStateIsIn(SpaceState.RESERVED)
                            .whereDescriptionIs(description)));
        }
        gst.setSpaceToken(Longs.toArray(spaces));
    }

    private void getFileSpaceTokens(GetFileSpaceTokensMessage getFileTokens)
            throws DataAccessException
    {
        PnfsId pnfsId = getFileTokens.getPnfsId();
        List<File> files = db.get(db.files().wherePnfsIdIs(pnfsId), null);
        getFileTokens.setSpaceToken(Longs.toArray(transform(files, File::getSpaceId)));
    }

    private void extendLifetime(ExtendLifetime extendLifetime) throws DataAccessException
    {
        long token = extendLifetime.getSpaceToken();
        long newLifetime = extendLifetime.getNewLifetime();
        Space space = db.selectSpaceForUpdate(token);
        if (space.getState().isFinal()) {
            throw new DataIntegrityViolationException("Space reservation was already released.");
        }
        Long oldExpirationTime = space.getExpirationTime();
        if (oldExpirationTime != null) {
            if (newLifetime == -1) {
                space.setExpirationTime(null);
                db.updateSpace(space);
                return;
            }
            long newExpirationTime = System.currentTimeMillis() + newLifetime;
            if (newExpirationTime > oldExpirationTime) {
                space.setExpirationTime(newExpirationTime);
                db.updateSpace(space);
            }
        }
    }

    /**
     * Utility runnable that does nothing if a request has exceeded its TTL and
     * reenqueues the request if processing fails while blocking the thread with
     * a Fibonacci backoff.
     */
    private abstract class FibonacciBackoffMessageProcessor implements Runnable
    {
        private final CellMessage envelope;
        private final Executor executor;
        private long previous = 0;
        private long current = 1;

        public FibonacciBackoffMessageProcessor(Executor executor, CellMessage envelope)
        {
            this.executor = executor;
            this.envelope = envelope;
        }

        public CellMessage getEnvelope()
        {
            return envelope;
        }

        protected abstract void process() throws Exception;

        /**
         * Returns the fibonacci numbers.
         */
        public long next()
        {
            long next = current + previous;
            previous = current;
            current = next;
            return previous;
        }

        @Override
        public void run()
        {
            try {
                if (envelope.getLocalAge() > envelope.getAdjustedTtl()) {
                    LOGGER.warn(
                            "Discarding {} because its age of {} ms exceeds its time to live of {} ms.",
                            envelope.getMessageObject().getClass().getSimpleName(), envelope.getLocalAge(),
                            envelope.getAdjustedTtl());
                } else {
                    process();
                }
            } catch (InterruptedException ignored) {
                notifyShutdown(envelope);
            } catch (Exception e) {
                /* Put the request at the end of the queue to (a) avoid starving other requests, (b) avoid
                 * retrying the same operation over and over in a tight loop.
                 */
                try {
                    Thread.sleep(next());
                } catch (InterruptedException ignored) {
                }
                executor.execute(this);
            }
        }
    }
}
