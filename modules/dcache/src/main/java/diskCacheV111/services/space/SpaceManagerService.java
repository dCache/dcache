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
//  (for files in states ALLOCATED or TRANSFERRING)
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
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.annotation.Transactional;

import javax.security.auth.Subject;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.services.space.message.CancelUse;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.services.space.message.GetLinkGroupNamesMessage;
import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.services.space.message.Release;
import diskCacheV111.services.space.message.Reserve;
import diskCacheV111.services.space.message.Use;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
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
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.auth.FQAN;
import org.dcache.auth.Subjects;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;

public final class SpaceManagerService
        extends AbstractCellComponent
        implements CellCommandListener,
                   CellMessageReceiver,
                   Runnable
{
        private static final Logger LOGGER = LoggerFactory.getLogger(SpaceManagerService.class);

        private long expireSpaceReservationsPeriod;

        private Thread expireSpaceReservations;

        private AccessLatency defaultAccessLatency;

        private boolean shouldDeleteStoredFileRecord;
        private boolean shouldReserveSpaceForNonSrmTransfers;
        private boolean shouldReturnFlushedSpaceToReservation;
        private boolean shouldCleanupExpiredSpaceFiles;
        private boolean isSpaceManagerEnabled;

        private CellPath poolManager;
        private PnfsHandler pnfs;

        private SpaceManagerAuthorizationPolicy authorizationPolicy;

        private Executor executor;

        private PoolMonitor poolMonitor;
        private SpaceManagerDatabase db;
        private LinkGroupLoader linkGroupLoader;
        private long perishedSpacePurgeDelay;

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
        public void setDefaultAccessLatency(AccessLatency defaultAccessLatency)
        {
                this.defaultAccessLatency = defaultAccessLatency;
        }

        @Required
        public void setShouldReserveSpaceForNonSrmTransfers(boolean shouldReserveSpaceForNonSrmTransfers)
        {
                this.shouldReserveSpaceForNonSrmTransfers = shouldReserveSpaceForNonSrmTransfers;
        }

        @Required
        public void setShouldDeleteStoredFileRecord(boolean shouldDeleteStoredFileRecord)
        {
                this.shouldDeleteStoredFileRecord = shouldDeleteStoredFileRecord;
        }

        @Required
        public void setShouldCleanupExpiredSpaceFiles(boolean shouldCleanupExpiredSpaceFiles)
        {
                this.shouldCleanupExpiredSpaceFiles = shouldCleanupExpiredSpaceFiles;
        }

        @Required
        public void setShouldReturnFlushedSpaceToReservation(boolean shouldReturnFlushedSpaceToReservation)
        {
                this.shouldReturnFlushedSpaceToReservation = shouldReturnFlushedSpaceToReservation;
        }

        @Required
        public void setExecutor(ExecutorService executor)
        {
            this.executor = new CDCExecutorServiceDecorator(executor);
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
                (expireSpaceReservations = new Thread(this,"ExpireThreadReservations")).start();
        }

        public void stop() throws InterruptedException
        {
                if (expireSpaceReservations != null) {
                        expireSpaceReservations.interrupt();
                        expireSpaceReservations.join();
                }
        }


        @Override
        public void getInfo(PrintWriter printWriter) {
                printWriter.println("isSpaceManagerEnabled=" + isSpaceManagerEnabled);
                printWriter.println("expireSpaceReservationsPeriod="
                                    + expireSpaceReservationsPeriod);
                printWriter.println("shouldDeleteStoredFileRecord="
                                    + shouldDeleteStoredFileRecord);
                printWriter.println("defaultLatencyForSpaceReservations="
                                            + defaultAccessLatency);
                printWriter.println("shouldReserveSpaceForNonSrmTransfers="
                                            + shouldReserveSpaceForNonSrmTransfers);
                printWriter.println("shouldReturnFlushedSpaceToReservation="
                                            + shouldReturnFlushedSpaceToReservation);
        }

        private void expireSpaceReservations() throws DataAccessException
        {
                LOGGER.trace("expireSpaceReservations()...");
                if (shouldCleanupExpiredSpaceFiles) {
                        /* We do not run the entire loop in a single transaction
                         * to prevent locking the space and link group records
                         * while deleting the files in the name space.
                         */
                        SpaceManagerDatabase.FileCriterion expiredFiles =
                                db.files()
                                        .whereStateIsIn(FileState.ALLOCATED,FileState.TRANSFERRING)
                                        .thatExpireBefore(System.currentTimeMillis());
                        final int maximumNumberFilesToLoadAtOnce = 1000;
                        for (File file : db.get(expiredFiles, maximumNumberFilesToLoadAtOnce)) {
                                try {
                                    removeExpiredFile(file.getId());
                                } catch (TransientDataAccessException e) {
                                        LOGGER.warn("Transient data access failure while deleting expired file {}: {}",
                                                     file, e.getMessage());
                                } catch (DataAccessException e) {
                                        LOGGER.error("Data access failure while deleting expired file {}: {}",
                                                     file, e.getMessage());
                                        break;
                                } catch (TimeoutCacheException e) {
                                        LOGGER.error("Failed to delete file {}: {}", file.getPnfsId(), e.getMessage());
                                        break;
                                } catch (CacheException e) {
                                        LOGGER.error("Failed to delete file {}: {}", file.getPnfsId(), e.getMessage());
                                }
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

        @Transactional(rollbackFor = { CacheException.class })
        private void removeExpiredFile(long id) throws CacheException
        {
                File file = db.selectFileForUpdate(id);
                if (file.isExpired()) {
                        if (file.getPnfsId() != null) {
                                try {
                                        pnfs.deletePnfsEntry(file.getPnfsId(), Objects.toString(file.getPath(), null));
                                } catch (FileNotFoundCacheException ignored) {
                                }
                        }
                        db.removeFile(file.getId());
                }
        }

        private void getValidSpaceTokens(GetSpaceTokensMessage msg) throws DataAccessException {
            msg.setSpaceTokenSet(db.get(db.spaces().thatNeverExpire().whereStateIsIn(SpaceState.RESERVED), null));
        }

        private void getLinkGroups(GetLinkGroupsMessage msg) throws DataAccessException {
                msg.setLinkGroups(db.get(db.linkGroups()));
        }

        private void getLinkGroupNames(GetLinkGroupNamesMessage msg) throws DataAccessException {
                msg.setLinkGroupNames(newArrayList(transform(db.get(db.linkGroups()), LinkGroup.getName)));
        }

    /** Returns true if message is of a type processed exclusively by SpaceManager */
        private boolean isSpaceManagerMessage(Message message)
        {
                return message instanceof Reserve
                        || message instanceof GetSpaceTokensMessage
                        || message instanceof GetLinkGroupsMessage
                        || message instanceof GetLinkGroupNamesMessage
                        || message instanceof Release
                        || message instanceof Use
                        || message instanceof CancelUse
                        || message instanceof GetSpaceMetaData
                        || message instanceof GetSpaceTokens
                        || message instanceof ExtendLifetime
                        || message instanceof GetFileSpaceTokensMessage;
        }

        /** Returns true if message is a notification to which SpaceManager subscribes */
        private boolean isNotificationMessage(Message message)
        {
                return message instanceof PoolFileFlushedMessage
                        || message instanceof PoolRemoveFilesMessage
                        || message instanceof PnfsDeleteEntryNotificationMessage;
        }

        /**
         * Returns true if message is of a type that needs processing by SpaceManager even if
         * SpaceManager is not the intended final destination.
         */
        private boolean isInterceptedMessage(Message message)
        {
                return (message instanceof PoolMgrSelectWritePoolMsg && ((PoolMgrSelectWritePoolMsg) message).getPnfsPath() != null && !message.isReply())
                       || message instanceof DoorTransferFinishedMessage
                       || (message instanceof PoolAcceptFileMessage && ((PoolAcceptFileMessage) message).getFileAttributes().getStorageInfo().getKey("LinkGroup") != null);
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
                    executor.execute(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            processMessage(message);
                            if (message.getReplyRequired()) {
                                try {
                                    envelope.revertDirection();
                                    sendMessage(envelope);
                                }
                                catch (NoRouteToCellException e) {
                                    LOGGER.error("Failed to send reply: {}", e.getMessage());
                                }
                            }
                        }
                    });
                } else if (message.getReplyRequired()) {
                    try {
                        message.setReply(1, "Space manager is disabled in configuration");
                        envelope.revertDirection();
                        sendMessage(envelope);
                    }
                    catch (NoRouteToCellException e) {
                        LOGGER.error("Failed to send reply: {}", e.getMessage());
                    }
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
                    executor.execute(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            processMessage(message);

                            if (message.getReturnCode() != 0 && !isEnRouteToDoor) {
                                envelope.revertDirection();
                            }

                            try {
                                sendMessage(envelope);
                            } catch (NoRouteToCellException e) {
                                LOGGER.error("Failed to forward message: {}", e.getMessage());
                            }
                        }
                    });
                } else {
                    try {
                        sendMessage(envelope);
                    } catch (NoRouteToCellException e) {
                        LOGGER.error("Failed to forward message: {}", e.getMessage());
                    }
                }
            }
        }

        private void processMessage(Message message)
        {
            try {
                boolean isSuccessful = false;
                for (int attempts = 0; !isSuccessful; attempts++) {
                    try {
                        if (message instanceof PoolRemoveFilesMessage) {
                            // fileRemoved does its own transaction management
                            fileRemoved((PoolRemoveFilesMessage) message);
                        }
                        else {
                            processMessageTransactionally(message);
                        }
                        isSuccessful = true;
                    } catch (TransientDataAccessException | RecoverableDataAccessException e) {
                        if (attempts >= 3) {
                            throw e;
                        }
                        LOGGER.warn("Retriable data access error: {}", e.toString());
                    }
                }
            } catch (SpaceAuthorizationException e) {
                message.setFailedConditionally(CacheException.PERMISSION_DENIED, e);
            } catch (NoFreeSpaceException e) {
                message.setFailedConditionally(CacheException.RESOURCE, e);
            } catch (SpaceException e) {
                message.setFailedConditionally(CacheException.DEFAULT_ERROR_CODE, e);
            } catch (IllegalArgumentException e) {
                LOGGER.error("Message processing failed: {}", e.getMessage(), e);
                message.setFailedConditionally(CacheException.INVALID_ARGS, e.getMessage());
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

        @Transactional(rollbackFor = { SpaceException.class })
        private void processMessageTransactionally(Message message) throws SpaceException
        {
            if (message instanceof PoolMgrSelectWritePoolMsg) {
                selectPool((PoolMgrSelectWritePoolMsg) message);
            }
            else if (message instanceof PoolAcceptFileMessage) {
                PoolAcceptFileMessage poolRequest = (PoolAcceptFileMessage) message;
                if (message.isReply()) {
                    transferStarted(poolRequest.getPnfsId(), poolRequest.getReturnCode() == 0);
                }
                else {
                    transferStarting(poolRequest);
                }
            }
            else if (message instanceof DoorTransferFinishedMessage) {
                transferFinished((DoorTransferFinishedMessage) message);
            }
            else if (message instanceof Reserve) {
                reserveSpace((Reserve) message);
            }
            else if (message instanceof GetSpaceTokensMessage) {
                getValidSpaceTokens((GetSpaceTokensMessage) message);
            }
            else if (message instanceof GetLinkGroupsMessage) {
                getLinkGroups((GetLinkGroupsMessage) message);
            }
            else if (message instanceof GetLinkGroupNamesMessage) {
                getLinkGroupNames((GetLinkGroupNamesMessage) message);
            }
            else if (message instanceof Release) {
                releaseSpace((Release) message);
            }
            else if (message instanceof Use) {
                useSpace((Use) message);
            }
            else if (message instanceof CancelUse) {
                cancelUseSpace((CancelUse) message);
            }
            else if (message instanceof GetSpaceMetaData) {
                getSpaceMetaData((GetSpaceMetaData) message);
            }
            else if (message instanceof GetSpaceTokens) {
                getSpaceTokens((GetSpaceTokens) message);
            }
            else if (message instanceof ExtendLifetime) {
                extendLifetime((ExtendLifetime) message);
            }
            else if (message instanceof PoolFileFlushedMessage) {
                fileFlushed((PoolFileFlushedMessage) message);
            }
            else if (message instanceof GetFileSpaceTokensMessage) {
                getFileSpaceTokens((GetFileSpaceTokensMessage) message);
            }
            else if (message instanceof PnfsDeleteEntryNotificationMessage) {
                namespaceEntryDeleted((PnfsDeleteEntryNotificationMessage) message);
            }
            else {
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
                Subject subject =  release.getSubject();
                authorizationPolicy.checkReleasePermission(subject, space);
                space.setState(SpaceState.RELEASED);
                db.updateSpace(space);
        }

        private void reserveSpace(Reserve reserve)
                throws DataAccessException, SpaceException
        {
                if (reserve.getRetentionPolicy()==null) {
                        throw new IllegalArgumentException("reserveSpace : retentionPolicy=null is not supported");
                }

                Space space = reserveSpace(reserve.getSubject(),
                                           reserve.getSizeInBytes(),
                                           (reserve.getAccessLatency() == null ?
                                                   defaultAccessLatency : reserve.getAccessLatency()),
                                           reserve.getRetentionPolicy(),
                                           reserve.getLifetime(),
                                           reserve.getDescription(),
                                           null,
                                           null,
                                           null);
                reserve.setSpaceToken(space.getId());
        }

        private void useSpace(Use use)
                throws DataAccessException, SpaceException
        {
                LOGGER.trace("useSpace({})", use);
                FsPath path = use.getPath();
                File file = db.findFile(path);
                if (file != null) {
                    throw new SpaceException("The file is locked by space management for another upload. " +
                                                     "The lock expires at " + new Date(file.getExpirationTime()) + ".");
                }
                long fileId = useSpace(use.getSpaceToken(),
                                       use.getSubject(),
                                       use.getSizeInBytes(),
                                       use.getLifetime(),
                                       path,
                                       null);
                use.setFileId(fileId);
        }

        private void transferStarting(PoolAcceptFileMessage message) throws DataAccessException, SpaceException
        {
            LOGGER.trace("transferStarting({})", message);
            PnfsId pnfsId = checkNotNull(message.getPnfsId());
            FileAttributes fileAttributes = message.getFileAttributes();
            Subject subject = message.getSubject();
            String linkGroupName = checkNotNull(fileAttributes.getStorageInfo().getKey("LinkGroup"));
            String spaceToken = fileAttributes.getStorageInfo().getKey("SpaceToken");
            String fileId = fileAttributes.getStorageInfo().setKey("SpaceFileId", null);
            if (fileId != null) {
                /* This takes care of records created by SRM before
                 * transfer has started
                 */
                File f = db.selectFileForUpdate(Long.parseLong(fileId));
                if (f.getPnfsId() != null) {
                    throw new SpaceException("The file is locked by space management for another upload.");
                }
                f.setPnfsId(pnfsId);
                db.updateFile(f);
            } else if (spaceToken != null) {
                LOGGER.trace("transferStarting: file is not " +
                                     "found, found default space " +
                                     "token, calling insertFile()");
                long lifetime = 1000 * 60 * 60;
                useSpace(Long.parseLong(spaceToken),
                        subject,
                        message.getPreallocated(),
                        lifetime,
                        null,
                        pnfsId);
            } else {
                LOGGER.trace("transferStarting: file is not found, no prior reservations for this file");

                long sizeInBytes = message.getPreallocated();
                long lifetime    = 1000*60*60;
                String description = null;
                LinkGroup linkGroup = db.getLinkGroupByName(linkGroupName);
                VOInfo voInfo = authorizationPolicy.checkReservePermission(subject, linkGroup);

                Space space = db.insertSpace(voInfo.getVoGroup(),
                                             voInfo.getVoRole(),
                                             fileAttributes.getRetentionPolicy(),
                                             fileAttributes.getAccessLatency(),
                                             linkGroup.getId(),
                                             sizeInBytes,
                                             lifetime,
                                             description,
                                             SpaceState.RESERVED,
                                             0,
                                             0);
                db.insertFile(space.getId(),
                              voInfo.getVoGroup(),
                              voInfo.getVoRole(),
                              sizeInBytes,
                              lifetime,
                              null,
                              pnfsId);

                /* One could inject SpaceToken and SpaceTokenDescription into storage
                 * info at this point, but since the space reservation is implicit and
                 * short lived, this information will not be of much use.
                 */
            }
        }

        private void transferStarted(PnfsId pnfsId,boolean success)
                throws DataAccessException
        {
            try {
                LOGGER.trace("transferStarted({},{})", pnfsId, success);
                File f = db.selectFileForUpdate(pnfsId);
                if (f.getState() == FileState.ALLOCATED) {
                    if(!success) {
                            if (f.getPath() != null) {
                                f.setPnfsId(null);
                                db.updateFile(f);
                            } else {
                                /* This reservation was created by space manager
                                 * when the transfer started. Delete it.
                                 */
                                db.removeFile(f.getId());

                                /* TODO: If we also created the reservation, we should
                                 * release it at this point, but at the moment we cannot
                                 * know who created it. It will eventually expire
                                 * automatically.
                                 */
                            }
                    } else {
                            f.setState(FileState.TRANSFERRING);
                            db.updateFile(f);
                    }
                }
            } catch (EmptyResultDataAccessException e) {
                LOGGER.trace("transferStarted failed: {}", e.getMessage());
            }
        }

        private void transferFinished(DoorTransferFinishedMessage finished)
                throws DataAccessException
        {
                boolean weDeleteStoredFileRecord = shouldDeleteStoredFileRecord;
                PnfsId pnfsId = finished.getPnfsId();
                long size = finished.getFileAttributes().getSize();
                boolean success = finished.getReturnCode() == 0;
                LOGGER.trace("transferFinished({},{})", pnfsId, success);
                File f;
                try {
                        f = db.selectFileForUpdate(pnfsId);
                }
                catch (EmptyResultDataAccessException e) {
                        LOGGER.trace("failed to find file {}: {}", pnfsId,
                                     e.getMessage());
                        return;
                }
                long spaceId = f.getSpaceId();
                if(f.getState() == FileState.ALLOCATED ||
                   f.getState() == FileState.TRANSFERRING) {
                        if(success) {
                                if(shouldReturnFlushedSpaceToReservation && weDeleteStoredFileRecord) {
                                        RetentionPolicy rp = db.getSpace(spaceId).getRetentionPolicy();
                                        if(rp.equals(RetentionPolicy.CUSTODIAL)) {
                                                //we do not delete it here, since the
                                                // file will get flushed and we will need
                                                // to account for that
                                                weDeleteStoredFileRecord = false;
                                        }
                                }
                                if(weDeleteStoredFileRecord) {
                                        LOGGER.trace("file transferred, deleting file record");
                                        db.removeFile(f.getId());
                                }
                                else {
                                        f.setSizeInBytes(size);
                                        f.setState(FileState.STORED);
                                        f.setPath(null);
                                        db.updateFile(f);
                                }
                        }
                        else {
                                if (f.getPath() != null) {
                                    f.setPnfsId(null);
                                    f.setState(FileState.ALLOCATED);
                                    db.updateFile(f);
                                } else {
                                    /* This reservation was created by space manager
                                     * when the transfer started. Delete it.
                                     */
                                    db.removeFile(f.getId());

                                    /* TODO: If we also created the reservation, we should
                                     * release it at this point, but at the moment we cannot
                                     * know who created it. It will eventually expire
                                     * automatically.
                                     */
                                }
                        }
                }
                else {
                        LOGGER.trace("transferFinished({}): file state={}",
                                     pnfsId, f.getState());
                }
        }

        private void  fileFlushed(PoolFileFlushedMessage fileFlushed) throws DataAccessException
        {
                if(!shouldReturnFlushedSpaceToReservation) {
                        return;
                }
                PnfsId pnfsId = fileFlushed.getPnfsId();
                LOGGER.trace("fileFlushed({})", pnfsId);
                FileAttributes fileAttributes = fileFlushed.getFileAttributes();
                AccessLatency ac = fileAttributes.getAccessLatency();
                if (ac.equals(AccessLatency.ONLINE)) {
                        LOGGER.trace("File Access latency is ONLINE " +
                                             "fileFlushed does nothing");
                        return;
                }
                long size = fileAttributes.getSize();
                try {
                        File f = db.selectFileForUpdate(pnfsId);
                        if(f.getState() == FileState.STORED) {
                                if(shouldDeleteStoredFileRecord) {
                                        LOGGER.trace("returnSpaceToReservation, " +
                                                             "deleting file record");
                                        db.removeFile(f.getId());
                                }
                                else {
                                        f.setSizeInBytes(size);
                                        f.setState(FileState.FLUSHED);
                                        db.updateFile(f);
                                }
                        }
                        else {
                                LOGGER.trace("returnSpaceToReservation({}): " +
                                                     "file state={}", pnfsId, f.getState());
                        }

                }
                catch (EmptyResultDataAccessException e) {
                    /* if this file is not in srmspacefile table, silently quit */
                }
        }

        private void fileRemoved(PoolRemoveFilesMessage fileRemoved)
        {
                for (String pnfsId : fileRemoved.getFiles()) {
                        try {
                                fileRemoved(pnfsId);
                        }
                        catch (IllegalArgumentException e) {
                                LOGGER.error("badly formed PNFS-ID: {}", pnfsId);
                        }
                        catch (DataAccessException sqle) {
                                LOGGER.trace("failed to remove file from space reservation: {}",
                                             sqle.getMessage());
                                LOGGER.trace("fileRemoved({}): file not in a " +
                                                     "reservation, do nothing", pnfsId);
                        }
                }
        }

        @Transactional
        private void fileRemoved(String pnfsId)
        {
            LOGGER.trace("fileRemoved({})", pnfsId);
            File f = db.selectFileForUpdate(new PnfsId(pnfsId));
            if ((f.getState() != FileState.ALLOCATED && f.getState() != FileState.TRANSFERRING) || f.getPath() == null) {
                db.removeFile(f.getId());
            } else if (f.getState() == FileState.TRANSFERRING) {
                f.setPnfsId(null);
                f.setState(FileState.ALLOCATED);
                db.updateFile(f);
            }
        }

        private void cancelUseSpace(CancelUse cancelUse)
                throws DataAccessException, SpaceException
        {
                LOGGER.trace("cancelUseSpace({})", cancelUse);
                long reservationId = cancelUse.getSpaceToken();
                FsPath path = cancelUse.getPath();
                File f;
                try {
                        f = db.selectFileForUpdate(path);
                } catch (EmptyResultDataAccessException ignored) {
                        return;
                }
                if ((f.getState() == FileState.ALLOCATED || f.getState() == FileState.TRANSFERRING) && f.getSpaceId() == reservationId) {
                        try {
                                if (f.getPnfsId() != null) {
                                        try {
                                        pnfs.deletePnfsEntry(f.getPnfsId(), path.toString());
                                        } catch (FileNotFoundCacheException ignored) {
                                        }
                                }
                                db.removeFile(f.getId());
                        } catch (CacheException e) {
                            throw new SpaceException("Failed to delete " + path +
                                                     " while attempting to cancel its reservation in space reservation " +
                                                     reservationId + ": " + e.getMessage(), e);
                        }
                }
        }

        private Space reserveSpace(Subject subject,
                                   long sizeInBytes,
                                   AccessLatency latency ,
                                   RetentionPolicy policy,
                                   long lifetime,
                                   String description,
                                   ProtocolInfo protocolInfo,
                                   FileAttributes fileAttributes,
                                   PnfsId pnfsId)
                throws DataAccessException, SpaceException
        {
                LOGGER.trace("reserveSpace( subject={}, sz={}, latency={}, " +
                                     "policy={}, lifetime={}, description={}", subject.getPrincipals(),
                             sizeInBytes, latency, policy, lifetime, description);
                List<LinkGroup> linkGroups = db.findLinkGroups(sizeInBytes, latency, policy, linkGroupLoader.getLatestUpdateTime());
                if(linkGroups.isEmpty()) {
                        LOGGER.warn("failed to find matching linkgroup");
                        throw new NoFreeSpaceException(" no space available");
                }
                //
                // filter out groups we are not authorized to use
                //
                Map<String,VOInfo> linkGroupNameVoInfoMap = new HashMap<>();
                for (LinkGroup linkGroup : linkGroups) {
                        try {
                                VOInfo voInfo =
                                        authorizationPolicy.checkReservePermission(subject,
                                                                                   linkGroup);
                                linkGroupNameVoInfoMap.put(linkGroup.getName(),voInfo);
                        }
                        catch (SpaceAuthorizationException ignored) {
                        }
                }
                if(linkGroupNameVoInfoMap.isEmpty()) {
                        LOGGER.warn("failed to find linkgroup where user is " +
                                            "authorized to reserve space.");
                        throw new SpaceAuthorizationException("Failed to find LinkGroup where user is authorized to reserve space.");
                }
                List<String> linkGroupNames = new ArrayList<>(linkGroupNameVoInfoMap.keySet());
                LOGGER.trace("Found {} linkgroups protocolInfo={}, " +
                                     "storageInfo={}, pnfsId={}", linkGroups.size(),
                             protocolInfo, fileAttributes, pnfsId);
                if (linkGroupNameVoInfoMap.size()>1 &&
                    protocolInfo != null &&
                    fileAttributes != null) {
                        try {
                                linkGroupNames = findLinkGroupForWrite(protocolInfo, fileAttributes, linkGroupNames);
                                if(linkGroupNames.isEmpty()) {
                                        throw new SpaceAuthorizationException("PoolManagerSelectLinkGroupForWriteMessage: Failed to find LinkGroup where user is authorized to reserve space.");
                                }
                        }
                        catch (SpaceAuthorizationException e)  {
                                LOGGER.warn("authorization problem: {}",
                                            e.getMessage());
                                throw e;
                        }
                        catch(Exception e) {
                                throw new SpaceException("Internal error : Failed to get list of link group ids from Pool Manager "+e.getMessage());
                        }

                }
                String linkGroupName = linkGroupNames.get(0);
                VOInfo voInfo        = linkGroupNameVoInfoMap.get(linkGroupName);
                LinkGroup linkGroup  = null;
                for (LinkGroup lg : linkGroups) {
                        if (lg.getName().equals(linkGroupName) ) {
                                linkGroup = lg;
                                break;
                        }
                }
                LOGGER.trace("Chose linkgroup {}", linkGroup);
                return db.insertSpace(voInfo.getVoGroup(),
                                      voInfo.getVoRole(),
                                      policy,
                                      latency,
                                      linkGroup.getId(),
                                      sizeInBytes,
                                      lifetime,
                                      description,
                                      SpaceState.RESERVED,
                                      0,
                                      0);
        }

        private LinkGroup findLinkGroupForWrite(Subject subject, ProtocolInfo protocolInfo,
                                                FileAttributes fileAttributes, long size)
                throws DataAccessException
        {
            List<LinkGroup> linkGroups =
                    db.findLinkGroups(size, fileAttributes.getAccessLatency(), fileAttributes.getRetentionPolicy(), linkGroupLoader.getLatestUpdateTime());
            List<String> linkGroupNames = new ArrayList<>();
            for (LinkGroup linkGroup : linkGroups) {
                try {
                    authorizationPolicy.checkReservePermission(subject, linkGroup);
                    linkGroupNames.add(linkGroup.getName());
                }
                catch (SpaceAuthorizationException ignored) {
                }
            }
            linkGroupNames = findLinkGroupForWrite(protocolInfo, fileAttributes, linkGroupNames);
            LOGGER.trace("Found {} linkgroups protocolInfo={}, fileAttributes={}",
                         linkGroups.size(), protocolInfo, fileAttributes);

            if (!linkGroupNames.isEmpty()) {
                String linkGroupName = linkGroupNames.get(0);
                for (LinkGroup lg : linkGroups) {
                    if (lg.getName().equals(linkGroupName) ) {
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
                for (String linkGroup: linkGroups) {
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

        private long useSpace(long reservationId,
                              Subject subject,
                              long sizeInBytes,
                              long lifetime,
                              FsPath path,
                              PnfsId pnfsId)
                throws DataAccessException, SpaceException
        {
            String effectiveGroup;
            String effectiveRole;
            String primaryFqan = Subjects.getPrimaryFqan(subject);
            if (primaryFqan != null) {
                FQAN fqan = new FQAN(primaryFqan);
                effectiveGroup = fqan.getGroup();
                effectiveRole = fqan.getRole();
            } else {
                effectiveGroup = Subjects.getUserName(subject);
                effectiveRole = null;
            }
            return db.insertFile(reservationId,
                                 effectiveGroup,
                                 effectiveRole,
                                 sizeInBytes,
                                 lifetime,
                                 path,
                                 pnfsId);
        }

        /**
         * Called upon intercepting PoolMgrSelectWritePoolMsg requests.
         *
         * Injects the link group name into the request message. Also adds SpaceToken, LinkGroup,
         * and SpaceFileId flags to StorageInfo. These are accessed when space manager intercepts
         * the subsequent PoolAcceptFileMessage.
         */
        private void selectPool(PoolMgrSelectWritePoolMsg selectWritePool) throws DataAccessException, SpaceException
        {
            LOGGER.trace("selectPool({})", selectWritePool);
            FileAttributes fileAttributes = selectWritePool.getFileAttributes();
            String defaultSpaceToken = fileAttributes.getStorageInfo().getMap().get("writeToken");
            Subject subject = selectWritePool.getSubject();
            boolean hasIdentity =
                    !Subjects.getFqans(subject).isEmpty() || Subjects.getUserName(subject) != null;

            FsPath path = new FsPath(checkNotNull(selectWritePool.getPnfsPath()));
            File file = db.findFile(path);
            if (file != null) {
                if (file.getPnfsId() != null) {
                    throw new SpaceException("The file is locked by space management for another upload.");
                }

                /*
                 * This takes care of records created by SRM before
                 * transfer has started
                 */
                Space space = db.getSpace(file.getSpaceId());
                LinkGroup linkGroup = db.getLinkGroup(space.getLinkGroupId());
                String linkGroupName = linkGroup.getName();
                selectWritePool.setLinkGroup(linkGroupName);

                StorageInfo storageInfo = fileAttributes.getStorageInfo();
                storageInfo.setKey("SpaceToken", Long.toString(space.getId()));
                storageInfo.setKey("LinkGroup", linkGroupName);
                fileAttributes.setAccessLatency(space.getAccessLatency());
                fileAttributes.setRetentionPolicy(space.getRetentionPolicy());

                if (fileAttributes.getSize() == 0 && file.getSizeInBytes() > 1) {
                    fileAttributes.setSize(file.getSizeInBytes());
                }
                if (space.getDescription() != null) {
                    storageInfo.setKey("SpaceTokenDescription", space.getDescription());
                }
                storageInfo.setKey("SpaceFileId", Long.toString(file.getId()));
                LOGGER.trace("selectPool: found linkGroup = {}, " +
                                     "forwarding message", linkGroupName);
            } else if (defaultSpaceToken != null) {
                LOGGER.trace("selectPool: file is not " +
                                     "found, found default space " +
                                     "token, calling insertFile()");
                Space space = db.getSpace(Long.parseLong(defaultSpaceToken));
                LinkGroup linkGroup = db.getLinkGroup(space.getLinkGroupId());
                String linkGroupName = linkGroup.getName();
                selectWritePool.setLinkGroup(linkGroupName);

                StorageInfo storageInfo = selectWritePool.getStorageInfo();
                storageInfo.setKey("SpaceToken", Long.toString(space.getId()));
                storageInfo.setKey("LinkGroup", linkGroupName);
                fileAttributes.setAccessLatency(space.getAccessLatency());
                fileAttributes.setRetentionPolicy(space.getRetentionPolicy());

                if (space.getDescription() != null) {
                    storageInfo.setKey("SpaceTokenDescription", space.getDescription());
                }
                LOGGER.trace("selectPool: found linkGroup = {}, " +
                                     "forwarding message", linkGroupName);
            } else if (shouldReserveSpaceForNonSrmTransfers && hasIdentity) {
                LOGGER.trace("selectPool: file is " +
                                     "not found, no prior " +
                                     "reservations for this file");

                LinkGroup linkGroup =
                        findLinkGroupForWrite(subject, selectWritePool
                                .getProtocolInfo(), fileAttributes, selectWritePool.getPreallocated());
                if (linkGroup != null) {
                    String linkGroupName = linkGroup.getName();
                    selectWritePool.setLinkGroup(linkGroupName);
                    fileAttributes.getStorageInfo().setKey("LinkGroup", linkGroupName);
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
                                     "shouldReserveSpaceForNonSrmTransfers={} " +
                                     "subject={}",
                             shouldReserveSpaceForNonSrmTransfers,
                             subject.getPrincipals());
            }
        }

        private void namespaceEntryDeleted(PnfsDeleteEntryNotificationMessage msg) throws DataAccessException
        {
            try {
                File f = db.selectFileForUpdate(msg.getPnfsId());
                LOGGER.trace("Marking file as deleted {}", f);
                if (f.getState() == FileState.FLUSHED) {
                    db.removeFile(f.getId());
                } else if (f.getState() == FileState.STORED || f.getPath() == null) {
                    f.setDeleted(true);
                    db.updateFile(f);
                } else if (f.getState() == FileState.TRANSFERRING || f.getState() == FileState.ALLOCATED) {
                    f.setPnfsId(null);
                    f.setState(FileState.ALLOCATED);
                    db.updateFile(f);
                }
            } catch (EmptyResultDataAccessException ignored) {
            }
        }

        private void getSpaceMetaData(GetSpaceMetaData gsmd) throws IllegalArgumentException {
                long[] tokens = gsmd.getSpaceTokens();
                if(tokens == null) {
                        throw new IllegalArgumentException("null space tokens");
                }
                Space[] spaces = new Space[tokens.length];
                for(int i=0;i<spaces.length; ++i){

                        Space space = null;
                        try {
                                space = db.getSpace(tokens[i]);
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
                        }
                        catch(EmptyResultDataAccessException e) {
                                LOGGER.error("failed to find space reservation {}: {}",
                                             tokens[i], e.getMessage());
                        }
                        spaces[i] = space;
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
                }
                else {
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
                getFileTokens.setSpaceToken(Longs.toArray(transform(files, File.getSpaceToken)));
        }

        private void extendLifetime(ExtendLifetime extendLifetime) throws DataAccessException
        {
                long token            = extendLifetime.getSpaceToken();
                long newLifetime      = extendLifetime.getNewLifetime();
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
}
