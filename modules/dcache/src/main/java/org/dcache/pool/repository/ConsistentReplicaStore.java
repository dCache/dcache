package org.dcache.pool.repository;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.FileIsNewCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.TimeoutCacheException;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.classic.ReplicaStatePolicy;
import org.dcache.pool.movers.IoMode;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.isEmpty;
import static org.dcache.namespace.FileAttribute.*;

/**
 * Wrapper for a ReplicaStore which encapsulates the logic for
 * recovering ReplicaRecord objects from PnfsManager in case they are
 * missing or broken in a ReplicaStore.
 *
 * Warning: The class is only thread-safe as long as its methods are
 * not invoked concurrently on the same PNFS ID.
 */
public class ConsistentReplicaStore
    implements ReplicaStore
{
    private static final Logger _log = LoggerFactory.getLogger(ConsistentReplicaStore.class);

    private static final String RECOVERING_MSG =
        "Recovering %1$s...";
    private static final String PARTIAL_FROM_TAPE_MSG =
        "Recovering: Removed %1$s because it was not fully staged.";
    private static final String FETCHED_STORAGE_INFO_FOR_1$S_FROM_PNFS =
        "Recovering: Fetched storage info for %1$s from name space.";
    private static final String FILE_NOT_FOUND_MSG =
        "Recovering: Removed %1$s because name space entry was deleted.";
    private static final String UPDATE_SIZE_MSG =
        "Recovering: Setting size of %1$s in name space to %2$d.";
    private static final String UPDATE_ACCESS_LATENCY_MSG =
        "Recovering: Setting access latency of %1$s in name space to %2$s.";
    private static final String UPDATE_RETENTION_POLICY_MSG =
        "Recovering: Setting retention policy of %1$s in name space to %2$s.";
    private static final String UPDATE_CHECKSUM_MSG =
        "Recovering: Setting checksum of %1$s in name space to %2$s.";
    private static final String MARKED_MSG =
        "Recovering: Marked %1$s as %2$s.";
    private static final String BAD_MSG =
        "Marked %1$s bad: %2$s.";
    private static final String BAD_SIZE_MSG =
        "File size mismatch for %1$s. Expected %2$d bytes, but found %3$d bytes.";
    private static final String MISSING_ACCESS_LATENCY =
        "Missing access latency for %1$s.";
    private static final String MISSING_RETENTION_POLICY =
        "Missing retention policy for %1$s.";
    private static final String EMPTY_FILE_IS_NEW_MSG =
        "Recovering: Removed empty file %1$s because name space entry is still new.";

    private final EnumSet<FileAttribute> REQUIRED_ATTRIBUTES =
            EnumSet.of(STORAGEINFO, LOCATIONS, ACCESS_LATENCY, RETENTION_POLICY, SIZE, CHECKSUM);

    private final PnfsHandler _pnfsHandler;
    private final ReplicaStore _replicaStore;
    private final ChecksumModule _checksumModule;
    private final ReplicaStatePolicy _replicaStatePolicy;
    private String _poolName;

    public ConsistentReplicaStore(PnfsHandler pnfsHandler,
                                  ChecksumModule checksumModule,
                                  ReplicaStore replicaStore,
                                  ReplicaStatePolicy replicaStatePolicy)
    {
        _pnfsHandler = pnfsHandler;
        _checksumModule = checksumModule;
        _replicaStore = replicaStore;
        _replicaStatePolicy = replicaStatePolicy;
    }

    public void setPoolName(String poolName)
    {
        if (poolName == null || poolName.isEmpty()) {
            throw new IllegalArgumentException("Invalid pool name");
        }
        _poolName = poolName;
    }

    public String getPoolName()
    {
        return _poolName;
    }

    @Override
    public void init() throws CacheException
    {
    }

    /**
     * Returns a collection of IDs of entries in the store. Removes
     * redundant meta data entries in the process.
     */
    @Override
    public Set<PnfsId> index(IndexOption... options) throws CacheException
    {
        return _replicaStore.index(options);
    }

    /**
     * Retrieves a CacheRepositoryEntry from the wrapped meta data
     * store. If the entry is missing or fails consistency checks, the
     * entry is reconstructed with information from PNFS.
     */
    @Override
    public ReplicaRecord get(PnfsId id)
        throws IllegalArgumentException, CacheException, InterruptedException
    {
        ReplicaRecord entry = _replicaStore.get(id);
        if (entry != null && isBroken(entry)) {
            _log.warn(String.format(RECOVERING_MSG, id));

            try {
                /* It is safe to remove FROM_STORE/FROM_POOL replicas: We have
                 * another copy anyway. Files in REMOVED or DESTROYED
                 * were about to be deleted, so we can finish the job.
                 */
                switch (entry.getState()) {
                case FROM_POOL:
                case FROM_STORE:
                case REMOVED:
                case DESTROYED:
                    _replicaStore.remove(id);
                    _pnfsHandler.clearCacheLocation(id);
                    _log.info(String.format(PARTIAL_FROM_TAPE_MSG, id));
                    return null;
                }

                entry = rebuildEntry(entry);
            } catch (IOException e) {
                throw new DiskErrorCacheException("I/O error in healer: " + e.getMessage(), e);
            } catch (FileNotFoundCacheException e) {
                _replicaStore.remove(id);
                _log.warn(String.format(FILE_NOT_FOUND_MSG, id));
                return null;
            } catch (FileIsNewCacheException e) {
                _replicaStore.remove(id);
                _log.warn("Recovering: Removed {}: {}", id, e.getMessage());
                return null;
            } catch (TimeoutCacheException e) {
                throw e;
            } catch (CacheException | NoSuchAlgorithmException e) {
                entry.update(r -> r.setState(ReplicaState.BROKEN));
                _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.BROKEN_FILE,
                                id.toString(),
                                _poolName),
                                String.format(BAD_MSG, id, e.getMessage()));
            }
        }

        return entry;
    }

    private boolean isBroken(ReplicaRecord entry) throws CacheException
    {
        FileAttributes attributes = entry.getFileAttributes();
        ReplicaState state = entry.getState();
        return !attributes.isDefined(FileAttribute.SIZE) ||
               attributes.getSize() != entry.getReplicaSize() ||
               state != ReplicaState.CACHED && state != ReplicaState.PRECIOUS;
    }

    private ReplicaRecord rebuildEntry(ReplicaRecord entry)
            throws CacheException, InterruptedException, IOException, NoSuchAlgorithmException
    {
           long length = entry.getReplicaSize();
           PnfsId id = entry.getPnfsId();

           ReplicaState state = entry.getState();

           _log.warn(String.format(FETCHED_STORAGE_INFO_FOR_1$S_FROM_PNFS, id));
           FileAttributes attributesInNameSpace = _pnfsHandler.getFileAttributes(id, REQUIRED_ATTRIBUTES);

           /* As a special case, empty files upon client upload are deleted, except if we already managed to
            * register it in the name space.
            */
           if (entry.getState() == ReplicaState.FROM_CLIENT && length == 0 && !attributesInNameSpace.getLocations().contains(_poolName)) {
               throw new FileIsNewCacheException("Client upload is empty.");
           }

            /* If the intended file size is known, then compare it
             * to the actual file size on disk. Fail in case of a
             * mismatch. Notice we do this before the checksum check,
             * as it is a lot cheaper than the checksum check and we
             * may thus safe some time for incomplete files.
             */
            if (attributesInNameSpace.isDefined(FileAttribute.SIZE) && attributesInNameSpace.getSize() != length) {
                String message = String.format(BAD_SIZE_MSG,
                                               id,
                                               attributesInNameSpace.getSize(),
                                               length);
                _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.BROKEN_FILE,
                                                        id.toString(),
                                                        _poolName),
                                                        message);
                throw new CacheException(message);
            }

            /* Verify checksum. Will fail if there is a mismatch.
             */
            Iterable<Checksum> expectedChecksums = attributesInNameSpace.getChecksumsIfPresent().or(Collections.emptySet());
            Iterable<Checksum> actualChecksums;
            if (_checksumModule != null &&
                    (_checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_WRITE) ||
                            _checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_TRANSFER) ||
                            _checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_RESTORE))) {
                try (RepositoryChannel channel = entry.openChannel(IoMode.READ)) {
                    actualChecksums = _checksumModule.verifyChecksum(channel, expectedChecksums);
                }
            } else {
                actualChecksums = Collections.emptySet();
            }

            /* We always register the file location.
             */
            FileAttributes attributesToUpdate = new FileAttributes();
            attributesToUpdate.setLocations(Collections.singleton(_poolName));


            /* If file size was not registered in the name space, we now replay the registration just as it would happen
             * in WriteHandleImpl. This includes initializing access latency, retention policy, and checksums.
             */
            if (state == ReplicaState.FROM_CLIENT || state == ReplicaState.BROKEN || state == ReplicaState.NEW) {
                if (attributesInNameSpace.isUndefined(ACCESS_LATENCY)) {
                    /* Access latency must have been injected by space manager, so we hope we still
                     * got it stored on the pool.
                     */
                    FileAttributes attributesOnPool = entry.getFileAttributes();
                    if (attributesOnPool.isUndefined(ACCESS_LATENCY)) {
                        String message = String.format(MISSING_ACCESS_LATENCY, id);
                        _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.BROKEN_FILE, id.toString(),
                                        _poolName), message);
                        throw new CacheException(message);
                    }

                    AccessLatency accessLatency = attributesOnPool.getAccessLatency();
                    attributesToUpdate.setAccessLatency(accessLatency);
                    attributesInNameSpace.setAccessLatency(accessLatency);
                    _log.warn(String.format(UPDATE_ACCESS_LATENCY_MSG, id, accessLatency));
                }

                if (attributesInNameSpace.isUndefined(RETENTION_POLICY)) {
                    /* Retention policy must have been injected by space manager, so we hope we still
                     * got it stored on the pool.
                     */
                    FileAttributes attributesOnPool = entry.getFileAttributes();
                    if (attributesOnPool.isUndefined(RETENTION_POLICY)) {
                        String message = String.format(MISSING_RETENTION_POLICY, id);
                        _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.BROKEN_FILE, id.toString(),
                                        _poolName), message);
                        throw new CacheException(message);
                    }

                    RetentionPolicy retentionPolicy = attributesOnPool.getRetentionPolicy();
                    attributesToUpdate.setRetentionPolicy(retentionPolicy);
                    attributesInNameSpace.setRetentionPolicy(retentionPolicy);

                    _log.warn(String.format(UPDATE_RETENTION_POLICY_MSG, id, retentionPolicy));
                }
                if (attributesInNameSpace.isUndefined(SIZE)) {
                    attributesToUpdate.setSize(length);
                    attributesInNameSpace.setSize(length);

                    _log.warn(String.format(UPDATE_SIZE_MSG, id, length));
                }
                if (!isEmpty(actualChecksums)) {
                    attributesToUpdate.setChecksums(Sets.newHashSet(actualChecksums));
                    attributesInNameSpace.setChecksums(
                            Sets.newHashSet(concat(expectedChecksums, actualChecksums)));
                    _log.warn(String.format(UPDATE_CHECKSUM_MSG, id, actualChecksums));
                }
            }

            /* Update file size, location, checksum, access_latency and
             * retention_policy within namespace.
             */
            _pnfsHandler.setFileAttributes(id, attributesToUpdate);

            /* Update the pool meta data.
             */
            /* If not already precious or cached, we move the entry to
             * the target state of a newly uploaded file.
             */
            if (state != ReplicaState.CACHED && state != ReplicaState.PRECIOUS) {
                ReplicaState targetState =
                        _replicaStatePolicy.getTargetState(attributesInNameSpace);
                List<StickyRecord> stickyRecords =
                        _replicaStatePolicy.getStickyRecords(attributesInNameSpace);
                entry.update(r -> {
                    r.setFileAttributes(attributesInNameSpace);
                    for (StickyRecord record : stickyRecords) {
                        r.setSticky(record.owner(), record.expire(), false);
                    }
                    r.setState(targetState);
                    return null;
                });
                _log.warn(String.format(MARKED_MSG, id, targetState));
            } else {
                entry.update(r -> r.setFileAttributes(attributesInNameSpace));
            }

            return entry;
    }

    /**
     * Creates a new entry. Fails if file already exists in the file
     * store. If the entry already exists in the meta data store, then
     * it is overwritten.
     */
    @Override
    public ReplicaRecord create(PnfsId id, Set<Repository.OpenFlags> flags)
        throws DuplicateEntryException, CacheException
    {
        return _replicaStore.create(id, flags);
    }

    /**
     * Calls through to the wrapped meta data store.
     */
    @Override
    public void remove(PnfsId id) throws CacheException
    {
        _replicaStore.remove(id);
    }

    /**
     * Calls through to the wrapped meta data store.
     */
    @Override
    public boolean isOk()
    {
        return _replicaStore.isOk();
    }

    @Override
    public void close()
    {
        _replicaStore.close();
    }

    @Override
    public String toString()
    {
        return _replicaStore.toString();
    }

    /**
     * Provides the amount of free space on the file system containing
     * the data files.
     */
    @Override
    public long getFreeSpace()
    {
        return _replicaStore.getFreeSpace();
    }

    /**
     * Provides the total amount of space on the file system
     * containing the data files.
     */
    @Override
    public long getTotalSpace()
    {
        return _replicaStore.getTotalSpace();
    }

    public ReplicaStore getStore()
    {
        return _replicaStore;
    }
}
