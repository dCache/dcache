package org.dcache.pool.repository;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.classic.ReplicaStatePolicy;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.isEmpty;
import static org.dcache.namespace.FileAttribute.*;

/**
 * Wrapper for a MetaDataStore which encapsulates the logic for
 * recovering MetaDataRecord objects from PnfsManager in case they are
 * missing or broken in a MetaDataStore.
 *
 * Warning: The class is only thread-safe as long as its methods are
 * not invoked concurrently on the same PNFS ID.
 */
public class ConsistentStore
    implements MetaDataStore
{
    private final static Logger _log = LoggerFactory.getLogger(ConsistentStore.class);

    private final static String RECOVERING_MSG =
        "Recovering %1$s...";
    private final static String MISSING_MSG =
        "Recovering: Reconstructing meta data for %1$s.";
    private final static String PARTIAL_FROM_TAPE_MSG =
        "Recovering: Removed %1$s because it was not fully staged.";
    private final static String FETCHED_STORAGE_INFO_FOR_1$S_FROM_PNFS =
        "Recovering: Fetched storage info for %1$s from name space.";
    private final static String FILE_NOT_FOUND_MSG =
        "Recovering: Removed %1$s because name space entry was deleted.";
    private final static String UPDATE_SIZE_MSG =
        "Recovering: Setting size of %1$s in name space to %2$d.";
    private final static String UPDATE_ACCESS_LATENCY_MSG =
        "Recovering: Setting access latency of %1$s in name space to %2$s.";
    private final static String UPDATE_RETENTION_POLICY_MSG =
        "Recovering: Setting retention policy of %1$s in name space to %2$s.";
    private final static String UPDATE_CHECKSUM_MSG =
        "Recovering: Setting checksum of %1$s in name space to %2$s.";
    private final static String MARKED_MSG =
        "Recovering: Marked %1$s as %2$s.";
    private final static String REMOVING_REDUNDANT_META_DATA =
        "Removing redundant meta data for %s.";

    private final static String BAD_MSG =
        "Marked %1$s bad: %2$s.";
    private final static String BAD_SIZE_MSG =
        "File size mismatch for %1$s. Expected %2$d bytes, but found %3$d bytes.";
    private final static String MISSING_ACCESS_LATENCY =
        "Missing access latency for %1$s.";
    private final static String MISSING_RETENTION_POLICY =
        "Missing retention policy for %1$s.";

    private final EnumSet<FileAttribute> REQUIRED_ATTRIBUTES =
            EnumSet.of(STORAGEINFO, ACCESS_LATENCY, RETENTION_POLICY, SIZE, CHECKSUM);

    private final PnfsHandler _pnfsHandler;
    private final MetaDataStore _metaDataStore;
    private final FileStore _fileStore;
    private final ChecksumModule _checksumModule;
    private final ReplicaStatePolicy _replicaStatePolicy;
    private String _poolName;

    public ConsistentStore(PnfsHandler pnfsHandler,
                           ChecksumModule checksumModule,
                           FileStore fileStore,
                           MetaDataStore metaDataStore,
                           ReplicaStatePolicy replicaStatePolicy)
    {
        _pnfsHandler = pnfsHandler;
        _checksumModule = checksumModule;
        _fileStore = fileStore;
        _metaDataStore = metaDataStore;
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

    /**
     * Returns a collection of IDs of entries in the store. Removes
     * redundant meta data entries in the process.
     */
    @Override
    public synchronized Collection<PnfsId> list() throws CacheException
    {
        Collection<PnfsId> files = _fileStore.list();
        Collection<PnfsId> records = _metaDataStore.list();
        records.removeAll(new HashSet<>(files));
        for (PnfsId id: records) {
            _log.warn(String.format(REMOVING_REDUNDANT_META_DATA, id));
            _metaDataStore.remove(id);
        }
        return files;
    }

    /**
     * Retrieves a CacheRepositoryEntry from the wrapped meta data
     * store. If the entry is missing or fails consistency checks, the
     * entry is reconstructed with information from PNFS.
     */
    @Override
    public MetaDataRecord get(PnfsId id)
        throws IllegalArgumentException, CacheException, InterruptedException
    {
        File file = _fileStore.get(id);
        if (!file.isFile()) {
            return null;
        }

        MetaDataRecord entry = _metaDataStore.get(id);

        if (isBroken(entry)) {
            _log.warn(String.format(RECOVERING_MSG, id));

            if (entry == null) {
                entry = _metaDataStore.create(id);
                _log.warn(String.format(MISSING_MSG, id));
            }

            try {
                /* It is safe to remove FROM_STORE files: We have a
                 * copy on HSM anyway. Files in REMOVED or DESTROYED
                 * where about to be deleted, so we can finish the
                 * job.
                 */
                switch (entry.getState()) {
                case FROM_STORE:
                case REMOVED:
                case DESTROYED:
                    delete(id, file);
                    _log.info(String.format(PARTIAL_FROM_TAPE_MSG, id));
                    return null;
                }

                entry = rebuildEntry(entry);
            } catch (IOException e) {
                throw new DiskErrorCacheException("I/O error in healer: " + e.getMessage());
            } catch (CacheException e) {
                switch (e.getRc()) {
                case CacheException.FILE_NOT_FOUND:
                    delete(id, file);
                    _log.warn(String.format(FILE_NOT_FOUND_MSG, id));
                    return null;

                case CacheException.TIMEOUT:
                    throw e;

                default:
                    entry.setState(EntryState.BROKEN);
                    _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.BROKEN_FILE,
                                                            id.toString(),
                                                            _poolName),
                               String.format(BAD_MSG, id, e.getMessage()));
                    break;
                }
            } catch (NoSuchAlgorithmException e) {
                entry.setState(EntryState.BROKEN);
                _log.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.BROKEN_FILE,
                                                        id.toString(),
                                                        _poolName),
                                String.format(BAD_MSG, id, e.getMessage()));
            }
        }

        return entry;
    }

    private boolean isBroken(MetaDataRecord entry) throws CacheException
    {
        boolean isBroken = true;
        if (entry != null) {
            FileAttributes attributes = entry.getFileAttributes();
            EntryState state = entry.getState();
            if (attributes.isDefined(FileAttribute.SIZE)
                    && attributes.getSize() == entry.getSize()
                    && (state == EntryState.CACHED || state == EntryState.PRECIOUS)) {
                isBroken = false;
            }
        }
        return isBroken;
    }

    private MetaDataRecord rebuildEntry(MetaDataRecord entry)
            throws CacheException, InterruptedException, IOException, NoSuchAlgorithmException
    {

               PnfsId id = entry.getPnfsId();

               EntryState state = entry.getState();
               if (state == EntryState.BROKEN) {
                   /* We replay file registration for BROKEN files.
                    */
                   state = EntryState.FROM_CLIENT;
               }

               _log.warn(String.format(FETCHED_STORAGE_INFO_FOR_1$S_FROM_PNFS, id));
               FileAttributes attributesInNameSpace = _pnfsHandler.getFileAttributes(id, REQUIRED_ATTRIBUTES);

                /* If the intended file size is known, then compare it
                 * to the actual file size on disk. Fail in case of a
                 * mismatch. Notice we do this before the checksum check,
                 * as it is a lot cheaper than the checksum check and we
                 * may thus safe some time for incomplete files.
                 */
                long length = entry.getDataFile().length();
                if (attributesInNameSpace.isDefined(FileAttribute.SIZE)
                    && (state != EntryState.FROM_CLIENT || attributesInNameSpace.getSize() != 0)
                    && attributesInNameSpace.getSize() != length) {
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
                Iterable<Checksum> expectedChecksums = attributesInNameSpace.getChecksumsIfPresent().or(Collections.<Checksum>emptySet());
                Iterable<Checksum> actualChecksums;
                if (_checksumModule != null &&
                        (_checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_WRITE) ||
                                _checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_TRANSFER) ||
                                _checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_RESTORE))) {
                    actualChecksums = _checksumModule.verifyChecksum(entry.getDataFile(), expectedChecksums);
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
                if (state == EntryState.FROM_CLIENT) {
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
                    if (attributesInNameSpace.isUndefined(SIZE) || attributesInNameSpace.getSize() == 0) {
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
                entry.setFileAttributes(attributesInNameSpace);

                /* If not already precious or cached, we move the entry to
                 * the target state of a newly uploaded file.
                 */
                if (state != EntryState.CACHED && state != EntryState.PRECIOUS) {
                    EntryState targetState =
                        _replicaStatePolicy.getTargetState(attributesInNameSpace);
                    List<StickyRecord> stickyRecords =
                        _replicaStatePolicy.getStickyRecords(attributesInNameSpace);

                    for (StickyRecord record: stickyRecords) {
                        entry.setSticky(record.owner(), record.expire(), false);
                    }

                    entry.setState(targetState);
                    _log.warn(String.format(MARKED_MSG, id, targetState));
                }

                return entry;
    }

    /**
     * Creates a new entry. Fails if file already exists in the file
     * store. If the entry already exists in the meta data store, then
     * it is overwritten.
     */
    @Override
    public MetaDataRecord create(PnfsId id)
        throws DuplicateEntryException, CacheException
    {
        if (_log.isInfoEnabled()) {
            _log.info("Creating new entry for " + id);
        }

        /* Fail if file already exists.
         */
        File dataFile = _fileStore.get(id);
        if (dataFile.exists()) {
            throw new DuplicateEntryException(id);
        }

        /* Create meta data record. Recreate if it already exists.
         */
        MetaDataRecord entry;
        try {
            entry = _metaDataStore.create(id);
        } catch (DuplicateEntryException e) {
            _log.warn("Deleting orphaned meta data entry for " + id);
            _metaDataStore.remove(id);
            try {
                entry = _metaDataStore.create(id);
            } catch (DuplicateEntryException f) {
                throw new DiskErrorCacheException("Unexpected repository error", e);
            }
        }

        return entry;
    }

    /**
     * Calls through to the wrapped meta data store.
     */
    @Override
    public MetaDataRecord create(MetaDataRecord entry)
        throws DuplicateEntryException, CacheException
    {
        return _metaDataStore.create(entry);
    }

    /**
     * Calls through to the wrapped meta data store.
     */
    @Override
    public void remove(PnfsId id) throws CacheException
    {
        File f = _fileStore.get(id);
        if (!f.delete() && f.exists()) {
            _log.error("Failed to delete {}", f);
            throw new RuntimeException("Failed to delete " + id + " on " + _poolName);
        }
        _metaDataStore.remove(id);
    }

    /**
     * Calls through to the wrapped meta data store.
     */
    @Override
    public boolean isOk()
    {
        return _fileStore.isOk() && _metaDataStore.isOk();
    }

    @Override
    public void close()
    {
        _metaDataStore.close();
    }

    @Override
    public String toString()
    {
        return String.format("[data=%s;meta=%s]", _fileStore, _metaDataStore);
    }

    /**
     * Provides the amount of free space on the file system containing
     * the data files.
     */
    @Override
    public long getFreeSpace()
    {
        return _metaDataStore.getFreeSpace();
    }

    /**
     * Provides the total amount of space on the file system
     * containing the data files.
     */
    @Override
    public long getTotalSpace()
    {
        return _metaDataStore.getTotalSpace();
    }

    private void delete(PnfsId id, File file) throws CacheException
    {
        _metaDataStore.remove(id);
        if (!file.delete() && file.exists()) {
            _log.error("Failed to delete {}" , file);
        }
        _pnfsHandler.clearCacheLocation(id);
    }

}
