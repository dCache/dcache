package org.dcache.chimera.namespace;

import static diskCacheV111.util.CacheException.INVALID_UPDATE;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ChimeraHsmStorageInfoExtractor implements
      ChimeraStorageInfoExtractable {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(ChimeraHsmStorageInfoExtractor.class);

    /**
     * default access latency for newly created files
     */
    private final AccessLatency _defaultAccessLatency;

    /**
     * default retention policy for newly created files
     */
    private final RetentionPolicy _defaultRetentionPolicy;


    public ChimeraHsmStorageInfoExtractor(AccessLatency defaultAL,
          RetentionPolicy defaultRP) {

        _defaultAccessLatency = defaultAL;
        _defaultRetentionPolicy = defaultRP;
    }

    public final AccessLatency getDefaultAccessLatency() {
        return _defaultAccessLatency;
    }

    public final RetentionPolicy getDefaultRetentionPolicy() {
        return _defaultRetentionPolicy;
    }

    @Override
    public AccessLatency getAccessLatency(ExtendedInode inode) throws CacheException {
        try {
            if (!inode.exists()) {
                throw new FileNotFoundCacheException(inode.toString() + " does not exist");
            }

            ExtendedInode dirInode;
            if (inode.isDirectory()) {
                dirInode = inode;
            } else {
                if (inode.statCache().isDefined(Stat.StatAttributes.ACCESS_LATENCY)) {
                    return inode.statCache().getAccessLatency();
                }
                dirInode = inode.getParent();
                if (dirInode == null) {
                    throw new FileNotFoundCacheException("File " + inode + " has been deleted.");
                }
            }

            Optional<String> accessLatency = getFirstLine(dirInode.getTag("AccessLatency"));
            if (accessLatency.isPresent()) {
                try {
                    return AccessLatency.getAccessLatency(accessLatency.get());
                } catch (IllegalArgumentException e) {
                    LOGGER.error("Badly formatted AccessLatency tag in {}: {}", dirInode,
                          e.getMessage());
                }
            }

            Optional<String> spaceToken = getFirstLine(dirInode.getTag("WriteToken"));
            if (spaceToken.isPresent()) {
                return null;
            }
            return getDefaultAccessLatency();
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException(e.getMessage(), e);
        } catch (ChimeraFsException e) {
            throw new CacheException("Failed to obtain AccessLatency: " + e.getMessage(), e);
        }
    }

    @Override
    public RetentionPolicy getRetentionPolicy(ExtendedInode inode) throws CacheException {
        try {
            if (!inode.exists()) {
                throw new FileNotFoundCacheException(inode.toString() + " does not exists");
            }

            ExtendedInode dirInode;
            if (inode.isDirectory()) {
                dirInode = inode;
            } else {
                if (inode.statCache().isDefined(Stat.StatAttributes.RETENTION_POLICY)) {
                    return inode.statCache().getRetentionPolicy();
                }
                dirInode = inode.getParent();
                if (dirInode == null) {
                    throw new FileNotFoundCacheException("File " + inode + " has been deleted.");
                }
            }

            Optional<String> retentionPolicy = getFirstLine(dirInode.getTag("RetentionPolicy"));
            if (retentionPolicy.isPresent()) {
                try {
                    return RetentionPolicy.getRetentionPolicy(retentionPolicy.get());
                } catch (IllegalArgumentException e) {
                    LOGGER.error("Badly formatted RetentionPolicy tag in {}: {}", dirInode,
                          e.getMessage());
                }
            }

            Optional<String> spaceToken = getFirstLine(dirInode.getTag("WriteToken"));
            if (spaceToken.isPresent()) {
                return null;
            }

            return getDefaultRetentionPolicy();
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException(e.getMessage(), e);
        } catch (ChimeraFsException e) {
            throw new CacheException("Failed to obtain RetentionPolicy: " + e.getMessage(), e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.StorageInfoExtractable#getFileAttributes(java.lang.String,
     *      diskCacheV111.util.PnfsId)
     */

    @Override
    public StorageInfo getStorageInfo(ExtendedInode inode)
          throws CacheException {

        try {
            if (!inode.exists()) {
                throw new FileNotFoundCacheException(inode.toString() + " does not exist");
            }
        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }

        StorageInfo info;
        ExtendedInode dirInode;

        if (inode.isDirectory()) {
            info = getDirStorageInfo(inode);
            dirInode = inode;
        } else {
            dirInode = inode.getParent();
            if (dirInode == null) {
                throw new FileNotFoundCacheException("File " + inode + " has been deleted.");
            }
            info = getFileStorageInfo(inode);
        }

        // overwrite hsm type with hsmInstance tag
        Optional<String> hsmInstance = getFirstLine(dirInode.getTag("hsmInstance"));
        if (hsmInstance.isPresent()) {
            info.setHsm(hsmInstance.get().intern());
        }

        Optional<String> cacheClass = getFirstLine(dirInode.getTag("cacheClass"));
        if (cacheClass.isPresent()) {
            info.setCacheClass(cacheClass.get().intern());
        }

        Optional<String> spaceToken = getFirstLine(dirInode.getTag("WriteToken"));
        if (spaceToken.isPresent()) {
            info.setKey("writeToken", spaceToken.get());
        }

        Optional<String> path = getFirstLine(dirInode.getTag("Path"));
        if (path.isPresent()) {
            info.setKey("path", path.get());
        }

        return info;
    }

    public abstract StorageInfo getFileStorageInfo(ExtendedInode inode) throws CacheException;

    public abstract StorageInfo getDirStorageInfo(ExtendedInode inode) throws CacheException;

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.StorageInfoExtractable#setStorageInfo(java.lang.String,
     *      diskCacheV111.util.PnfsId, diskCacheV111.vehicles.StorageInfo, int)
     */
    @Override
    public void setStorageInfo(FsInode inode, StorageInfo dCacheStorageInfo) throws CacheException {

        try {
            if (dCacheStorageInfo.isSetAddLocation()) {
                checkFlushUpdate(dCacheStorageInfo);

                List<URI> locationURIs = dCacheStorageInfo.locations();

                if (!locationURIs.isEmpty()) {
                    InodeStorageInformation storageInfo = new InodeStorageInformation(inode,
                          dCacheStorageInfo.getHsm(),
                          dCacheStorageInfo.getKey("store"),
                          dCacheStorageInfo.getKey("group"));
                    inode.getFs().setStorageInfo(inode, storageInfo);
                }

                for (URI location : locationURIs) {
                    // skip bad URI's if the get here
                    if (location.toString().isEmpty()) {
                        continue;
                    }
                    HsmLocationExtractorFactory.validate(location);
                    inode.getFs().addInodeLocation(inode, StorageGenericLocation.TAPE,
                          location.toString());
                }
            }

        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException(e.getMessage());
        } catch (ChimeraFsException he) {
            throw new CacheException(he.getMessage());
        }
    }

    protected void checkFlushUpdate(StorageInfo info) throws CacheException {
        List<URI> locations = info.locations();

        if (locations.isEmpty()) {
            throw new CacheException(INVALID_UPDATE, "Flush was successful but"
                  + " no extra (tape) locations were reported.");
        }
    }

    protected static Optional<String> getFirstLine(List<String> lines) {
        if (!lines.isEmpty()) {
            String line = lines.get(0).trim();
            if (!line.isEmpty()) {
                return Optional.of(line);
            }
        }
        return Optional.empty();
    }
}
