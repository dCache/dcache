package org.dcache.chimera.namespace;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.store.InodeStorageInformation;

public abstract class ChimeraHsmStorageInfoExtractor implements
       ChimeraStorageInfoExtractable {

    private final static Logger LOGGER =
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
    public AccessLatency getAccessLatency(ExtendedInode inode) throws CacheException
    {
        try {
            if (!inode.exists()) {
                throw new FileNotFoundCacheException(inode.toString() + " does not exist");
            }

            ExtendedInode dirInode;
            if (inode.isDirectory()) {
                dirInode = inode;
            } else {
                Optional<AccessLatency> al = inode.getAccessLatency();
                if (al.isPresent()) {
                    return al.get();
                }
                dirInode = inode.getParent();
            }

            Optional<String> accessLatency = getFirstLine(dirInode.getTag("AccessLatency"));
            if (accessLatency.isPresent()) {
                try {
                    return AccessLatency.getAccessLatency(accessLatency.get());
                } catch (IllegalArgumentException e) {
                    LOGGER.error("Badly formatted AccessLatency tag in {}: {}", dirInode, e.getMessage());
                }
            }

            Optional<String> spaceToken = getFirstLine(dirInode.getTag("WriteToken"));
            if (spaceToken.isPresent() ) {
                return null;
            }
            return getDefaultAccessLatency();
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException(e.getMessage(), e);
        } catch (ChimeraFsException e) {
            throw new CacheException("Failed to obtain AccessLatency: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new CacheException(37, "Failed to obtain AccessLatency: " + e.getMessage(), e);
        }
    }

    @Override
    public RetentionPolicy getRetentionPolicy(ExtendedInode inode) throws CacheException
    {
        try {
            if (!inode.exists()) {
                throw new FileNotFoundCacheException(inode.toString() + " does not exists");
            }

            ExtendedInode dirInode;
            if (inode.isDirectory()) {
                dirInode = inode;
            } else {
                Optional<RetentionPolicy> rp = inode.getRetentionPolicy();
                if (rp.isPresent()) {
                    return rp.get();
                }
                dirInode = inode.getParent();
            }

            Optional<String> retentionPolicy = getFirstLine(dirInode.getTag("RetentionPolicy"));
            if (retentionPolicy.isPresent()) {
                try {
                    return RetentionPolicy.getRetentionPolicy(retentionPolicy.get());
                } catch (IllegalArgumentException e) {
                    LOGGER.error("Badly formatted RetentionPolicy tag in {}: {}", dirInode, e.getMessage());
                }
            }

            Optional<String> spaceToken = getFirstLine(dirInode.getTag("WriteToken"));
            if (spaceToken.isPresent() ) {
                return null;
            }

            return getDefaultRetentionPolicy();
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException(e.getMessage(), e);
        } catch (ChimeraFsException e) {
            throw new CacheException("Failed to obtain RetentionPolicy: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new CacheException(37, "Failed to obtain RetentionPolicy: " + e.getMessage(), e);
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
            if( !inode.exists() ) {
                throw new FileNotFoundCacheException(inode.toString() + " does not exist");
            }
        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }

        StorageInfo info;
        ExtendedInode dirInode;

        if (inode.isDirectory()) {
            info =  getDirStorageInfo(inode);
            dirInode = inode;
        } else {
            info =  getFileStorageInfo(inode);
            dirInode = inode.getParent();
        }

        try {
            // overwrite hsm type with hsmInstance tag
            Optional<String> hsmInstance = getFirstLine(dirInode.getTag("hsmInstance"));
            if (hsmInstance.isPresent()) {
                info.setHsm(hsmInstance.get());
            }

            Optional<String> cacheClass = getFirstLine(dirInode.getTag("cacheClass"));
            if (cacheClass.isPresent()) {
                info.setCacheClass(cacheClass.get());
            }

            Optional<String> spaceToken = getFirstLine(dirInode.getTag("WriteToken"));
            if (spaceToken.isPresent() ) {
                info.setKey("writeToken", spaceToken.get());
            }

            Optional<String> path = getFirstLine(dirInode.getTag("Path"));
            if (path.isPresent() ) {
                info.setKey("path", path.get());
            }
        } catch (IOException e) {
            throw new CacheException( 37, "Unable to fetch tags: " + e.getMessage());
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
            if(dCacheStorageInfo.isSetAddLocation() ) {
                List<URI> locationURIs = dCacheStorageInfo.locations();

                if( !locationURIs.isEmpty() ) {
                    InodeStorageInformation storageInfo = new InodeStorageInformation(inode,
                    dCacheStorageInfo.getHsm(),
                    dCacheStorageInfo.getKey("store"),
                    dCacheStorageInfo.getKey("group"));
                    inode.getFs().setStorageInfo(inode, storageInfo);
                }

                for(URI location : locationURIs) {
                    // skip bad URI's if the get here
                    if(location.toString().isEmpty()) {
                        continue;
                    }
                    HsmLocationExtractorFactory.validate(location);
                    inode.getFs().addInodeLocation(inode, StorageGenericLocation.TAPE, location.toString());
                }
            }

        }catch(FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException(e.getMessage());
        }catch(ChimeraFsException he ) {
            throw new CacheException(he.getMessage() );
        }
    }

    protected static Optional<String> getFirstLine(ImmutableList<String> lines)
    {
        if (!lines.isEmpty()) {
            String line = lines.get(0).trim();
            if (!line.isEmpty()) {
                return Optional.of(line);
            }
        }
        return Optional.absent();
    }
}
