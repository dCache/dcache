package org.dcache.chimera.namespace;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileState;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;


public class ChimeraOsmStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ChimeraOsmStorageInfoExtractor.class);

    public ChimeraOsmStorageInfoExtractor(AccessLatency defaultAL,
                                          RetentionPolicy defaultRP) {
        super(defaultAL,defaultRP);
    }

    @Override
    public StorageInfo getFileStorageInfo(ExtendedInode inode)
            throws CacheException {
        try {
            Stat stat = inode.statCache();

            if (stat.getState() == FileState.CREATED) {
                StorageInfo info = getDirStorageInfo(inode);
                info.setIsNew(true);
                return info;
            }

            List<String> tapeLocations = inode.getLocations(StorageGenericLocation.TAPE);

            if (tapeLocations.isEmpty()) {
                StorageInfo info = getDirStorageInfo(inode);
                info.setIsNew(false);
                return info;
            }

            InodeStorageInformation inodeStorageInfo = inode.getStorageInfo();

            StorageInfo info = new OSMStorageInfo(inodeStorageInfo.storageGroup(),
                    inodeStorageInfo.storageSubGroup());

            for (String location : tapeLocations) {
                try {
                    info.addLocation(new URI(location));
                } catch (URISyntaxException e) {
                    LOGGER.debug("Ignoring bad tape location {}: {}", location, e.toString());
                }
            }

            info.setIsNew(false);
            return info;
        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }
    }

    @Override
    public StorageInfo getDirStorageInfo(ExtendedInode inode) throws CacheException {
        ExtendedInode dirInode;
        if (!inode.isDirectory()) {
            dirInode = inode.getParent();
            if (dirInode == null) {
                throw new FileNotFoundCacheException("file unlinked");
            }
        }
        else {
            dirInode = inode;
        }
        HashMap<String, String> hash = new HashMap<>();
        String store = null;
        ImmutableList<String> OSMTemplate = dirInode.getTag("OSMTemplate");
        if (!OSMTemplate.isEmpty()) {
            for (String line: OSMTemplate) {
                StringTokenizer st = new StringTokenizer(line);
                if (st.countTokens() < 2) {
                    continue;
                }
                hash.put(st.nextToken().intern(), st.nextToken());
            }
            store = hash.get("StoreName");
            if (store == null) {
                throw new CacheException(37, "StoreName not found in template");
            }
        }

        ImmutableList<String> sGroup = dirInode.getTag("sGroup");
        String group = getFirstLine(sGroup).map(String::intern).orElse(null);
        OSMStorageInfo info = new OSMStorageInfo(store, group);
        info.addKeys(hash);
        return info;
    }

}
