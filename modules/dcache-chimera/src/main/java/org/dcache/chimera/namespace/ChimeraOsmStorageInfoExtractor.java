package org.dcache.chimera.namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            if (inode.statCache().getState() == FileState.CREATED) {
                return getDirStorageInfo(inode);
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
    public StorageInfo getDirStorageInfo(ExtendedInode inode)
            throws CacheException {
        ExtendedInode directory = inode.isDirectory() ? inode: inode.getParent();

        if (directory == null) {
            throw new FileNotFoundCacheException("file unlinked");
        }

        List<String> osmTemplateTag = directory.getTag("OSMTemplate");

        Map<String, String> hash = new HashMap<>();
        osmTemplateTag.stream()
                .map(StringTokenizer::new)
                .filter(t -> t.countTokens() >= 2)
                .forEach(t -> hash.put(t.nextToken().intern(), t.nextToken()));

        String store = hash.get("StoreName");

        if (store == null && !osmTemplateTag.isEmpty()) {
            throw new CacheException(37, "StoreName not found in template");
        }

        List<String> sGroupTag = directory.getTag("sGroup");
        String group = getFirstLine(sGroupTag).map(String::intern).orElse(null);

        OSMStorageInfo info = new OSMStorageInfo(store, group);
        info.addKeys(hash);
        return info;
    }
}
