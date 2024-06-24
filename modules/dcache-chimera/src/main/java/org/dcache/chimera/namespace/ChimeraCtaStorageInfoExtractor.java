package org.dcache.chimera.namespace;

import com.google.common.collect.ImmutableList;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.CtaStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileState;
import org.dcache.chimera.StorageGenericLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChimeraCtaStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor {


    private static final Logger LOGGER = LoggerFactory.getLogger(
          ChimeraHsmStorageInfoExtractor.class);


    public ChimeraCtaStorageInfoExtractor(AccessLatency defaultAL,
                                          RetentionPolicy defaultRP) {
        super(defaultAL, defaultRP);
    }


    @Override
    public StorageInfo getFileStorageInfo(ExtendedInode inode) throws CacheException {
        try {

            CtaStorageInfo parentStorageInfo = (CtaStorageInfo) getDirStorageInfo(inode);

            List<String> locations = inode.
                getLocations(StorageGenericLocation.TAPE);

            if (locations.isEmpty()) {
                if (inode.statCache().getState() != FileState.CREATED) {
                    parentStorageInfo.setIsNew(false);
                }
                return parentStorageInfo;
            } else {
                StorageInfo info = new CtaStorageInfo(parentStorageInfo.getStorageGroup(),
                                                      parentStorageInfo.getFileFamily());
                info.setIsNew(false);
                for (String location : locations) {
                    try {
                        info.addLocation(new URI(location));
                    } catch (URISyntaxException e) {
                        LOGGER.debug("Ignoring bad tape location {}: {}",
                                     location, e.toString());
                    }
                }
                return info;
            }
        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }
    }

    @Override
    public StorageInfo getDirStorageInfo(ExtendedInode inode) throws CacheException {
        ExtendedInode directory = inode.isDirectory() ?
            inode : inode.getParent();

        if (directory == null) {
            throw new FileNotFoundCacheException("file unlinked");
        }

        ImmutableList<String> group = directory.getTag("storage_group");
        ImmutableList<String> family = directory.getTag("file_family");

        CtaStorageInfo info;

        if (!group.isEmpty()) {
            /**
             * Enstore
             */
            String sg = getFirstLine(group).map(String::intern).orElse("none");
            String ff = getFirstLine(family).map(String::intern).orElse("none");
            info = new CtaStorageInfo(sg, ff);
        } else {
            /**
             * OSM
             */
            List<String> osmTemplateTag = directory.getTag("OSMTemplate");

            Map<String, String> hash = new HashMap<>();
            osmTemplateTag.stream()
                .map(StringTokenizer::new)
                .filter(t -> t.countTokens() >= 2)
                .forEach(t -> hash.put(t.nextToken().intern(), t.nextToken()));

            String storeName = hash.get("StoreName");

            if (storeName == null && !osmTemplateTag.isEmpty()) {
                throw new CacheException(37, "StoreName not found in template");
            }

            List<String> sGroupTag = directory.getTag("sGroup");
            String sGroup = getFirstLine(sGroupTag).map(String::intern).orElse(null);
            info = new CtaStorageInfo(storeName, sGroup);
        }

        return info;
    }
}
