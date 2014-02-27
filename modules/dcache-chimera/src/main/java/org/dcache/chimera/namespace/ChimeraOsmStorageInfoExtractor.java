package org.dcache.chimera.namespace;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.StringTokenizer;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;


public class ChimeraOsmStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor {

    public ChimeraOsmStorageInfoExtractor(AccessLatency defaultAL,
                                          RetentionPolicy defaultRP) {
        super(defaultAL,defaultRP);
    }

    @Override
    public StorageInfo getFileStorageInfo(ExtendedInode inode) throws CacheException {

        OSMStorageInfo info;

        try {
            Stat stat = inode.statCache();
            ExtendedInode level2 = inode.getLevel(2);

            boolean isNew = (stat.getSize() == 0) && (!level2.exists());

            if (!isNew) {
                ImmutableList<String> locations = inode.getLocations(StorageGenericLocation.TAPE);

                if (locations.isEmpty()) {
                    info = (OSMStorageInfo) getDirStorageInfo(inode);
                } else {
                    InodeStorageInformation inodeStorageInfo = inode.getStorageInfo();

                    info = new OSMStorageInfo(inodeStorageInfo.storageGroup(),
                            inodeStorageInfo.storageSubGroup());

                    for (String location : locations) {
                        try {
                            info.addLocation(new URI(location));
                        } catch (URISyntaxException e) {
                            // bad URI
                        }
                    }
                }
            } else {
                info = (OSMStorageInfo) getDirStorageInfo(inode);
            }

            info.setIsNew(isNew);

        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }
        return info;
    }

    @Override
    public StorageInfo getDirStorageInfo(ExtendedInode inode) throws CacheException {
        ExtendedInode dirInode;
        if (!inode.isDirectory()) {
            dirInode = inode.getParent();
        }
        else {
            dirInode = inode;
        }
        try {
            HashMap<String, String> hash = new HashMap<>();
            String store = null;
            ImmutableList<String> OSMTemplate = dirInode.getTag("OSMTemplate");
            if (!OSMTemplate.isEmpty()) {
                for (String line: OSMTemplate) {
                    StringTokenizer st = new StringTokenizer(line);
                    if (st.countTokens() < 2) {
                        continue;
                    }
                    hash.put(st.nextToken(), st.nextToken());
                }
                store = hash.get("StoreName");
                if (store == null) {
                    throw new CacheException(37, "StoreName not found in template");
                }
            }

            ImmutableList<String> sGroup = dirInode.getTag("sGroup");
            String group = getFirstLine(sGroup).orNull();
            OSMStorageInfo info = new OSMStorageInfo(store, group);
            info.addKeys(hash);
            return info;
        }
        catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

}
