package org.dcache.chimera.namespace;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;


public class ChimeraOsmStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor {

    public ChimeraOsmStorageInfoExtractor(AccessLatency defaultAL,
                                          RetentionPolicy defaultRP) {
        super(defaultAL,defaultRP);
    }

    @Override
    public StorageInfo getFileStorageInfo(FsInode inode) throws CacheException {

        OSMStorageInfo info;

        try {
            Stat stat = inode.statCache();
            FsInode level2 = new FsInode(inode.getFs(), inode.toString(), 2);

            boolean isNew = (stat.getSize() == 0) && (!level2.exists());

            if (!isNew) {
                List<StorageLocatable> locations = inode.getFs().getInodeLocations(inode, StorageGenericLocation.TAPE);

                if (locations.isEmpty()) {
                    info = (OSMStorageInfo) getDirStorageInfo(inode);
                } else {
                    InodeStorageInformation inodeStorageInfo = inode.getFs().getStorageInfo(inode);

                    info = new OSMStorageInfo(inodeStorageInfo.storageGroup(),
                            inodeStorageInfo.storageSubGroup());

                    for (StorageLocatable location : locations) {
                        if (location.isOnline()) {
                            try {
                                info.addLocation(new URI(location.location()));
                            } catch (URISyntaxException e) {
                                // bad URI
                            }
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
    public StorageInfo getDirStorageInfo(FsInode inode) throws CacheException {
        FsInode dirInode;
        if (!inode.isDirectory()) {
            dirInode = inode.getParent();
        }
        else {
            dirInode = inode;
        }
        try {
            HashMap<String, String> hash = new HashMap<>();
            String store = null;
            String group = null;
            String[] OSMTemplate = getTag(dirInode, "OSMTemplate");
            if (OSMTemplate != null) {
                for ( String line: OSMTemplate) {
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
            String [] sGroup = getTag(dirInode, "sGroup");
            if( sGroup != null ) {
                group = sGroup[0].trim();
            }
            OSMStorageInfo info = new OSMStorageInfo(store, group);
            info.addKeys(hash);
            return info;
        }
        catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

}
