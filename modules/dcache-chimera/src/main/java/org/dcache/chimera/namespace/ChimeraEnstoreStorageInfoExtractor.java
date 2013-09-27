package org.dcache.chimera.namespace;

import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriComponents;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.EnstoreStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;


public class ChimeraEnstoreStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor {

    public ChimeraEnstoreStorageInfoExtractor(AccessLatency defaultAL,
                                              RetentionPolicy defaultRP) {
        super(defaultAL,defaultRP);
    }

    @Override
    public StorageInfo getFileStorageInfo(FsInode inode) throws CacheException {
        EnstoreStorageInfo info;
        Stat stat;
        FsInode level2 = new FsInode(inode.getFs(), inode.toString(), 2);
        try {
            List<StorageLocatable> locations = inode.getFs().getInodeLocations(inode,
                                                                               StorageGenericLocation.TAPE );
            EnstoreStorageInfo parentStorageInfo = (EnstoreStorageInfo) getDirStorageInfo(inode);
            if (locations.isEmpty()) {
                info = parentStorageInfo;
            }
            else {
                InodeStorageInformation inodeStorageInfo = inode.getFs().getStorageInfo(inode);
                info = new EnstoreStorageInfo(parentStorageInfo.getStorageGroup(),
                                              parentStorageInfo.getFileFamily());
                info.setIsNew(false);
                for(StorageLocatable location: locations) {
                    if (!location.isOnline()) {
                        continue;
                    }
                    String locationStr = location.location();
                    UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(locationStr);
                    try {
                        URI uri = builder.build(isEncoded(locationStr)).toUri();
                        info.addLocation(uri);
                        for (String part : uri.getQuery().split("&")) {
                            String[] data = part.split("=");
                            String key    = data[0];
                            String value  = (data.length == 2 ? data[1] : "");
                            switch (key) {
                            case "bfid":
                                info.setBitfileId(value);
                                break;
                            case "volume":
                                info.setVolume(value);
                                break;
                            case "location_cookie":
                                info.setLocation(value);
                                break;
                            case "original_name":
                                info.setPath(value);
                                break;
                            }
                        }
                    } catch (UnsupportedEncodingException ignore) {
                    }
                }
            }
            stat = inode.stat();
            info.setIsNew((stat.getSize() == 0) && (!level2.exists()));
        }
        catch (ChimeraFsException e) {
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
            Map<String, String> hash = new HashMap<>();
            String [] OSMTemplate = getTag(dirInode,"OSMTemplate") ;
            String [] group       = getTag(dirInode,"storage_group" ) ;
            String [] family      = getTag(dirInode,"file_family" ) ;

            if (OSMTemplate != null) {
                for ( String line: OSMTemplate) {
                    StringTokenizer st = new StringTokenizer(line);
                    if (st.countTokens() < 2) {
                        continue;
                    }
                    hash.put(st.nextToken(), st.nextToken());
                }
            }
            String sg = (group==null||group.length == 0)||(sg=group[0].trim()).equals("") ? "none" : sg;
            String ff = (family==null||family.length == 0)||(ff=family[0].trim()).equals("") ? "none" : ff;
            EnstoreStorageInfo info = new EnstoreStorageInfo(sg,ff);
            info.addKeys(hash);
            return info;
        }
        catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

    private static boolean isEncoded(String s) throws UnsupportedEncodingException {
        return !s.equals(URLDecoder.decode(s,"UTF-8"));
    }
}
