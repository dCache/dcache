package org.dcache.chimera.namespace;

import com.google.common.collect.ImmutableList;
import org.springframework.web.util.UriComponentsBuilder;

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
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;


public class ChimeraEnstoreStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor {

    public ChimeraEnstoreStorageInfoExtractor(AccessLatency defaultAL,
                                              RetentionPolicy defaultRP) {
        super(defaultAL,defaultRP);
    }

    @Override
    public StorageInfo getFileStorageInfo(ExtendedInode inode) throws CacheException {
        EnstoreStorageInfo info;
        Stat stat;
        ExtendedInode level2 = inode.getLevel(2);
        try {
            List<String> locations = inode.getLocations(StorageGenericLocation.TAPE);
            EnstoreStorageInfo parentStorageInfo = (EnstoreStorageInfo) getDirStorageInfo(inode);
            if (locations.isEmpty()) {
                info = parentStorageInfo;
            }
            else {
                InodeStorageInformation inodeStorageInfo = inode.getFs().getStorageInfo(inode);
                info = new EnstoreStorageInfo(parentStorageInfo.getStorageGroup(),
                                              parentStorageInfo.getFileFamily());
                info.setIsNew(false);
                for(String location: locations) {
                    UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(location);
                    try {
                        URI uri = builder.build(isEncoded(location)).toUri();
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
    public StorageInfo getDirStorageInfo(ExtendedInode inode) throws CacheException {
        ExtendedInode dirInode;
        if (!inode.isDirectory()) {
            dirInode = inode.getParent();
        }
        else {
            dirInode = inode;
        }
        try {
            Map<String, String> hash = new HashMap<>();
            ImmutableList<String> OSMTemplate = dirInode.getTag("OSMTemplate");
            ImmutableList<String> group       = dirInode.getTag("storage_group");
            ImmutableList<String> family      = dirInode.getTag("file_family");

            for (String line: OSMTemplate) {
                StringTokenizer st = new StringTokenizer(line);
                if (st.countTokens() >= 2) {
                    hash.put(st.nextToken(), st.nextToken());
                }
            }
            String sg = getFirstLine(group).or("none");
            String ff = getFirstLine(family).or("none");
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
