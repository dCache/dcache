package org.dcache.chimera.namespace;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.dcache.chimera.FileState;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.EnstoreStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.posix.Stat;


public class ChimeraEnstoreStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor {

    public ChimeraEnstoreStorageInfoExtractor(AccessLatency defaultAL,
                                              RetentionPolicy defaultRP) {
        super(defaultAL,defaultRP);
    }

    @Override
    public StorageInfo getFileStorageInfo(ExtendedInode inode) throws CacheException {
        EnstoreStorageInfo info;

        try {
            Stat stat = inode.stat();
            boolean isNew = stat.getState() == FileState.CREATED || stat.getState() == FileState.LEGACY && stat.getSize() == 0 && !inode.getLevel(2).exists();

            info = (EnstoreStorageInfo) getDirStorageInfo(inode);
            if (!isNew) {
                List<String> locations = inode.getLocations(StorageGenericLocation.TAPE);
                if (!locations.isEmpty()) {
                    info = new EnstoreStorageInfo(info.getStorageGroup(), info.getFileFamily());
                    for (String location : locations) {
                        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(location);
                        URI uri = builder.build(isEncoded(location)).toUri();
                        info.addLocation(uri);
                        String queryString = uri.getQuery();
                        if (!Strings.isNullOrEmpty(queryString)) {
                            for (String part : uri.getQuery().split("&")) {
                                String[] data = part.split("=");
                                String key = data[0];
                                String value = (data.length == 2 ? data[1] : "");
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
                        }
                    }
                }
            }
            info.setIsNew(isNew);
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
            if (dirInode == null) {
                throw new FileNotFoundCacheException("file unlinked");
            }
        }
        else {
            dirInode = inode;
        }
        Map<String, String> hash = new HashMap<>();
        ImmutableList<String> OSMTemplate = dirInode.getTag("OSMTemplate");
        ImmutableList<String> group       = dirInode.getTag("storage_group");
        ImmutableList<String> family      = dirInode.getTag("file_family");

        for (String line: OSMTemplate) {
            StringTokenizer st = new StringTokenizer(line);
            if (st.countTokens() >= 2) {
                hash.put(st.nextToken().intern(), st.nextToken());
            }
        }
        String sg = getFirstLine(group).map(String::intern).orElse("none");
        String ff = getFirstLine(family).map(String::intern).orElse("none");
        EnstoreStorageInfo info = new EnstoreStorageInfo(sg,ff);
        info.addKeys(hash);
        return info;
    }

    private static boolean isEncoded(String s) {
        return !s.equals(UriUtils.decode(s, StandardCharsets.UTF_8));
    }

}
