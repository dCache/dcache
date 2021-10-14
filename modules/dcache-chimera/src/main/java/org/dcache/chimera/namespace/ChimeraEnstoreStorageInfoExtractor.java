package org.dcache.chimera.namespace;

import com.google.common.base.Strings;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.EnstoreStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.StringTokenizer;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileState;
import org.dcache.chimera.StorageGenericLocation;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;


public class ChimeraEnstoreStorageInfoExtractor
      extends ChimeraHsmStorageInfoExtractor {

    public ChimeraEnstoreStorageInfoExtractor(AccessLatency defaultAL,
          RetentionPolicy defaultRP) {
        super(defaultAL, defaultRP);
    }

    @Override
    public StorageInfo getFileStorageInfo(ExtendedInode inode)
          throws CacheException {
        try {
            EnstoreStorageInfo info = getDirStorageInfo(inode);

            if (inode.stat().getState() == FileState.CREATED) {
                return info;
            }

            List<String> tapeLocations = inode.getLocations(StorageGenericLocation.TAPE);
            if (!tapeLocations.isEmpty()) {
                info.clearKeys();

                for (String location : tapeLocations) {
                    URI uri = UriComponentsBuilder.fromUriString(location)
                          .build(isEncoded(location)).toUri();
                    info.addLocation(uri);

                    String queryString = Strings.nullToEmpty(uri.getQuery());
                    for (String part : queryString.split("&")) {
                        String[] data = part.split(
                              "="); // REVISIT what if 'part' contains multiple '='?
                        String value = data.length == 2 ? data[1] : "";
                        switch (data[0]) {
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

            info.setIsNew(false);
            return info;
        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }
    }

    @Override
    public EnstoreStorageInfo getDirStorageInfo(ExtendedInode inode)
          throws CacheException {
        ExtendedInode directory = inode.isDirectory() ? inode : inode.getParent();

        if (directory == null) {
            throw new FileNotFoundCacheException("file unlinked");
        }

        List<String> groupTag = directory.getTag("storage_group");
        String storageGroup = getFirstLine(groupTag).map(String::intern).orElse("none");

        List<String> familyTag = directory.getTag("file_family");
        String fileFamily = getFirstLine(familyTag).map(String::intern).orElse("none");

        EnstoreStorageInfo info = new EnstoreStorageInfo(storageGroup, fileFamily);

        directory.getTag("OSMTemplate").stream()
              .map(StringTokenizer::new)
              .filter(t -> t.countTokens() >= 2)
              .forEach(t -> info.setKey(t.nextToken().intern(), t.nextToken()));

        return info;
    }

    private static boolean isEncoded(String s) {
        return !s.equals(UriUtils.decode(s, StandardCharsets.UTF_8));
    }

    @Override
    protected void checkFlushUpdate(StorageInfo info) throws CacheException {
        /* No checks needed: Enstore updates the namespace directly. */
    }
}
