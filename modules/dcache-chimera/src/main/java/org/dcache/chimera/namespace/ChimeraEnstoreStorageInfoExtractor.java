package org.dcache.chimera.namespace;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInode_TAG;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.vehicles.EnstoreStorageInfo;
import diskCacheV111.vehicles.StorageInfo;


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
                AccessLatency al = inode.getFs().getAccessLatency(inode);
                if(al != null) {
                    info.setAccessLatency(AccessLatency.getAccessLatency(al.getId()));
                }
                RetentionPolicy rp = inode.getFs().getRetentionPolicy(inode);
                if( rp != null ) {
                    info.setRetentionPolicy(RetentionPolicy.getRetentionPolicy(rp.getId()));
                }
            }
            else {
                InodeStorageInformation inodeStorageInfo = inode.getFs().getSorageInfo(inode);
                info = new EnstoreStorageInfo(parentStorageInfo.getStorageGroup(),
                                              parentStorageInfo.getFileFamily());
                info.setIsNew(false);
                info.setAccessLatency(inodeStorageInfo.accessLatency());
                info.setRetentionPolicy(inodeStorageInfo.retentionPolicy());
                for(StorageLocatable location: locations) {
                    if( location.isOnline() ) {
                        try {
                            URI uri = new URI(location.location());
                            info.addLocation(uri);
                            for (String part : uri.getQuery().split("&")) {
                                String[] data = part.split("=");
                                String key    = data[0];
                                String value  = (data.length == 2 ? data[1] : "");
                                if (key.equals("bfid")) {
                                    info.setBitfileId(value);
                                }
                                else if (key.equals("volume")) {
                                    info.setVolume(value);
                                }
                                else if (key.equals("location_cookie")) {
                                    info.setLocation(value);
                                }
                                else if (key.equals("original_name")) {
                                    info.setPath(value);
                                }
                            }
                        }
                        catch (URISyntaxException e) {
                            // bad URI
                        }
                    }
                }
            }
        }
        catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }
        try {
            stat = inode.stat();
        }
        catch( ChimeraFsException hfe) {
            throw new CacheException(hfe.getMessage());
        }
        info.setFileSize(stat.getSize());
        info.setIsNew((stat.getSize() == 0) && (!level2.exists()));
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
            HashMap<String, String> hash = new HashMap<String, String>();
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
            String[] accessLatency   = getTag(dirInode, "AccessLatency");
            String[] retentionPolicy = getTag(dirInode, "RetentionPolicy");
            if(accessLatency != null) {
                try {
                    info.setAccessLatency(AccessLatency.getAccessLatency(accessLatency[0].trim()));
                    info.isSetAccessLatency(true);
                }
                catch(IllegalArgumentException iae) {
                }
            }
            else{
                info.setAccessLatency(getDefaultAccessLatency());
            }

            if(retentionPolicy != null) {
                try {
                    info.setRetentionPolicy(RetentionPolicy.getRetentionPolicy(retentionPolicy[0].trim()));
                    info.isSetRetentionPolicy(true);
                }
                catch(IllegalArgumentException iae) {
                }
            }
            else{
                info.setRetentionPolicy(getDefaultRetentionPolicy());
            }
            return info;
        }
        catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

}