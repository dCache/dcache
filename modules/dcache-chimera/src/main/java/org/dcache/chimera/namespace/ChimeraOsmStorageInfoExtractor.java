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
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;


public class ChimeraOsmStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor {

    public ChimeraOsmStorageInfoExtractor(AccessLatency defaultAL,
                                          RetentionPolicy defaultRP) {
        super(defaultAL,defaultRP);
    }

    @Override
    public StorageInfo getFileStorageInfo(FsInode inode) throws CacheException {

        OSMStorageInfo info;

        Stat stat;
        FsInode level2 = new FsInode(inode.getFs(), inode.toString(), 2);

        try {

            List<StorageLocatable> locations = inode.getFs().getInodeLocations(inode, StorageGenericLocation.TAPE );

            if ( locations.isEmpty() ) {
                info = (OSMStorageInfo) getDirStorageInfo(inode);
                AccessLatency al = inode.getFs().getAccessLatency(inode);

                /*
                 * currently storage class and AL and RP are asymmetric:
                 *   storage class of the file is bound to directory as log as file is not stored on tape,
                 *   while AL and RP are bound to file
                 */
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

                info = new OSMStorageInfo( inodeStorageInfo.storageGroup(),
                                           inodeStorageInfo.storageSubGroup() );

                info.setIsNew(false);
                info.setAccessLatency(inodeStorageInfo.accessLatency());
                info.setRetentionPolicy(inodeStorageInfo.retentionPolicy());

                for(StorageLocatable location: locations) {
                    if( location.isOnline() ) {
                        try {
                            info.addLocation( new URI(location.location()) );
                        } catch (URISyntaxException e) {
                            // bad URI
                        }
                    }
                }
            }

        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }

        try {
            stat = inode.stat();
        }catch( ChimeraFsException hfe) {
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
            String[] accessLatency = getTag(dirInode, "AccessLatency");
            String[] retentionPolicy = getTag(dirInode, "RetentionPolicy");
            if(accessLatency != null) {
                try {
                    info.setAccessLatency(AccessLatency.getAccessLatency(accessLatency[0].trim()));
                    info.isSetAccessLatency(true);
                }
                catch(IllegalArgumentException iae) {
                    // TODO: do we fail here or not?
                }
            }
            else {
	         // force default
                info.setAccessLatency(getDefaultAccessLatency());
            }

            if(retentionPolicy != null) {
                try {
                    info.setRetentionPolicy( diskCacheV111.util.RetentionPolicy.getRetentionPolicy(retentionPolicy[0].trim()));
                    info.isSetRetentionPolicy(true);
                }
                catch(IllegalArgumentException iae) {
                }
            }else{
                info.setRetentionPolicy(getDefaultRetentionPolicy());
            }
            return info;
        }
        catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

}