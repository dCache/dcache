package org.dcache.chimera.namespace;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInode_TAG;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.store.InodeStorageInformation;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.vehicles.StorageInfo;

public abstract class ChimeraHsmStorageInfoExtractor implements
       ChimeraStorageInfoExtractable {

    /**
     * default access latency for newly created files
     */
    private final AccessLatency _defaultAccessLatency;

    /**
     * default retention policy for newly created files
     */
    private final RetentionPolicy _defaultRetentionPolicy;


    public ChimeraHsmStorageInfoExtractor(AccessLatency defaultAL,
                                          RetentionPolicy defaultRP) {

        _defaultAccessLatency = defaultAL;
        _defaultRetentionPolicy = defaultRP;
    }

    public final AccessLatency getDefaultAccessLatency() {
        return _defaultAccessLatency;
    }

    public final RetentionPolicy getDefaultRetentionPolicy() {
        return _defaultRetentionPolicy;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.StorageInfoExtractable#getFileAttributes(java.lang.String,
     *      diskCacheV111.util.PnfsId)
     */

    @Override
    public StorageInfo getStorageInfo(FsInode inode)
            throws CacheException {

        try {
            if( !inode.exists() ) {
                throw new FileNotFoundCacheException(inode.toString() + " does not exists");
            }
        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }

        StorageInfo info;
        FsInode dirInode;

        if (inode.isDirectory()) {
            info =  getDirStorageInfo(inode);
            dirInode = inode;
        } else {
            info =  getFileStorageInfo(inode);
            dirInode = inode.getParent();
        }

        try {
            // overwrite hsm type with hsmInstance tag
            String[] hsmInstance;
            hsmInstance = getTag(dirInode, "hsmInstance");
            if( hsmInstance != null ) {
                info.setHsm( hsmInstance[0].toLowerCase().trim());
            }

            String[] cacheClass = getTag(dirInode, "cacheClass");
            if( cacheClass != null ) {
                info.setCacheClass( cacheClass[0].trim());
            }

            String [] spaceToken = getTag(dirInode, "WriteToken");
            if( spaceToken != null ) {
                info.setKey("writeToken", spaceToken[0].trim());
            }
        } catch (IOException e) {
            throw new CacheException( 37, "Unable to fetch tags: " + e.getMessage());
        }

        return info;
    }

    public abstract StorageInfo getFileStorageInfo(FsInode inode) throws CacheException;
    public abstract StorageInfo getDirStorageInfo(FsInode inode) throws CacheException;

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.StorageInfoExtractable#setStorageInfo(java.lang.String,
     *      diskCacheV111.util.PnfsId, diskCacheV111.vehicles.StorageInfo, int)
     */
    @Override
    public void setStorageInfo(FsInode inode, StorageInfo dCacheStorageInfo,
            int arg3) throws CacheException {

        try {

            if( dCacheStorageInfo.isSetAccessLatency() ) {
                AccessLatency accessLatency = dCacheStorageInfo.getAccessLatency();
                inode.getFs().setAccessLatency(inode, accessLatency);
            }

            if( dCacheStorageInfo.isSetRetentionPolicy() ) {
                RetentionPolicy retentionPolicy = dCacheStorageInfo.getRetentionPolicy();
                inode.getFs().setRetentionPolicy(inode, retentionPolicy);
            }

            if(dCacheStorageInfo.isSetAddLocation() ) {
                List<URI> locationURIs = dCacheStorageInfo.locations();

                if( !locationURIs.isEmpty() ) {
                    InodeStorageInformation storageInfo = new InodeStorageInformation(inode,
                    dCacheStorageInfo.getHsm(),
                    dCacheStorageInfo.getKey("store"),
                    dCacheStorageInfo.getKey("group"));
                    inode.getFs().setStorageInfo(inode, storageInfo);
                }

                for(URI location : locationURIs) {
                    // skip bad URI's if the get here
                    if(location.toString().isEmpty()) {
                        continue;
                    }
                    URI validatedUri = HsmLocationExtractorFactory.validate(location);
                    inode.getFs().addInodeLocation(inode, StorageGenericLocation.TAPE, validatedUri.toString());
                }
            }

        }catch(FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException(e.getMessage());
        }catch(ChimeraFsException he ) {
            throw new CacheException(he.getMessage() );
        }
    }

    /**
     *
     * get content of a virtual file named .(tag)(&lt;tagname&gt;).
     *
     * @param dirInode inode of directory
     * @param tag tag name
     * @return array of strings corresponding to lines of tag file or null
     * if tag does not exist or empty.
     * @throws IOException
     */
    public static String[] getTag(FsInode dirInode, String tag)
        throws IOException {

        FsInode_TAG tagInode = new FsInode_TAG(dirInode.getFs(), dirInode
                .toString(), tag);


        if( !tagInode.exists() ) {
            return null;
        }

        byte[] buff = new byte[256];

        int len = tagInode.read(0, buff, 0, buff.length);
        /* empty and bad tags are treated as non existing tags */
        if( len <= 0 ) {
            return null;
        }

        List<String> lines = new ArrayList<>();
        CharArrayReader ca = new CharArrayReader(new String(buff, 0, len)
                .toCharArray());

        BufferedReader br = new BufferedReader(ca);

        String line;
        while ((line = br.readLine()) != null) {
            lines.add(line);
        }

        return lines.toArray(new String[lines.size()]);

    }
}
