/*
 * $Id: ChimeraOsmStorageInfoExtractor.java,v 1.8 2007-08-24 08:22:59 tigran Exp $
 */
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
import org.dcache.chimera.store.AccessLatency;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.chimera.store.RetentionPolicy;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;


public class ChimeraOsmStorageInfoExtractor implements
        ChimeraStorageInfoExtractable {

    /**
     * default access latency for newly created files
     */
    private final diskCacheV111.util.AccessLatency _defaultAccessLatency;

    /**
     * default retention policy for newly created files
     */
    private final diskCacheV111.util.RetentionPolicy _defaultRetentionPolicy;


    public ChimeraOsmStorageInfoExtractor(diskCacheV111.util.AccessLatency defaultAL,
            diskCacheV111.util.RetentionPolicy defaultRP) {

        _defaultAccessLatency = defaultAL;
        _defaultRetentionPolicy = defaultRP;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.StorageInfoExtractable#getStorageInfo(java.lang.String,
     *      diskCacheV111.util.PnfsId)
     */

    public StorageInfo getStorageInfo(FsInode inode)
            throws CacheException {

        if( !inode.exists() ) {
            throw new FileNotFoundCacheException(inode.toString() + " does not exists");
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

    private StorageInfo getFileStorageInfo(FsInode inode) throws CacheException {

        OSMStorageInfo info = null;

        Stat stat = null;
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
                    info.setAccessLatency(diskCacheV111.util.AccessLatency.getAccessLatency(al.getId()));
                }

                RetentionPolicy rp = inode.getFs().getRetentionPolicy(inode);
                if( rp != null ) {
                    info.setRetentionPolicy(diskCacheV111.util.RetentionPolicy.getRetentionPolicy(rp.getId()));
                }
            } else {


                InodeStorageInformation inodeStorageInfo = inode.getFs().getSorageInfo(inode);

                info = new OSMStorageInfo( inodeStorageInfo.storageGroup(),
                        inodeStorageInfo.storageSubGroup() );

                info.setIsNew(false);
                info.setAccessLatency(diskCacheV111.util.AccessLatency.getAccessLatency( inodeStorageInfo.accessLatency().getId() ) );
                info.setRetentionPolicy(diskCacheV111.util.RetentionPolicy.getRetentionPolicy( inodeStorageInfo.retentionPolicy().getId() ) );

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

    private StorageInfo getDirStorageInfo(FsInode inode) throws CacheException {

        FsInode dirInode = null;

        if (!inode.isDirectory()) {
            dirInode = inode.getParent();
        } else {
            dirInode = inode;
        }

        StorageInfo si = null;

        try {

            String[] OSMTemplate = getTag(dirInode, "OSMTemplate");
            if (OSMTemplate == null) {
                throw new CacheException(37, "OSMTemplate tag not found");
            }
            HashMap<String, String> hash = new HashMap<String, String>();

            for ( String line: OSMTemplate) {
                StringTokenizer st = new StringTokenizer(line);
                if (st.countTokens() < 2)
                    continue;
                hash.put(st.nextToken(), st.nextToken());
            }

            String store = hash.get("StoreName");
            if (store == null) {
                throw new CacheException(37, "StoreName not found in template");
            }

            String [] sGroup = getTag(dirInode, "sGroup");
            if( sGroup == null ) {
                throw new CacheException(37, "sGroup tag not found");
            }

            String gr = sGroup[0].trim();

            OSMStorageInfo info = new OSMStorageInfo(store, gr);
            info.addKeys(hash);

            si = info;


            String[] accessLatency = getTag(dirInode, "AccessLatency");
            String[] retentionPolicy = getTag(dirInode, "RetentionPolicy");

            /*
             * if Access latency and/or retention policy is defined for a directory
             * apply it to the file and make it persistent, while it's a file attribute and directory
             * tag is default value only
             */
            if(accessLatency != null) {
                try {
                    info.setAccessLatency( diskCacheV111.util.AccessLatency.getAccessLatency(accessLatency[0].trim()));
                    info.isSetAccessLatency(true);
                }catch(IllegalArgumentException iae) {
                    // TODO: do we fail here or not?
                }
            }else{
                // force default
                info.setAccessLatency(_defaultAccessLatency);
            }

            if(retentionPolicy != null) {
                try {
                    info.setRetentionPolicy( diskCacheV111.util.RetentionPolicy.getRetentionPolicy(retentionPolicy[0].trim()));
                    info.isSetRetentionPolicy(true);
                }catch(IllegalArgumentException iae) {
                    // TODO: do we fail here or not?
                }
            }else{
                // force default
                info.setRetentionPolicy(_defaultRetentionPolicy);
            }

        } catch (IOException e) {
            throw new CacheException(e.getMessage());
        }

        return si;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.StorageInfoExtractable#setStorageInfo(java.lang.String,
     *      diskCacheV111.util.PnfsId, diskCacheV111.vehicles.StorageInfo, int)
     */
    public void setStorageInfo(FsInode inode, StorageInfo dCacheStorageInfo,
            int arg3) throws CacheException {

        try {

            if( dCacheStorageInfo.isSetAccessLatency() ) {
                AccessLatency accessLatency = AccessLatency.valueOf(dCacheStorageInfo.getAccessLatency().getId());
                inode.getFs().setAccessLatency(inode, accessLatency);
            }

            if( dCacheStorageInfo.isSetRetentionPolicy() ) {
                RetentionPolicy retentionPolicy = RetentionPolicy.valueOf( dCacheStorageInfo.getRetentionPolicy().getId());
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
                    if(location.toString().isEmpty()) continue;
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
    private static String[] getTag(FsInode dirInode, String tag)
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

        List<String> lines = new ArrayList<String>();
        CharArrayReader ca = new CharArrayReader(new String(buff, 0, len)
                .toCharArray());

        BufferedReader br = new BufferedReader(ca);

        String line = null;
        while ((line = br.readLine()) != null) {
            lines.add(line);
        }

        return lines.toArray(new String[lines.size()]);

    }
}
