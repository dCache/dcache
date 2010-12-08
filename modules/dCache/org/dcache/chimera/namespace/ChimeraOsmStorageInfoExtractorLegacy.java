/*
 * $Id: ChimeraOsmStorageInfoExtractorLegacy.java,v 1.1 2007-07-11 12:48:42 tigran Exp $
 */
package org.dcache.chimera.namespace;

import diskCacheV111.util.AccessLatency;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInode_TAG;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.ChimeraFsException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.HsmLocation;
import diskCacheV111.util.HsmLocationExtractorFactory;
import diskCacheV111.util.OsmLocationExtractor;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import java.io.CharArrayReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.dcache.chimera.store.InodeStorageInformation;


/**
 *
 * Extractor store and restore storageInfo into pnfs levels
 *
 */
public class ChimeraOsmStorageInfoExtractorLegacy implements
        ChimeraStorageInfoExtractable {

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.StorageInfoExtractable#getStorageInfo(java.lang.String,
     *      diskCacheV111.util.PnfsId)
     */

    public StorageInfo getStorageInfo(FsInode inode)
            throws CacheException {

        if (inode.isDirectory()) {
            return getDirStorageInfo(inode);
        }

        return getFileStorageInfo(inode);

    }

    private static StorageInfo getFileStorageInfo(FsInode inode) throws CacheException {

        OSMStorageInfo info = null;
        Stat levelStat = null;
        Stat stat = null;
        FsInode level1 = new FsInode(inode.getFs(), inode.toString(), 1);
        FsInode level2 = new FsInode(inode.getFs(), inode.toString(), 2);

        if (!level1.exists()) {
            info = (OSMStorageInfo) getDirStorageInfo(inode);
        } else {

            try {
                levelStat = level1.stat();

                byte[] buff = new byte[(int) levelStat.getSize()];
                int len = level1.read(0, buff, 0, buff.length);

                Map<Integer, String> levels = new HashMap<Integer, String>(1);

                levels.put(Integer.valueOf(1), new String(buff));
                URI location = new OsmLocationExtractor(levels).location();

                InodeStorageInformation storageInfo = inode.getFs().getSorageInfo(inode);

                info = new OSMStorageInfo(storageInfo.storageGroup(), storageInfo.storageSubGroup());
                info.addLocation(location);

            } catch (ChimeraFsException e) {
                throw new CacheException(e.getMessage());
            }
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

    private static StorageInfo getDirStorageInfo(FsInode inode) throws CacheException {

        FsInode dirInode = null;
        if (!inode.isDirectory()) {
            dirInode = inode.getParent();
        } else {
            dirInode = inode;
        }


        FsInode_TAG tagInode = new FsInode_TAG(dirInode.getFs(), dirInode.toString(), "OSMTemplate");

        byte[] buff = new byte[256];

        int len = tagInode.read(0, buff, 0, buff.length);

        StorageInfo si = null;

        CharArrayReader ca = new CharArrayReader(new String(buff, 0, len)
                .toCharArray());

        BufferedReader br = new BufferedReader(ca);
        HashMap<String, String> hash = new HashMap<String, String>();
        StringTokenizer st = null;

        try {
            String line = null;
            while ((line = br.readLine()) != null) {
                st = new StringTokenizer(line);
                if (st.countTokens() < 2)
                    continue;
                hash.put(st.nextToken(), st.nextToken());
            }

            String store =  hash.get("StoreName");
            if (store == null) {
                throw new CacheException(37, "StoreName not found in template");
            }

            tagInode = new FsInode_TAG(dirInode.getFs(), dirInode.toString(), "sGroup");
            len = tagInode.read(0, buff, 0, buff.length);

            ca = new CharArrayReader(new String(buff, 0, len).toCharArray());
            br = new BufferedReader(ca);

            String gr = br.readLine();

            OSMStorageInfo info = new OSMStorageInfo(store, gr);
            info.addKeys(hash);

            // overwrite htm type with hsmInstance tag

            tagInode = new FsInode_TAG(dirInode.getFs(), dirInode.toString(), "hsmInstance");
            len = tagInode.read(0, buff, 0, buff.length);

            ca = new CharArrayReader(new String(buff, 0, len).toCharArray());
            br = new BufferedReader(ca);

            if( len > 0 ) {
            	String hsm = br.readLine();
            	if( hsm != null ) {
            		info.setHsm( hsm.toLowerCase());
            	}
            }


            si = info;
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
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


        RetentionPolicy retentionPolicy = dCacheStorageInfo.getRetentionPolicy();
        AccessLatency accessLatency = dCacheStorageInfo.getAccessLatency();

    	InodeStorageInformation storageInfo = new InodeStorageInformation(inode,
    			dCacheStorageInfo.getHsm(),
    			dCacheStorageInfo.getKey("store"),
    			dCacheStorageInfo.getKey("group"),
    			accessLatency,
    			retentionPolicy);

    	try {
    		inode.getFs().setStorageInfo(inode, storageInfo);

    		if(dCacheStorageInfo.isSetAddLocation() ) {
	    		List<URI> locationURIs = dCacheStorageInfo.locations();
	    		for(URI location : locationURIs) {
	    			HsmLocation hsmLocation = HsmLocationExtractorFactory.extractorOf(location);

	    			Map<Integer, String> levels = hsmLocation.toLevels();
	    			for( Map.Entry<Integer, String> levelEntry: levels.entrySet() ) {
	    				FsInode levelInode = new FsInode(inode.getFs(), inode.toString(), levelEntry.getKey().intValue() );
	    				byte[] levelData = levelEntry.getValue().getBytes();
	    				levelInode.write(0,levelData , 0, levelData.length);
	    			}
	    		}
    		}

    	}catch(ChimeraFsException he ) {
    		throw new CacheException(he.getMessage() );
    	}
    }

}
