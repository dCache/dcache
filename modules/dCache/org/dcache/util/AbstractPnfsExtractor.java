package org.dcache.util;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.StorageInfoExtractable;
import diskCacheV111.vehicles.CacheInfo;
import diskCacheV111.vehicles.StorageInfo;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An extractor of storage information from pnfs.
 * @since 1.9.2-6
 */
public abstract class AbstractPnfsExtractor implements StorageInfoExtractable {

    /**
     * default access latency for newly created files
     */
    protected final diskCacheV111.util.AccessLatency _defaultAccessLatency;

    /**
     * default retention policy for newly created files
     */
    protected final diskCacheV111.util.RetentionPolicy _defaultRetentionPolicy;

    protected AbstractPnfsExtractor(AccessLatency defaultAL, RetentionPolicy defaultRP) {
        _defaultAccessLatency = defaultAL;
        _defaultRetentionPolicy = defaultRP;
    }

    @Override
    public StorageInfo getStorageInfo(String pnfsMountpoint, PnfsId pnfsId) throws CacheException {

        try {
            PnfsFile x = PnfsFile.getFileByPnfsId(pnfsMountpoint, pnfsId);
            if (x == null) {
                throw new CacheException(37, "Not a valid PnfsId " + pnfsId);
            } else if (x.isDirectory()) {
                return extractDirectory(pnfsMountpoint, x);
            } else if (x.isFile()) {

                StorageInfo storageInfo = extractFile(pnfsMountpoint, x);
                /*
                 * update file size only in if extracor did not set it yet
                 */
                if( storageInfo.getFileSize() == 0) {
                    storageInfo.setFileSize(x.length());
                }

                /*
                 * skip yet another NFS lookup:
                 *
                 * if not stored yet, check that bfid is not the default value.
                 */
                if( ! storageInfo.isStored() )
                    storageInfo.setIsStored(!"<Unknown>".equals(storageInfo.getBitfileId()));

                /*
                 * stored file can't be new - so skip the test.
                 */
                if( ! storageInfo.isStored() ) {
                    // reuse file size to avoid extra NFS lookup
                    storageInfo.setIsNew( (storageInfo.getFileSize() == 0)  && (x.getLevelFile(2).length() == 0));
                }

                return storageInfo;
            } else if (x.isLink()) {
                return getStorageInfo(pnfsMountpoint, new PnfsFile(x.getCanonicalPath()).getPnfsId());
            } else {
                throw new CacheException(34, "Invalid file type " + pnfsId);
            }
        } catch (IOException e) {
            throw new CacheException(33, "Unexpected IO exeption: " + e);
        }
    }

    @Override
    public abstract void setStorageInfo(String pnfsMountpoint, PnfsId pnfsId,
            StorageInfo storageInfo, int accessMode) throws CacheException;

    /**
     * Utility method to extract {@link StorageInfo} of a directory.
     *
     * @param pnfsMountpoint
     * @param pnfsFile
     * @return directories storage info
     */
    protected abstract StorageInfo extractDirectory(String pnfsMountpoint,
            PnfsFile pnfsFile) throws CacheException;

    /**
     * Utility method to extract {@link StorageInfo} of a file.
     *
     * @param pnfsMountpoint
     * @param pnfsFile
     * @return file's storage info
     */
    protected abstract StorageInfo extractFile(String pnfsMountpoint, PnfsFile pnfsFile)
            throws CacheException;

    /**
     * HACK -  AccessLatency and RetentionPolicy stored as a flags
     * FIXME: this information shouldn't be stored here.
     * @param storageInfo
     * @param pnfsFile
     * @throws CacheException
     */
    protected void storeAlRpInLevel2(StorageInfo storageInfo, PnfsFile pnfsFile)
            throws CacheException {
        if (storageInfo.isSetAccessLatency() || storageInfo.isSetRetentionPolicy()) {
            try {

                CacheInfo info = new CacheInfo(pnfsFile);
                CacheInfo.CacheFlags flags = info.getFlags();

                if (storageInfo.isSetAccessLatency()) {
                    flags.put("al", storageInfo.getAccessLatency().toString());
                }

                if (storageInfo.isSetRetentionPolicy()) {
                    flags.put("rp", storageInfo.getRetentionPolicy().toString());
                }

                info.writeCacheInfo(pnfsFile);

            } catch (IOException ee) {
                throw new CacheException(107, "Problem in set(OSM)StorageInfo : " + ee);
            }
        }
    }

    /**
     * Read given file and returns a {@link List} of strings. Each element in the
     * list string per file line.
     * @param level
     * @return array
     * @throws CacheException
     */
    protected List<String> readLines(File file) throws CacheException {

        BufferedReader br = null;
        List<String> lines = new ArrayList<String>();
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            while((line = br.readLine()) != null) {
                line = line.trim();
                lines.add(line);
            }

            return lines;

        } catch (FileNotFoundException e) {
            throw new CacheException(37, "File not found when opening level " + file);
        } catch (IOException e) {
            throw new CacheException(37, "Failed to read from level-1 " + file);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    // too late to react
                }
            }
        }
    }
}
