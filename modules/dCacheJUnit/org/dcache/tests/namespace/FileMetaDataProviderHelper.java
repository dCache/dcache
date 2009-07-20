package org.dcache.tests.namespace;

import java.util.HashMap;
import java.util.Map;

import diskCacheV111.services.FileMetaDataSource;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileMetaDataX;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellEndpoint;

/**
*
* Fake metadata source
*
*/
public class FileMetaDataProviderHelper implements FileMetaDataSource {


        private static final Map<PnfsId, FileMetaData> _metadataById = new HashMap<PnfsId, FileMetaData>();
        private static final Map<String, FileMetaData> _metadataByPath = new HashMap<String, FileMetaData>();

        private static final Map<PnfsId, FileMetaDataX> _metadataXById = new HashMap<PnfsId, FileMetaDataX>();
        private static final Map<String, FileMetaDataX> _metadataXByPath = new HashMap<String, FileMetaDataX>();

        public FileMetaDataProviderHelper(CellEndpoint cell) {
        	// forced by use in dcache .batch files
        }
        
        public FileMetaData getMetaData(String path) throws CacheException {

            FileMetaData metaData = _metadataByPath.get(path);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(path + " not found");
            }
            return metaData;
        }

        public FileMetaData getMetaData(PnfsId pnfsId) throws CacheException {

            FileMetaData metaData = _metadataById.get(pnfsId);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(pnfsId + " not found");
            }
            return metaData;
        }


        public FileMetaDataX getXMetaData(String path) throws CacheException {

            FileMetaDataX metaData = _metadataXByPath.get(path);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(path + " not found");
            }
            return metaData;
        }

        public FileMetaDataX getXMetaData(PnfsId pnfsId) throws CacheException {

            FileMetaDataX metaData = _metadataXById.get(pnfsId);

            if( metaData == null ) {
                throw new FileNotFoundCacheException(pnfsId + " not found");
            }
            return metaData;
        }

        public void setMetaData(PnfsId pnfsId,FileMetaData metaData ) {
            _metadataById.put(pnfsId, metaData);
        }


        public void setMetaData(String path,FileMetaData metaData ) {
            _metadataByPath.put(path, metaData);
        }

        public void setXMetaData(PnfsId pnfsId,FileMetaDataX metaData ) {
            _metadataXById.put(pnfsId, metaData);
        }


        public void setXMetaData(String path,FileMetaDataX metaData ) {
            _metadataXByPath.put(path, metaData);
        }

        public void cleanAll() {
            _metadataById.clear();
            _metadataByPath.clear();
            _metadataXById.clear();
            _metadataXByPath.clear();

        }

    }
