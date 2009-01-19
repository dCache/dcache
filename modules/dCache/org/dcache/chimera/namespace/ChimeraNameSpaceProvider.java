/*
 * $Id: ChimeraNameSpaceProvider.java,v 1.7 2007-10-01 12:28:03 tigran Exp $
 */
package org.dcache.chimera.namespace;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.XMLconfig;
import org.dcache.chimera.posix.Stat;

import diskCacheV111.namespace.CacheLocationProvider;
import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.namespace.StorageInfoProvider;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.FileNotFoundCacheException;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;


public class ChimeraNameSpaceProvider implements NameSpaceProvider, StorageInfoProvider, CacheLocationProvider {

    private final JdbcFs       _fs;
    private final Args         _args;
    private final ChimeraStorageInfoExtractable _extractor;

    private static final Logger _logNameSpace =  Logger.getLogger("logger.org.dcache.namespace");

    public ChimeraNameSpaceProvider( Args args, CellNucleus nucleus) throws Exception {

	    XMLconfig config = new XMLconfig( new File( args.getOpt("chimeraConfig") ) );
	    _fs = new JdbcFs(  config );
	    _args = args;

	    Class<ChimeraStorageInfoExtractable> exClass = (Class<ChimeraStorageInfoExtractable>) Class.forName( _args.argv(0)) ;
        _extractor = exClass.newInstance() ;

    }

    private static Stat fileMetadata2Stat(FileMetaData metaData, boolean isDir) {

		Stat stat = new Stat();

		int mode = 0;

		// user
		if (metaData.getUserPermissions().canRead()) {
			mode |= 0400;
		}
		if (metaData.getUserPermissions().canWrite()) {
			mode |= 0200;
		}
		if (metaData.getUserPermissions().canExecute()) {
			mode |= 0100;
		}

		// group
		if (metaData.getGroupPermissions().canRead()) {
			mode |= 0040;
		}
		if (metaData.getGroupPermissions().canWrite()) {
			mode |= 0020;
		}
		if (metaData.getGroupPermissions().canExecute()) {
			mode |= 0010;
		}

		// world
		if (metaData.getWorldPermissions().canRead()) {
			mode |= 0004;
		}
		if (metaData.getWorldPermissions().canWrite()) {
			mode |= 0002;
		}
		if (metaData.getWorldPermissions().canExecute()) {
			mode |= 0001;
		}

		if (isDir) {
			mode |= UnixPermission.S_IFDIR;
		} else {
			mode |= UnixPermission.S_IFREG;
		}

		stat.setMode(mode);
		stat.setUid(metaData.getUid());
		stat.setGid(metaData.getGid());
		stat.setSize(metaData.getFileSize());

		return stat;
	}

    public void setFileMetaData(PnfsId pnfsId, FileMetaData metaData) {

    	if(_logNameSpace.isDebugEnabled() ) {
    		_logNameSpace.debug ("setFileMetaData:" + pnfsId + " " + metaData);
    	}
        FsInode inode = new FsInode(_fs, pnfsId.toIdString() );

        try {

        	Stat metadataStat = fileMetadata2Stat(metaData, inode.isDirectory() );

            Stat inodeStat = inode.statCache();
            inodeStat.setMode(metadataStat.getMode());
            inodeStat.setUid(metadataStat.getUid());
            inodeStat.setGid(metadataStat.getGid());
            inodeStat.setSize(metadataStat.getSize() );

            inode.setStat(inodeStat);

        }catch(ChimeraFsException hfe) {
        	_logNameSpace.error("setFileMetadata failed: " + hfe.getMessage());
        }

        return ;
    }

    public FileMetaData getFileMetaData(PnfsId pnfsId) throws Exception {

        FsInode inode = new FsInode(_fs, pnfsId.toIdString() );
        Stat stat = null;
        try {
            stat = inode.stat();
        }catch(FileNotFoundHimeraFsException fnf) {
            throw new FileNotFoundCacheException(inode.toString());
        }

        FileMetaData fileMetaData = new FileMetaData(inode.isDirectory(), stat.getUid(), stat.getGid(), stat.getMode() );
        fileMetaData.setFileType(!inode.isDirectory(), inode.isDirectory(), inode.isLink());
        fileMetaData.setSize( stat.getSize() );
        fileMetaData.setTimes(stat.getATime(), stat.getMTime(), stat.getCTime());

        return fileMetaData;
    }

    public PnfsId createEntry(String path,  FileMetaData metaData, boolean isDir ) throws Exception {


        FsInode inode = null;

        try {
        Stat metadataStat = fileMetadata2Stat(metaData, isDir );

        File newEntryFile = new File(path);

        FsInode parent = _fs.path2inode(newEntryFile.getParent());

        if( isDir ) {
            inode = _fs.mkdir(parent, newEntryFile.getName(), metadataStat.getUid(), metadataStat.getGid(), metadataStat.getMode() );
        }else{
            inode = _fs.createFile(parent, newEntryFile.getName(), metadataStat.getUid(), metadataStat.getGid(), metadataStat.getMode() );
        }

        }catch( FileExistsChimeraFsException fee) {
            throw new FileExistsCacheException(path);
        }

        return new PnfsId(inode.toString());
    }

    public void deleteEntry(PnfsId pnfsId) throws Exception {
        FsInode inode = new FsInode(_fs, pnfsId.toIdString() );
        _fs.remove(inode);
    }

    public void deleteEntry(String path) throws Exception {
        _fs.remove(path);
    }
    public void renameEntry(PnfsId pnfsId, String newName) throws Exception {

        FsInode inode = new FsInode(_fs, pnfsId.toIdString());
        FsInode parentDir = _fs.getParentOf( inode );
        String path = _fs.inode2path(inode);

        File pathFile = new File(path);
        String name = pathFile.getName();


        File dest = new File(newName);
        FsInode destDir = null;

        if( dest.getParent().equals( pathFile.getParent()) ) {
            destDir = parentDir;
        }else{
            destDir = _fs.path2inode( dest.getParent() );
        }

        _fs.move(parentDir, name, destDir, dest.getName());
    }

    public void addCacheLocation(PnfsId pnfsId, String cacheLocation) throws Exception {

    	if(_logNameSpace.isDebugEnabled() ) {
    		_logNameSpace.debug ("add cache location "+ cacheLocation +" for "+pnfsId);
    	}

        try {
        	FsInode inode = new FsInode(_fs, pnfsId.toIdString());
        	_fs.addInodeLocation(inode, StorageGenericLocation.DISK, cacheLocation);
        }catch(FileNotFoundHimeraFsException e) {
               throw new FileNotFoundCacheException(e.getMessage()); 
        } catch (ChimeraFsException e){
            _logNameSpace.error("Exception in addCacheLocation "+e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    public List<String> getCacheLocation(PnfsId pnfsId) throws Exception {

        List<String> locations = new ArrayList<String>();

        FsInode inode = new FsInode(_fs, pnfsId.toIdString());
        List<StorageLocatable> localyManagerLocations = _fs.getInodeLocations(inode, StorageGenericLocation.DISK );

        for (StorageLocatable location: localyManagerLocations) {
             locations.add( location.location() );
        }

        return locations;
    }

    public void clearCacheLocation(PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws Exception {

    	if(_logNameSpace.isDebugEnabled() ) {
    		_logNameSpace.debug("clearCacheLocation : "+cacheLocation+" for "+pnfsId) ;
    	}

        try {
        	FsInode inode = new FsInode(_fs, pnfsId.toIdString());

        	_fs.clearInodeLocation(inode, StorageGenericLocation.DISK, cacheLocation);

        	if( removeIfLast ) {
        	List<StorageLocatable> locations = _fs.getInodeLocations(inode, StorageGenericLocation.DISK);
        	    if( locations.isEmpty() ) {

        	        if(_logNameSpace.isDebugEnabled() ) {
        	            _logNameSpace.debug("last location cleaned. removeing file " + inode) ;
        	        }
        	        _fs.remove(inode);
        	    }
        	}

        } catch (ChimeraFsException e){
            _logNameSpace.error("Exception in clearCacheLocation for : "+pnfsId+" -> "+e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    public String pnfsidToPath(PnfsId pnfsId) throws Exception {
        FsInode inode = new FsInode(_fs, pnfsId.toIdString() );

        if( ! inode.exists() ) {
        	throw new FileNotFoundCacheException(pnfsId.toIdString() + " : no such file or directory");
        }
        return _fs.inode2path(inode);
    }

    public PnfsId pathToPnfsid(String path, boolean followLink) throws Exception {

    	FsInode inode = null;
        try {
			inode = _fs.path2inode(path);
		} catch (FileNotFoundHimeraFsException e) {
			throw new FileNotFoundCacheException("no such file or directory " + path);
		}

        return new PnfsId( inode.toString() );
    }

    public StorageInfo getStorageInfo(PnfsId pnfsId) throws Exception {

    	if(_logNameSpace.isDebugEnabled() ) {
    		_logNameSpace.debug ("getStorageInfo for " + pnfsId);
    	}

        FsInode inode = new FsInode(_fs, pnfsId.toString());
        return _extractor.getStorageInfo(inode);
    }

    public void setStorageInfo(PnfsId pnfsId, StorageInfo storageInfo, int accessMode) throws Exception {

    	if(_logNameSpace.isDebugEnabled() ) {
    		_logNameSpace.debug ("setStorageInfo for " + pnfsId);
    	}

        FsInode inode = new FsInode(_fs, pnfsId.toString());
        _extractor.setStorageInfo(inode, storageInfo, accessMode);
    }

    public String[] getFileAttributeList(PnfsId pnfsId) {
        String[] keys = null;

        try {
        	FsInode inode = new FsInode(_fs, pnfsId.toString(), 2);
            ChimeraCacheInfo info   = new ChimeraCacheInfo( inode ) ;
            ChimeraCacheInfo.CacheFlags flags = info.getFlags() ;
            Set<Map.Entry<String, String>> s = flags.entrySet();

            keys = new String[s.size()];
            Iterator<Map.Entry<String, String>> it = s.iterator();

            for( int i = 0; i < keys.length ; i++) {
            	Map.Entry<String, String> entry = it.next() ;
                keys[i] = entry.getKey() ;
            }
        }catch(Exception e){
            _logNameSpace.error(e.getMessage());
        }

        return keys;
    }

    public Object getFileAttribute(PnfsId pnfsId, String attribute) {
        Object attr = null;
        try {
        	FsInode inode = new FsInode(_fs, pnfsId.toString(), 2);
            ChimeraCacheInfo info   = new ChimeraCacheInfo( inode ) ;
            ChimeraCacheInfo.CacheFlags flags = info.getFlags() ;

            attr =  flags.get(attribute);

        }catch( Exception e){
            _logNameSpace.error(e.getMessage());
        }

        return attr;
    }

    public void removeFileAttribute(PnfsId pnfsId, String attribute) {
        try {
        	FsInode inode = new FsInode(_fs, pnfsId.toString(), 2);
            ChimeraCacheInfo info   = new ChimeraCacheInfo( inode ) ;
            ChimeraCacheInfo.CacheFlags flags = info.getFlags() ;

            flags.remove( attribute ) ;
            info.writeCacheInfo( inode ) ;
        }catch( Exception e){
            _logNameSpace.error(e.getMessage());
        }
    }

    public void setFileAttribute(PnfsId pnfsId, String attribute, Object data) {

        try {
        	FsInode inode = new FsInode(_fs, pnfsId.toString(), 2);
            ChimeraCacheInfo info   = new ChimeraCacheInfo( inode ) ;
            ChimeraCacheInfo.CacheFlags flags = info.getFlags() ;

            flags.put( attribute ,  data.toString()) ;

            info.writeCacheInfo( inode ) ;
       }catch( Exception e){
           _logNameSpace.error(e.getMessage());
       }
    }

    public void addChecksum(PnfsId pnfsId, int type, String value) throws Exception
    {
        _fs.setInodeChecksum(new FsInode(_fs, pnfsId.toString()), type, value);
    }

    public String getChecksum(PnfsId pnfsId, int type) throws Exception
    {
    	return _fs.getInodeChecksum(new FsInode(_fs, pnfsId.toString()), type );
    }

    public void removeChecksum(PnfsId pnfsId, int type) throws Exception
    {
    	_fs.removeInodeChecksum(new FsInode(_fs, pnfsId.toString()), type);
    }

    public Set<Checksum> getChecksums(PnfsId pnfsId) throws Exception {
        Set<Checksum> checksums = new HashSet<Checksum>();
        for(ChecksumType type:ChecksumType.values()) {
            int int_type = type.getType();
            String value = null;
            try {
                value = getChecksum(pnfsId, int_type);
            }
            catch(Exception e) {}
            if(value != null) {
                checksums.add(new Checksum(type,value));
            }
            
        }
        return checksums;
    }
    
    public int[] listChecksumTypes(PnfsId pnfsId ) throws Exception
    {
        return null;
    }

    @Override
    public String toString() {
       StringBuilder sb = new StringBuilder();

       sb.append("$Id: ChimeraNameSpaceProvider.java,v 1.7 2007-10-01 12:28:03 tigran Exp $ \n");
       sb.append(_fs.getInfo() );
        return sb.toString();

    }

    public PnfsId getParentOf(PnfsId id)
    {
        // TODO!
        return null;
    }
}
