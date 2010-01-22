/*
 * $Id: ChimeraNameSpaceProvider.java,v 1.7 2007-10-01 12:28:03 tigran Exp $
 */
package org.dcache.chimera.namespace;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.regex.Pattern;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;

import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.NotDirChimeraException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.XMLconfig;
import org.dcache.chimera.HimeraDirectoryEntry;
import org.dcache.chimera.posix.Stat;

import diskCacheV111.namespace.NameSpaceProvider;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.FileNotFoundCacheException;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;
import java.io.IOException;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.util.Interval;
import org.dcache.vehicles.FileAttributes;
import org.dcache.acl.ACLException;
import org.dcache.acl.handler.singleton.AclHandler;
import org.dcache.chimera.DirectoryStreamB;

public class ChimeraNameSpaceProvider
    implements NameSpaceProvider
{
    private final JdbcFs       _fs;
    private final Args         _args;
    private final ChimeraStorageInfoExtractable _extractor;

    private static final Logger _logNameSpace =  Logger.getLogger("logger.org.dcache.namespace");

    private final diskCacheV111.util.AccessLatency _defaultAccessLatency;
    private final diskCacheV111.util.RetentionPolicy _defaultRetentionPolicy;

    public ChimeraNameSpaceProvider( Args args, CellNucleus nucleus) throws Exception {

	    XMLconfig config = new XMLconfig( new File( args.getOpt("chimeraConfig") ) );
	    _fs = new JdbcFs(  config );
	    _args = args;

        String accessLatensyOption = args.getOpt("DefaultAccessLatency");
            if( accessLatensyOption != null && accessLatensyOption.length() > 0) {
                /*
                 * IllegalArgumentException thrown if option is invalid
                 */
                _defaultAccessLatency = diskCacheV111.util.AccessLatency.getAccessLatency(accessLatensyOption);
            }else{
                _defaultAccessLatency = StorageInfo.DEFAULT_ACCESS_LATENCY;
            }

            String retentionPolicyOption = args.getOpt("DefaultRetentionPolicy");
            if( retentionPolicyOption != null && retentionPolicyOption.length() > 0) {
                /*
                 * IllegalArgumentException thrown if option is invalid
                 */
                _defaultRetentionPolicy = diskCacheV111.util.RetentionPolicy.getRetentionPolicy(retentionPolicyOption);
            }else{
                _defaultRetentionPolicy = StorageInfo.DEFAULT_RETENTION_POLICY;
            }

        Class<ChimeraStorageInfoExtractable> exClass = (Class<ChimeraStorageInfoExtractable>) Class.forName( _args.argv(0)) ;
        Constructor<ChimeraStorageInfoExtractable>  extractorInit =
            exClass.getConstructor(AccessLatency.class, RetentionPolicy.class);
        _extractor =  extractorInit.newInstance(_defaultAccessLatency, _defaultRetentionPolicy);

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

    public void setFileMetaData(Subject subject, PnfsId pnfsId, FileMetaData metaData) {

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

    public FileMetaData getFileMetaData(Subject subject, PnfsId pnfsId) throws CacheException {

        FsInode inode = new FsInode(_fs, pnfsId.toIdString() );
        Stat stat = null;
        try {
            stat = inode.stat();
        }catch(FileNotFoundHimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + inode);
        }catch(ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }

        FileMetaData fileMetaData = new FileMetaData(inode.isDirectory(), stat.getUid(), stat.getGid(), stat.getMode() );
        fileMetaData.setFileType(!inode.isDirectory(), inode.isDirectory(), inode.isLink());
        fileMetaData.setSize( stat.getSize() );
        fileMetaData.setTimes(stat.getATime(), stat.getMTime(), stat.getCTime());

        return fileMetaData;
    }

    public PnfsId createEntry(Subject subject, String path,  FileMetaData metaData, boolean isDir ) throws CacheException {


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
        } catch (NotDirChimeraException e) {
            throw new NotDirCacheException("Not a directory: " + path);
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        } catch (FileExistsChimeraFsException e) {
            throw new FileExistsCacheException("File exists: " + path);
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }

        return new PnfsId(inode.toString());
    }

    public void deleteEntry(Subject subject, PnfsId pnfsId) throws CacheException {
        try {
            FsInode inode = new FsInode(_fs, pnfsId.toIdString() );
            _fs.remove(inode);
        }catch(FileNotFoundHimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        }catch(ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public void deleteEntry(Subject subject, String path) throws CacheException {
        try {
            _fs.remove(path);
        }catch(FileNotFoundHimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        }catch(ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public void renameEntry(Subject subject, PnfsId pnfsId,
                            String newName, boolean overwrite)
        throws CacheException
    {
        try {
            FsInode inode = new FsInode(_fs, pnfsId.toIdString());
            FsInode parentDir = _fs.getParentOf( inode );
            String path = _fs.inode2path(inode);

            File pathFile = new File(path);
            String name = pathFile.getName();


            File dest = new File(newName);
            FsInode destDir = null;

            try {
                if( dest.getParent().equals( pathFile.getParent()) ) {
                    destDir = parentDir;
                }else{
                    destDir = _fs.path2inode( dest.getParent() );
                }
            } catch (FileNotFoundHimeraFsException e) {
                throw new NotDirCacheException("No such directory: " +
                                               dest.getParent());
            }

            if (!overwrite) {
                try {
                    _fs.path2inode(newName);
                    throw new FileExistsCacheException("File exists:" + newName);
                } catch (FileNotFoundHimeraFsException e) {
                    /* This is what we want; unfortunately there is no way
                     * to test this with Chimera without throwing an
                     * exception.
                     */
                }
            }

            _fs.move(parentDir, name, destDir, dest.getName());
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: "
                                                 + pnfsId);
        } catch (FileExistsChimeraFsException e) {
            /* With the current implementation of Chimera, I don't
             * expect this to be thrown. Instead Chimera insists on
             * overwriting the destination file.
             */
            throw new FileExistsCacheException("File exists:" + newName);
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public void addCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation) throws CacheException {

    	if(_logNameSpace.isDebugEnabled() ) {
    		_logNameSpace.debug ("add cache location "+ cacheLocation +" for "+pnfsId);
    	}

        try {
        	FsInode inode = new FsInode(_fs, pnfsId.toIdString());
        	_fs.addInodeLocation(inode, StorageGenericLocation.DISK, cacheLocation);
        }catch(FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file: " + pnfsId);
        } catch (ChimeraFsException e){
            _logNameSpace.error("Exception in addCacheLocation "+e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    public List<String> getCacheLocation(Subject subject, PnfsId pnfsId) throws CacheException {

        try {
            List<String> locations = new ArrayList<String>();

            FsInode inode = new FsInode(_fs, pnfsId.toIdString());
            List<StorageLocatable> localyManagerLocations = _fs.getInodeLocations(inode, StorageGenericLocation.DISK );

            for (StorageLocatable location: localyManagerLocations) {
                locations.add( location.location() );
            }

            return locations;
        } catch (ChimeraFsException e){
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    public void clearCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws CacheException {

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

    public String pnfsidToPath(Subject subject, PnfsId pnfsId) throws CacheException {
        try {
            FsInode inode = new FsInode(_fs, pnfsId.toIdString() );

            if( ! inode.exists() ) {
        	throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
            }
            return _fs.inode2path(inode);
        } catch (ChimeraFsException e){
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    public PnfsId pathToPnfsid(Subject subject, String path, boolean followLink) throws CacheException {

    	FsInode inode = null;
        try {
            inode = _fs.path2inode(path);
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory " + path);
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }

        return new PnfsId( inode.toString() );
    }

    public StorageInfo getStorageInfo(Subject subject, PnfsId pnfsId) throws CacheException {

    	if(_logNameSpace.isDebugEnabled() ) {
    		_logNameSpace.debug ("getStorageInfo for " + pnfsId);
    	}

        FsInode inode = new FsInode(_fs, pnfsId.toString());
        return _extractor.getStorageInfo(inode);
    }

    public void setStorageInfo(Subject subject, PnfsId pnfsId, StorageInfo storageInfo, int accessMode) throws CacheException {

    	if(_logNameSpace.isDebugEnabled() ) {
    		_logNameSpace.debug ("setStorageInfo for " + pnfsId);
    	}

        FsInode inode = new FsInode(_fs, pnfsId.toString());
        _extractor.setStorageInfo(inode, storageInfo, accessMode);
    }

    public String[] getFileAttributeList(Subject subject, PnfsId pnfsId) {
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

    public Object getFileAttribute(Subject subject, PnfsId pnfsId, String attribute) {
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

    public void removeFileAttribute(Subject subject, PnfsId pnfsId, String attribute) {
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

    public void setFileAttribute(Subject subject, PnfsId pnfsId, String attribute, Object data) {

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

    public void addChecksum(Subject subject, PnfsId pnfsId, int type, String value) throws CacheException
    {
        try {
            FsInode inode = new FsInode(_fs, pnfsId.toString());
            String existingValue = _fs.getInodeChecksum(inode, type);
            if (existingValue != null) {
                if (!existingValue.equals(value)) {
                    throw new CacheException(CacheException.INVALID_ARGS,
                                             "Checksum mismatch");
                }
                return;
            }
            _fs.setInodeChecksum(inode, type, value);
        }catch(FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        }catch(ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public String getChecksum(Subject subject, PnfsId pnfsId, int type) throws CacheException
    {
        try {
            return _fs.getInodeChecksum(new FsInode(_fs, pnfsId.toString()), type );
        }catch(FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        }catch(ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public void removeChecksum(Subject subject, PnfsId pnfsId, int type) throws CacheException
    {
        try {
            _fs.removeInodeChecksum(new FsInode(_fs, pnfsId.toString()), type);
        }catch(FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        }catch(ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public Set<Checksum> getChecksums(Subject subject, PnfsId pnfsId) throws CacheException {
        Set<Checksum> checksums = new HashSet<Checksum>();
        for(ChecksumType type:ChecksumType.values()) {
            int int_type = type.getType();
            String value = null;
            try {
                value = getChecksum(subject, pnfsId, int_type);
            }
            catch(Exception e) {}
            if(value != null) {
                checksums.add(new Checksum(type,value));
            }

        }
        return checksums;
    }

    public int[] listChecksumTypes(Subject subject, PnfsId pnfsId ) throws CacheException
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

    public PnfsId getParentOf(Subject subject, PnfsId pnfsId) throws CacheException {
        FsInode inodeOfResource = new FsInode(_fs, pnfsId.toIdString());
        FsInode inodeParent;

        try {
            inodeParent = _fs.getParentOf(inodeOfResource);
        }catch(ChimeraFsException e) {
            _logNameSpace.error("getParentOf failed : " + e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }

        if (inodeParent == null) {
        	throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        }

        return new PnfsId( inodeParent.toString() );
    }

    private FileAttributes getFileAttributes(Subject subject, FsInode inode,
                                             Set<FileAttribute> attr)
        throws IOException, ChimeraFsException, ACLException
    {
        FileAttributes attributes = new FileAttributes();
        Stat stat;

        for (FileAttribute attribute: attr) {
            switch (attribute) {
            case ACL:
                if (AclHandler.getAclConfig().isAclEnabled()) {
                    attributes.setAcl(AclHandler.getACL(inode.toString()));
                } else {
                    attributes.setAcl(null);
                }
                break;
            case ACCESS_LATENCY:
                attributes.setAccessLatency(diskCacheV111.util.AccessLatency.getAccessLatency(_fs.getAccessLatency(inode).getId()));
                break;
            case RETENTION_POLICY:
                attributes.setRetentionPolicy(diskCacheV111.util.RetentionPolicy.getRetentionPolicy(_fs.getRetentionPolicy(inode).getId()));
                break;
            case SIZE:
                stat = inode.statCache();
                attributes.setSize(stat.getSize());
                break;
            case MODIFICATION_TIME:
                stat = inode.statCache();
                attributes.setModificationTime(stat.getMTime());
                break;
            case OWNER:
                stat = inode.statCache();
                attributes.setOwner(stat.getUid());
                break;
            case OWNER_GROUP:
                stat = inode.statCache();
                attributes.setGroup(stat.getGid());
                break;
            case CHECKSUM:
                Set<Checksum> checksums = new HashSet<Checksum>();
                for (ChecksumType type: ChecksumType.values()) {
                    String value = _fs.getInodeChecksum(inode, type.getType());
                    if (value != null) {
                        checksums.add(new Checksum(type,value));
                    }
                }
                attributes.setChecksums(checksums);
                break;
            case LOCATIONS:
                List<String> locations = new ArrayList<String>();
                List<StorageLocatable> localyManagerLocations =
                    _fs.getInodeLocations(inode, StorageGenericLocation.DISK);
                for (StorageLocatable location: localyManagerLocations) {
                    locations.add(location.location());
                }
                attributes.setLocations(locations);
                break;
            case FLAGS:
                ChimeraCacheInfo info = new ChimeraCacheInfo(inode);
                Map<String,String> flags = new HashMap<String,String>();
                for (Map.Entry<String,String> e: info.getFlags().entrySet()) {
                    flags.put(e.getKey(), e.getValue());
                }
                attributes.setFlags(flags);
                break;
            case TYPE:
                stat = inode.statCache();
                UnixPermission perm = new UnixPermission(stat.getMode());
                if (perm.isReg()) {
                    attributes.setFileType(FileType.REGULAR);
                } else if (perm.isDir()) {
                    attributes.setFileType(FileType.DIR);
                } else if (perm.isSymLink()) {
                    attributes.setFileType(FileType.LINK);
                } else {
                    attributes.setFileType(FileType.SPECIAL);
                }
                break;
            case MODE:
                stat = inode.statCache();
                attributes.setMode(stat.getMode());
                break;
            case PNFSID:
                attributes.setPnfsId(new PnfsId(inode.toString()));
                break;
            default:
                throw new UnsupportedOperationException("Attribute " + attribute + " not supported yet.");
            }
        }
        return attributes;
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
                                            Set<FileAttribute> attr)
        throws CacheException
    {
        try {
            return getFileAttributes(subject,
                                     new FsInode(_fs, pnfsId.toIdString()),
                                     attr);
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        } catch (ACLException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    @Override
    public void setFileAttributes(Subject subject, PnfsId pnfsId, FileAttributes attr) throws CacheException {
        _logNameSpace.debug("File attributes update: " + attr.getDefinedAttributes());

        StorageInfo dir = null;
        FsInode inode = new FsInode(_fs, pnfsId.toIdString());
        try {

            for (FileAttribute attribute : attr.getDefinedAttributes()) {

                switch (attribute) {

                    case LOCATIONS:
                        for (String location: attr.getLocations()) {
                            _fs.addInodeLocation(inode, StorageGenericLocation.DISK, location);
                        }
                        break;
                    case SIZE:
                        _fs.setFileSize(inode, attr.getSize());
                        break;
                    case CHECKSUM:
                        for(Checksum sum: attr.getChecksums() ) {
                            _fs.setInodeChecksum(inode, sum.getType().getType() , sum.getValue());
                        }
                        break;
                    case ACCESS_LATENCY:
                        _fs.setAccessLatency(inode,  org.dcache.chimera.store.AccessLatency.valueOf(attr.getAccessLatency().getId()));
                        break;
                    case RETENTION_POLICY:
                        _fs.setRetentionPolicy(inode, org.dcache.chimera.store.RetentionPolicy.valueOf(attr.getRetentionPolicy().getId()));
                        break;
                    case DEFAULT_ACCESS_LATENCY:
                        /*
                         * update the value only if some one did not
                         * updated it yet
                         *
                         * FIXME: this is a quick hack and have to go
                         * into Chimera code
                         */
                        if (_fs.getAccessLatency(inode) == null) {
                            if (dir == null) {
                                dir = _extractor.getStorageInfo(inode);
                            }
                            _fs.setAccessLatency(inode, org.dcache.chimera.store.AccessLatency.valueOf(dir.getAccessLatency().getId()));
                        }
                        break;
                    case DEFAULT_RETENTION_POLICY:
                        /*
                         * update the value only if some one did not
                         * updated it yet
                         *
                         * FIXME: this is a quick hack and have to go
                         * into Chimera code
                         */
                        if(_fs.getRetentionPolicy(inode) == null) {
                            if (dir == null) {
                                dir = _extractor.getStorageInfo(inode);
                            }
                            _fs.setRetentionPolicy(inode, org.dcache.chimera.store.RetentionPolicy.valueOf(dir.getRetentionPolicy().getId()));
                        }
                        break;
                    case FLAGS:
                        ChimeraCacheInfo cacheInfo = new ChimeraCacheInfo(inode);
                        for (Map.Entry<String, String> flag : attr.getFlags().entrySet()) {
                            cacheInfo.getFlags().put(flag.getKey(), flag.getValue());
                        }
                        cacheInfo.writeCacheInfo(inode);
                        break;
                    default:
                        throw new UnsupportedOperationException("Attribute " + attribute + " not supported yet.");
                }

            }
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        } catch (ChimeraFsException e) {
            _logNameSpace.error("Exception in setFileAttributes: " + e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        } catch (IOException e) {
            _logNameSpace.error("Exception in setFileAttributes: " + e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }

    }

    public void list(Subject subject, String path, Glob glob, Interval range,
                     Set<FileAttribute> attrs, ListHandler handler)
        throws CacheException
    {
        try {
            Pattern pattern = (glob == null) ? null : glob.toPattern();
            FsInode dir = _fs.path2inode(path);
            if (!dir.isDirectory()) {
                throw new NotDirCacheException("Not a directory: " + path);
            }

            long counter = 0;
            DirectoryStreamB<HimeraDirectoryEntry> dirStream = dir.newDirectoryStream();
            try{
                for (HimeraDirectoryEntry entry: dirStream) {
                    try {
                        String name = entry.getName();
                        if (!name.equals(".") && !name.equals("..") &&
                            (pattern == null || pattern.matcher(name).matches()) &&
                            (range == null || range.contains(counter++))) {
                            // FIXME: actually, HimeraDirectoryEntry
                            // already contains most of attributes
                            FileAttributes fa =
                                attrs.isEmpty()
                                ? null
                                : getFileAttributes(subject, entry.getInode(), attrs);
                            handler.addEntry(name, fa);
                        }
                    } catch (FileNotFoundHimeraFsException e) {
                        /* Not an error; files may be deleted during the
                         * list operation.
                         */
                    }
                }
            }finally{
                dirStream.close();
            }
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        } catch (ChimeraFsException e) {
            _logNameSpace.error("Exception in list: " + e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        } catch (ACLException e) {
            _logNameSpace.error("Exception in list: " + e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            _logNameSpace.error("Exception in list: " + e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }
}
