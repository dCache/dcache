/*
 * $Id: ChimeraNameSpaceProvider.java,v 1.7 2007-10-01 12:28:03 tigran Exp $
 */
package org.dcache.chimera.namespace;

import com.mchange.v2.c3p0.DataSources;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.regex.Pattern;
import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.NotDirChimeraException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.UnixPermission;
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
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.vehicles.StorageInfo;
import dmg.util.Args;
import java.io.IOException;
import javax.sql.DataSource;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.util.Interval;
import org.dcache.vehicles.FileAttributes;
import org.dcache.acl.ACLException;
import org.dcache.acl.handler.singleton.AclHandler;
import org.dcache.chimera.DirectoryStreamB;
import org.dcache.auth.Subjects;
import static org.dcache.acl.enums.AccessType.*;

import org.springframework.beans.factory.annotation.Required;

public class ChimeraNameSpaceProvider
    implements NameSpaceProvider
{
    private final JdbcFs       _fs;
    private final Args         _args;
    private final ChimeraStorageInfoExtractable _extractor;

    private static final Logger _log =  LoggerFactory.getLogger(ChimeraNameSpaceProvider.class);

    private final AccessLatency _defaultAccessLatency;
    private final RetentionPolicy _defaultRetentionPolicy;

    private final boolean _inheritFileOwnership;
    private PermissionHandler _permissionHandler;

    public ChimeraNameSpaceProvider(Args args) throws Exception {

        // FIXME: the initialization have to go into spring xml file
        Class.forName(args.getOpt("chimera.db.driver"));

        DataSource dataSource = DataSources.unpooledDataSource(args.getOpt("chimera.db.url"),
                args.getOpt("chimera.db.user"), args.getOpt("chimera.db.password"));

        _fs = new JdbcFs(DataSources.pooledDataSource(dataSource), args.getOpt("chimera.db.dialect"));
        _args = args;

        String accessLatensyOption = args.getOpt("DefaultAccessLatency");
            if( accessLatensyOption != null && accessLatensyOption.length() > 0) {
                /*
                 * IllegalArgumentException thrown if option is invalid
                 */
                _defaultAccessLatency = AccessLatency.getAccessLatency(accessLatensyOption);
            }else{
                _defaultAccessLatency = StorageInfo.DEFAULT_ACCESS_LATENCY;
            }

            String retentionPolicyOption = args.getOpt("DefaultRetentionPolicy");
            if( retentionPolicyOption != null && retentionPolicyOption.length() > 0) {
                /*
                 * IllegalArgumentException thrown if option is invalid
                 */
                _defaultRetentionPolicy = RetentionPolicy.getRetentionPolicy(retentionPolicyOption);
            }else{
                _defaultRetentionPolicy = StorageInfo.DEFAULT_RETENTION_POLICY;
            }

            _inheritFileOwnership =
                Boolean.valueOf(args.getOpt("inheritFileOwnership"));

        Class<ChimeraStorageInfoExtractable> exClass = (Class<ChimeraStorageInfoExtractable>) Class.forName( _args.argv(0)) ;
        Constructor<ChimeraStorageInfoExtractable>  extractorInit =
            exClass.getConstructor(AccessLatency.class, RetentionPolicy.class);
        _extractor =  extractorInit.newInstance(_defaultAccessLatency, _defaultRetentionPolicy);
    }

    @Required
    public void setPermissionHandler(PermissionHandler handler)
    {
        _permissionHandler = handler;
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

		setModeOf(stat, mode);
		stat.setUid(metaData.getUid());
		stat.setGid(metaData.getGid());
		stat.setSize(metaData.getFileSize());

		return stat;
	}

    private FsInode pathToInode(Subject subject, String path)
        throws IOException, ChimeraFsException, ACLException, CacheException
    {
        if (!Subjects.isRoot(subject)) {
            File file = new File(path);
            String parentPath = file.getParent();

            if (parentPath != null) {
                FsInode parentInode = _fs.path2inode(parentPath);
                FileAttributes attributes =
                    getFileAttributesForPermissionHandler(parentInode);
                if (_permissionHandler.canLookup(subject, attributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + path);
                }
            }
        }

        return _fs.path2inode(path);
    }

    public PnfsId createEntry(Subject subject, String path,
                              int uid, int gid, int mode, boolean isDir)
        throws CacheException
    {
        FsInode inode;

        try {
            File newEntryFile = new File(path);
            String parentPath = newEntryFile.getParent();
            if (parentPath == null) {
                throw new FileExistsCacheException("File exists: " + path);
            }
            FsInode parent = pathToInode(subject, parentPath);

            if (!Subjects.isRoot(subject)) {
                FileAttributes attributes =
                    getFileAttributesForPermissionHandler(parent);
                if (isDir) {
                    if (_permissionHandler.canCreateSubDir(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + path);
                    }
                } else {
                    if (_permissionHandler.canCreateFile(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + path);
                    }
                }
            }

            if (uid == DEFAULT) {
                if (Subjects.isNobody(subject) || _inheritFileOwnership) {
                    uid = parent.statCache().getUid();
                } else {
                    uid = (int) Subjects.getUid(subject);
                }
            }

            if (gid == DEFAULT) {
                if (Subjects.isNobody(subject) || _inheritFileOwnership) {
                    gid = parent.statCache().getGid();
                } else {
                    gid = (int) Subjects.getPrimaryGid(subject);
                }
            }

            if (mode == DEFAULT) {
                mode = parent.statCache().getMode();
                if (isDir) {
                    mode &= UMASK_DIR;
                } else {
                    mode &= UMASK_FILE;
                }
            }

            if( isDir ) {
                inode = _fs.mkdir(parent, newEntryFile.getName(), uid, gid, mode);
            }else{
                inode = _fs.createFile(parent, newEntryFile.getName(), uid, gid, mode);
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
        } catch (ACLException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }

        return new PnfsId(inode.toString());
    }

    public void deleteEntry(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        boolean removed;

        try {
            FsInode inode = new FsInode(_fs, pnfsId.toIdString() );

            if (!Subjects.isRoot(subject)) {
                FsInode inodeParent = _fs.getParentOf(inode);
                FileAttributes parentAttributes =
                    getFileAttributesForPermissionHandler(inodeParent);
                FileAttributes fileAttributes =
                    getFileAttributesForPermissionHandler(inode);

                if (inode.isDirectory()) {
                    if (_permissionHandler.canDeleteDir(subject,
                                                        parentAttributes,
                                                        fileAttributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
                    }
                } else {
                    if (_permissionHandler.canDeleteFile(subject,
                                                         parentAttributes,
                                                         fileAttributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
                    }
                }
            }

            removed = _fs.remove(inode);
        }catch(FileNotFoundHimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        }catch(ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (ACLException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }

        if (!removed) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     "Entry could not be removed: " + pnfsId);
        }
    }

    public void deleteEntry(Subject subject, String path)
        throws CacheException
    {
        boolean removed;

        try {
            if (!Subjects.isRoot(subject)) {
                File file = new File(path);
                String parentPath = file.getParent();

                if (parentPath != null) {
                    FsInode inode = pathToInode(subject, path);
                    FsInode inodeParent = _fs.path2inode(parentPath);

                    FileAttributes parentAttributes =
                        getFileAttributesForPermissionHandler(inodeParent);
                    FileAttributes fileAttributes =
                        getFileAttributesForPermissionHandler(inode);

                    if (inode.isDirectory()) {
                        if (_permissionHandler.canDeleteDir(subject,
                                                            parentAttributes,
                                                            fileAttributes) != ACCESS_ALLOWED) {
                            throw new PermissionDeniedCacheException("Access denied: " + path);
                        }
                    } else {
                        if (_permissionHandler.canDeleteFile(subject,
                                                             parentAttributes,
                                                             fileAttributes) != ACCESS_ALLOWED) {
                            throw new PermissionDeniedCacheException("Access denied: " + path);
                        }
                    }
                }
            }

            removed = _fs.remove(path);
        }catch(FileNotFoundHimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        }catch(ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (ACLException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }

        if (!removed) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     "Entry could not be removed: " + path);
        }
    }

    public void renameEntry(Subject subject, PnfsId pnfsId,
                            String newName, boolean overwrite)
        throws CacheException
    {
        try {
            FsInode inode = new FsInode(_fs, pnfsId.toIdString());
            FsInode parentDir = _fs.getParentOf( inode );
            FileAttributes parentDirAttributes =
                getFileAttributesForPermissionHandler(parentDir);

            String path = _fs.inode2path(inode);
            File pathFile = new File(path);
            String name = pathFile.getName();

            File dest = new File(newName);
            FsInode destDir;
            FileAttributes destDirAttributes;

            try {
                if( dest.getParent().equals( pathFile.getParent()) ) {
                    destDir = parentDir;
                    destDirAttributes = parentDirAttributes;
                }else{
                    destDir = pathToInode(subject, dest.getParent());
                    destDirAttributes =
                        getFileAttributesForPermissionHandler(destDir);
                }
            } catch (FileNotFoundHimeraFsException e) {
                throw new NotDirCacheException("No such directory: " +
                                               dest.getParent());
            }

            if (!Subjects.isRoot(subject) &&
                _permissionHandler.canRename(subject,
                                             parentDirAttributes,
                                             destDirAttributes,
                                             inode.isDirectory()) != ACCESS_ALLOWED) {
                throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
            }

            if (Subjects.isRoot(subject) && overwrite) {
                _fs.move(parentDir, name, destDir, dest.getName());
                return;
            }

            try {
                FsInode destInode = _fs.path2inode(newName);
                if (!overwrite) {
                    throw new FileExistsCacheException("File exists:" + newName);
                }

                /* Destination name exists and we were requested to
                 * overwrite it.  Thus the subject must have delete
                 * permission for the destination name.
                 */
                FileAttributes destAttributes =
                    getFileAttributesForPermissionHandler(destInode);
                if (destInode.isDirectory()) {
                    if (_permissionHandler.canDeleteDir(subject,
                                                        destDirAttributes,
                                                        destAttributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " +
                                                                 newName);
                    }
                } else {
                    if (_permissionHandler.canDeleteFile(subject,
                                                         destDirAttributes,
                                                         destAttributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " +
                                                                 newName);
                    }
                }
            } catch (FileNotFoundHimeraFsException e) {
                /* Destination doesn't exist and we can move the file;
                 * unfortunately there is no way to test this with
                 * Chimera without throwing an exception.
                 */
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
        } catch (ACLException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public void addCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation) throws CacheException {

        _log.debug ("add cache location {} for {}", cacheLocation, pnfsId);

        try {
        	FsInode inode = new FsInode(_fs, pnfsId.toIdString());
        	_fs.addInodeLocation(inode, StorageGenericLocation.DISK, cacheLocation);
        }catch(FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file: " + pnfsId);
        } catch (ChimeraFsException e){
            _log.error("Exception in addCacheLocation {}", e);
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

        _log.debug("clearCacheLocation : {} for {}", cacheLocation, pnfsId) ;

        try {
        	FsInode inode = new FsInode(_fs, pnfsId.toIdString());

        	_fs.clearInodeLocation(inode, StorageGenericLocation.DISK, cacheLocation);

        	if( removeIfLast ) {
        	List<StorageLocatable> locations = _fs.getInodeLocations(inode, StorageGenericLocation.DISK);
        	    if( locations.isEmpty() ) {

                        _log.debug("last location cleaned. removing file {}", inode) ;
        	        _fs.remove(inode);
        	    }
        	}

        } catch (ChimeraFsException e){
            _log.error("Exception in clearCacheLocation for {} : {}", pnfsId, e);
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

    public PnfsId pathToPnfsid(Subject subject, String path, boolean followLink)
        throws CacheException
    {
    	FsInode inode;
        try {
            inode = pathToInode(subject, path);
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory " + path);
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        } catch (ACLException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }

        return new PnfsId( inode.toString() );
    }

    public void setStorageInfo(Subject subject, PnfsId pnfsId, StorageInfo storageInfo, int accessMode) throws CacheException {

        _log.debug ("setStorageInfo for {}", pnfsId);

        FsInode inode = new FsInode(_fs, pnfsId.toString());
        _extractor.setStorageInfo(inode, storageInfo, accessMode);
    }

    public void removeFileAttribute(Subject subject, PnfsId pnfsId, String attribute)
        throws CacheException
    {
        try {
            FsInode inode = new FsInode(_fs, pnfsId.toString(), 2);
            ChimeraCacheInfo info = new ChimeraCacheInfo(inode);
            ChimeraCacheInfo.CacheFlags flags = info.getFlags();
            flags.remove(attribute);
            info.writeCacheInfo(inode);
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory " + pnfsId);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    public void removeChecksum(Subject subject, PnfsId pnfsId, ChecksumType type)
        throws CacheException
    {
        try {
            _fs.removeInodeChecksum(new FsInode(_fs, pnfsId.toString()),
                                    type.getType());
        }catch(FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        }catch(ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
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
            _log.error("getParentOf failed : {}", e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }

        if (inodeParent == null) {
        	throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        }

        return new PnfsId( inodeParent.toString() );
    }

    private FileAttributes getFileAttributesForPermissionHandler(FsInode inode)
        throws IOException, ChimeraFsException, ACLException, CacheException
    {
        return getFileAttributes(Subjects.ROOT, inode,
                                 _permissionHandler.getRequiredAttributes());
    }

    private FileAttributes getFileAttributes(Subject subject, FsInode inode,
                                             Set<FileAttribute> attr)
        throws IOException, ChimeraFsException, ACLException, CacheException
    {
        if (!inode.exists()) {
            throw new FileNotFoundHimeraFsException();
        }

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
            case ACCESS_TIME:
                stat = inode.statCache();
                attributes.setAccessTime(stat.getATime());
                break;
            case RETENTION_POLICY:
                attributes.setRetentionPolicy(diskCacheV111.util.RetentionPolicy.getRetentionPolicy(_fs.getRetentionPolicy(inode).getId()));
                break;
            case SIZE:
                stat = inode.statCache();
                attributes.setSize(stat.getSize());
                break;
            case CREATION_TIME:
                stat = inode.statCache();
                attributes.setCreationTime(stat.getCTime());
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
                FsInode level2 = new FsInode(_fs, inode.toString(), 2);
                ChimeraCacheInfo info = new ChimeraCacheInfo(level2);
                Map<String,String> flags = new HashMap<String,String>();
                for (Map.Entry<String,String> e: info.getFlags().entrySet()) {
                    flags.put(e.getKey(), e.getValue());
                }
                attributes.setFlags(flags);
                break;
            case SIMPLE_TYPE:
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
            case STORAGEINFO:
                attributes.setStorageInfo(_extractor.getStorageInfo(inode));
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
            FsInode inode = new FsInode(_fs, pnfsId.toIdString());

            if (Subjects.isRoot(subject)) {
                return getFileAttributes(subject, inode, attr);
            }

            /* If we have to authorize the check then we fetch
             * permission handler attributes in addition to the
             * attributes requested by the caller.
             */
            Set<FileAttribute> required = EnumSet.noneOf(FileAttribute.class);
            required.addAll(_permissionHandler.getRequiredAttributes());
            required.addAll(attr);
            FileAttributes fileAttributes =
                getFileAttributes(subject, inode, required);

            /* The permission check is performed after we fetched the
             * attributes to avoid fetching the attributes twice.
             */
            try {
                FsInode inodeParent = _fs.getParentOf(inode);

                FileAttributes parent =
                    getFileAttributesForPermissionHandler(inodeParent);
                if (_permissionHandler.canGetAttributes(subject, parent, fileAttributes, attr) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
                }
            } catch (FileNotFoundCacheException e) {
                /* This usually means that the file doesn't have a
                 * parent. That is we are fetching attributes of the
                 * root directory. We cannot handle this situation
                 * correctly with the current PermissionHandler. As a
                 * temporary workaround we simply allow fetching
                 * attributes of the root directory.
                 */
            }
            return fileAttributes;
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
    public void setFileAttributes(Subject subject, PnfsId pnfsId,
                                  FileAttributes attr)
        throws CacheException
    {
        _log.debug("File attributes update: {}", attr.getDefinedAttributes());

        FsInode inode = new FsInode(_fs, pnfsId.toIdString());

        try {
            if (!Subjects.isRoot(subject)) {
                FsInode inodeParent = _fs.getParentOf(inode);
                FileAttributes parentAttributes =
                    getFileAttributesForPermissionHandler(inodeParent);
                FileAttributes attributes =
                    getFileAttributesForPermissionHandler(inode);

                if (_permissionHandler.canSetAttributes(subject, parentAttributes, attributes,
                                                        attr.getDefinedAttributes()) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
                }
            }

            StorageInfo dir = null;
            Stat stat = null;

            for (FileAttribute attribute : attr.getDefinedAttributes()) {

                switch (attribute) {

                    case LOCATIONS:
                        for (String location: attr.getLocations()) {
                            _fs.addInodeLocation(inode, StorageGenericLocation.DISK, location);
                        }
                        break;
                    case SIZE:
                        if (stat == null) stat = inode.statCache();
                        stat.setSize(attr.getSize());
                        break;
                    case MODE:
                        if (stat == null) stat = inode.statCache();
                        // FIXME this is temporary work-around: we must
                        // preserve file type (high-order bits) that
                        // the end-user doesn't provide.  This should
                        // be fixed in Chimera.
                        setModeOf(stat, attr.getMode());
                        break;
                    case OWNER:
                        if (stat == null) stat = inode.statCache();
                        stat.setUid(attr.getOwner());
                        break;
                    case OWNER_GROUP:
                        if (stat == null) stat = inode.statCache();
                        stat.setGid(attr.getGroup());
                        break;
                    case CHECKSUM:
                        for (Checksum sum: attr.getChecksums()) {
                            int type = sum.getType().getType();
                            String value = sum.getValue();
                            String existingValue =
                                _fs.getInodeChecksum(inode, type);
                            if (existingValue == null) {
                                _fs.setInodeChecksum(inode, type, value);
                            } else if (!existingValue.equals(value)) {
                                throw new CacheException(CacheException.INVALID_ARGS,
                                                         "Checksum mismatch");
                            }
                        }
                        break;
                    case ACCESS_LATENCY:
                        _fs.setAccessLatency(inode, attr.getAccessLatency());
                        break;
                    case RETENTION_POLICY:
                        _fs.setRetentionPolicy(inode, attr.getRetentionPolicy());
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
                            _fs.setAccessLatency(inode, dir.getAccessLatency());
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
                            _fs.setRetentionPolicy(inode, dir.getRetentionPolicy());
                        }
                        break;
                    case FLAGS:
                        FsInode level2 = new FsInode(_fs, pnfsId.toString(), 2);
                        ChimeraCacheInfo cacheInfo = new ChimeraCacheInfo(level2);
                        for (Map.Entry<String,String> flag: attr.getFlags().entrySet()) {
                            cacheInfo.getFlags().put(flag.getKey(), flag.getValue());
                        }
                        cacheInfo.writeCacheInfo(level2);
                        break;
                    default:
                        throw new UnsupportedOperationException("Attribute " + attribute + " not supported yet.");
                }
            }

            if (stat != null) {
                inode.setStat(stat);
            }

        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        } catch (ChimeraFsException e) {
            _log.error("Exception in setFileAttributes: {}", e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        } catch (ACLException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            _log.error("Exception in setFileAttributes: {}", e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }

    }

    /**
     * Update provided Stat object with supplied Unix permission.
     * FIXME This method is a work-around for a bug in Chimera's
     * Stat.setMode where the client (us) must preserve the high-
     * order bits (that encode the inode type).
     * See RT ticket 5575
     *         http://rt.dcache.org/Ticket/Display.html?id=5575
     */
    private static void setModeOf(Stat stat, int mode)
    {
        int newMode = (mode & UnixPermission.S_PERMS) |
                (stat.getMode() & ~UnixPermission.S_PERMS);
        stat.setMode(newMode);
    }

    public void list(Subject subject, String path, Glob glob, Interval range,
                     Set<FileAttribute> attrs, ListHandler handler)
        throws CacheException
    {
        try {
            Pattern pattern = (glob == null) ? null : glob.toPattern();
            FsInode dir = pathToInode(subject, path);
            if (!dir.isDirectory()) {
                throw new NotDirCacheException("Not a directory: " + path);
            }

            if (!Subjects.isRoot(subject)) {
                FileAttributes attributes =
                    getFileAttributesForPermissionHandler(dir);
                if (!dir.isDirectory()) {
                    throw new NotDirCacheException("Not a directory");
                } else if (_permissionHandler.canListDir(subject, attributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " +
                                                             path);
                }
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
            _log.error("Exception in list: {}", e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        } catch (ACLException e) {
            _log.error("Exception in list: {}", e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            _log.error("Exception in list: {}", e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }
}
