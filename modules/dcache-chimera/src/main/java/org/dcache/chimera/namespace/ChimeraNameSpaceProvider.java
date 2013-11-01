/*
 * $Id: ChimeraNameSpaceProvider.java,v 1.7 2007-10-01 12:28:03 tigran Exp $
 */
package org.dcache.chimera.namespace;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.enums.RsType;
import org.dcache.auth.Subjects;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.DirNotEmptyHimeraFsException;
import org.dcache.chimera.DirectoryStreamB;
import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.HimeraDirectoryEntry;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.NotDirChimeraException;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.posix.Stat;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.namespace.PermissionHandler;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.acl.enums.AccessType.ACCESS_ALLOWED;

public class ChimeraNameSpaceProvider
    implements NameSpaceProvider
{
    private JdbcFs       _fs;
    private ChimeraStorageInfoExtractable _extractor;

    private static final Logger _log =  LoggerFactory.getLogger(ChimeraNameSpaceProvider.class);

    private boolean _inheritFileOwnership;
    private boolean _verifyAllLookups;
    private boolean _aclEnabled;
    private PermissionHandler _permissionHandler;

    /**
     * A value of difference in seconds which controls file's access time updates.
     */
    private long _atimeGap;

    @Required
    public void setExtractor(ChimeraStorageInfoExtractable extractor)
    {
        _extractor = extractor;
    }

    @Required
    public void setInheritFileOwnership(boolean inherit)
    {
        _inheritFileOwnership = inherit;
    }

    @Required
    public void setVerifyAllLookups(boolean verify)
    {
        _verifyAllLookups = verify;
    }

    @Required
    public void setPermissionHandler(PermissionHandler handler)
    {
        _permissionHandler = handler;
    }

    @Required
    public void setFileSystem(JdbcFs fs)
    {
        _fs = fs;
    }

    @Required
    public void setAclEnabled(boolean isEnabled)
    {
        _aclEnabled = isEnabled;
    }

    @Required
    public void setAtimeGap(long gap) {
        _atimeGap = TimeUnit.SECONDS.toMillis(gap);
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
        throws IOException, ChimeraFsException, CacheException
    {
        if (Subjects.isRoot(subject)) {
            return _fs.path2inode(path);
        }

        List<FsInode> inodes = _fs.path2inodes(path);
        if (_verifyAllLookups) {
            for (FsInode inode: inodes.subList(0, inodes.size() - 1)) {
                if (inode.isDirectory()) {
                    FileAttributes attributes =
                        getFileAttributesForPermissionHandler(inode);
                    if (_permissionHandler.canLookup(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + path);
                    }
                }
            }
        } else {
            for (FsInode inode: Iterables.skip(Lists.reverse(inodes), 1)) {
                if (inode.isDirectory()) {
                    FileAttributes attributes =
                        getFileAttributesForPermissionHandler(inode);
                    if (_permissionHandler.canLookup(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + path);
                    }
                    /* dCache only checks the lookup permissions of
                     * the last directory of a path.
                     */
                    break;
                }
            }
        }
        return inodes.get(inodes.size() - 1);
    }

    @Override
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
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }

        return new PnfsId(inode.toString());
    }

    @Override
    public void deleteEntry(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
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

            _fs.remove(inode);
        }catch(FileNotFoundHimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        }catch(DirNotEmptyHimeraFsException e) {
            throw new CacheException("Directory is not empty: " + pnfsId);
        }catch(IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    @Override
    public void deleteEntry(Subject subject, String path)
        throws CacheException
    {
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

            _fs.remove(path);
        }catch(FileNotFoundHimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        }catch(DirNotEmptyHimeraFsException e) {
            throw new CacheException("Directory is not empty: " + path);
        }catch(IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    @Override
    public void renameEntry(Subject subject, PnfsId pnfsId,
                            String newName, boolean overwrite)
        throws CacheException
    {
        try {
            FsInode inode = new FsInode(_fs, pnfsId.toIdString());
            FsPath source = new FsPath(_fs.inode2path(inode));
            if (source.isEmpty()) {
                throw new PermissionDeniedCacheException("Access denied: " + source);
            }

            FsInode sourceDir = _fs.getParentOf(inode);
            FileAttributes sourceDirAttributes =
                getFileAttributesForPermissionHandler(sourceDir);

            FsPath dest = new FsPath(newName);
            if (dest.isEmpty()) {
                throw new PermissionDeniedCacheException("Access denied: " + dest);
            }

            FsInode destDir;
            FileAttributes destDirAttributes;

            try {
                if (dest.getParent().equals(source.getParent())) {
                    destDir = sourceDir;
                    destDirAttributes = sourceDirAttributes;
                } else {
                    destDir = pathToInode(subject, dest.getParent().toString());
                    destDirAttributes =
                        getFileAttributesForPermissionHandler(destDir);
                }
            } catch (FileNotFoundHimeraFsException e) {
                throw new NotDirCacheException("No such directory: " +
                                               dest.getParent());
            }

            if (!Subjects.isRoot(subject) &&
                _permissionHandler.canRename(subject,
                                             sourceDirAttributes,
                                             destDirAttributes,
                                             inode.isDirectory()) != ACCESS_ALLOWED) {
                throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
            }

            if (Subjects.isRoot(subject) && overwrite) {
                _fs.move(sourceDir, source.getName(), destDir, dest.getName());
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

            _fs.move(sourceDir, source.getName(), destDir, dest.getName());
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: "
                                                 + pnfsId);
        } catch (FileExistsChimeraFsException e) {
            /* With the current implementation of Chimera, I don't
             * expect this to be thrown. Instead Chimera insists on
             * overwriting the destination file.
             */
            throw new FileExistsCacheException("File exists:" + newName);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    @Override
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

    @Override
    public List<String> getCacheLocation(Subject subject, PnfsId pnfsId) throws CacheException {

        try {
            List<String> locations = new ArrayList<>();

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

    @Override
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

    @Override
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

    @Override
    public PnfsId pathToPnfsid(Subject subject, String path, boolean followLink)
        throws CacheException
    {
    	FsInode inode;
        try {
            inode = pathToInode(subject, path);
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory " + path);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }

        return new PnfsId( inode.toString() );
    }

    @Override
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
        }
    }

    @Override
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
       sb.append("Acl Enabled: ").append(_aclEnabled).append("\n");
       sb.append("atime precision: ").append(_atimeGap < 0 ? "Disabled" : TimeUnit.MILLISECONDS.toSeconds(_atimeGap)).append("\n");
       sb.append(_fs.getInfo() );
        return sb.toString();

    }

    @Override
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
        throws IOException, ChimeraFsException, CacheException
    {
        return (inode == null)
                ? null
                : getFileAttributes(Subjects.ROOT, inode, _permissionHandler.getRequiredAttributes());
    }

    private FileAttributes getFileAttributes(Subject subject, FsInode inode,
                                             Set<FileAttribute> attr)
        throws IOException, ChimeraFsException, CacheException
    {
        if (!inode.exists()) {
            throw new FileNotFoundHimeraFsException();
        }

        FileAttributes attributes = new FileAttributes();
        Stat stat;

        for (FileAttribute attribute: attr) {
            switch (attribute) {
            case ACL:
                if(_aclEnabled) {
                    RsType rsType = inode.isDirectory() ? RsType.DIR : RsType.FILE;
                    List<ACE> acl = _fs.getACL(inode);
                    attributes.setAcl( new ACL(rsType, acl));
                } else {
                    attributes.setAcl(null);
                }
                break;
            case ACCESS_LATENCY:
                attributes.setAccessLatency(_extractor.getAccessLatency(inode));
                break;
            case ACCESS_TIME:
                stat = inode.statCache();
                attributes.setAccessTime(stat.getATime());
                break;
            case RETENTION_POLICY:
                attributes.setRetentionPolicy(_extractor.getRetentionPolicy(inode));
                break;
            case SIZE:
                stat = inode.statCache();
                attributes.setSize(stat.getSize());
                break;
            case CHANGE_TIME:
                stat = inode.statCache();
                attributes.setChangeTime(stat.getCTime());
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
                Set<Checksum> checksums = new HashSet<>();
                for (ChecksumType type: ChecksumType.values()) {
                    String value = _fs.getInodeChecksum(inode, type.getType());
                    if (value != null) {
                        checksums.add(new Checksum(type,value));
                    }
                }
                attributes.setChecksums(checksums);
                break;
            case LOCATIONS:
                List<String> locations = new ArrayList<>();
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
                Map<String,String> flags = new HashMap<>();
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
            FsInode inodeParent = _fs.getParentOf(inode);
            FileAttributes parent =
                    getFileAttributesForPermissionHandler(inodeParent);
            if (_permissionHandler.canGetAttributes(subject, parent, fileAttributes, attr) != ACCESS_ALLOWED) {
                throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
            }
            return fileAttributes;
        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
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
                        if (stat == null) {
                            stat = inode.statCache();
                        }
                        stat.setSize(attr.getSize());
                        break;
                    case MODE:
                        if (stat == null) {
                            stat = inode.statCache();
                        }
                        // FIXME this is temporary work-around: we must
                        // preserve file type (high-order bits) that
                        // the end-user doesn't provide.  This should
                        // be fixed in Chimera.
                        setModeOf(stat, attr.getMode());
                        break;
                    case ACCESS_TIME:
                        if (_atimeGap >= 0) {
                            Stat atimeStat;
                            if (stat == null) {
                                atimeStat = inode.statCache();
                            } else {
                                atimeStat = stat;
                            }
                            if (Math.abs(atimeStat.getATime() - attr.getAccessTime()) > _atimeGap) {
                                atimeStat.setATime(attr.getAccessTime());
                                // propagate update only if there is a change
                                stat = atimeStat;
                            }
                        }
                        break;
                    case OWNER:
                        if (stat == null) {
                            stat = inode.statCache();
                        }
                        stat.setUid(attr.getOwner());
                        break;
                    case OWNER_GROUP:
                        if (stat == null) {
                            stat = inode.statCache();
                        }
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
                    case FLAGS:
                        FsInode level2 = new FsInode(_fs, pnfsId.toString(), 2);
                        ChimeraCacheInfo cacheInfo = new ChimeraCacheInfo(level2);
                        for (Map.Entry<String,String> flag: attr.getFlags().entrySet()) {
                            cacheInfo.getFlags().put(flag.getKey(), flag.getValue());
                        }
                        cacheInfo.writeCacheInfo(level2);
                        break;
                    case ACL:
                        if(_aclEnabled) {
                            ACL acl = attr.getAcl();
                            _fs.setACL(inode, acl.getList());
                        }
                        break;
                    case STORAGEINFO:
                        _extractor.setStorageInfo(inode, attr.getStorageInfo());
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

    @Override
    public void list(Subject subject, String path, Glob glob, Range<Integer> range,
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

            int counter = 0;
            try (DirectoryStreamB<HimeraDirectoryEntry> dirStream = dir
                    .newDirectoryStream()) {
                for (HimeraDirectoryEntry entry : dirStream) {
                    try {
                        String name = entry.getName();
                        if (!name.equals(".") && !name.equals("..") &&
                                (pattern == null || pattern.matcher(name)
                                        .matches()) &&
                                range.contains(counter++)) {
                            // FIXME: actually, HimeraDirectoryEntry
                            // already contains most of attributes
                            FileAttributes fa =
                                    attrs.isEmpty()
                                            ? null
                                            : getFileAttributes(subject, entry
                                            .getInode(), attrs);
                            handler.addEntry(name, fa);
                        }
                    } catch (FileNotFoundHimeraFsException e) {
                        /* Not an error; files may be deleted during the
                         * list operation.
                         */
                    }
                }
            }

        } catch (FileNotFoundHimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        } catch (IOException e) {
            _log.error("Exception in list: {}", e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }
}
