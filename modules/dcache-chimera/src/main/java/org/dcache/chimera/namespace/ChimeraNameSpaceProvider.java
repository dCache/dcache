package org.dcache.chimera.namespace;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.dcache.acl.enums.AccessType.ACCESS_ALLOWED;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.NO_STAT;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.STAT;
import static org.dcache.namespace.FileAttribute.ACCESS_LATENCY;
import static org.dcache.namespace.FileAttribute.CACHECLASS;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.namespace.FileAttribute.CREATION_TIME;
import static org.dcache.namespace.FileAttribute.FLAGS;
import static org.dcache.namespace.FileAttribute.HSM;
import static org.dcache.namespace.FileAttribute.LABELS;
import static org.dcache.namespace.FileAttribute.LOCATIONS;
import static org.dcache.namespace.FileAttribute.MODE;
import static org.dcache.namespace.FileAttribute.NLINK;
import static org.dcache.namespace.FileAttribute.OWNER;
import static org.dcache.namespace.FileAttribute.OWNER_GROUP;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.RETENTION_POLICY;
import static org.dcache.namespace.FileAttribute.SIMPLE_TYPE;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.STORAGECLASS;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.namespace.FileAttribute.XATTR;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.AttributeExistsCacheException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileCorruptedCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileIsNewCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.InvalidMessageCacheException;
import diskCacheV111.util.LockedCacheException;
import diskCacheV111.util.NoAttributeCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.util.command.Command;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.auth.Subjects;
import org.dcache.chimera.ChimeraDirectoryEntry;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.DirNotEmptyChimeraFsException;
import org.dcache.chimera.DirectoryStreamB;
import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FileNotFoundChimeraFsException;
import org.dcache.chimera.FileState;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FileSystemProvider.SetXattrMode;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.NoXdataChimeraException;
import org.dcache.chimera.NotDirChimeraException;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.posix.Stat;
import org.dcache.commons.stats.MonitoringProxy;
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.namespace.PermissionHandler;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Exceptions;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

public class ChimeraNameSpaceProvider
      implements NameSpaceProvider, CellInfoProvider, CellCommandListener {

    private static final int SYMLINK_MODE = 0777;

    private static final int INHERIT_MODE = -1;

    public static final String TAG_EXPECTED_SIZE = "ExpectedSize";
    public static final String TAG_PATH = "Path";
    public static final String TAG_WRITE_TOKEN = "WriteToken";
    public static final String TAG_RETENTION_POLICY = "RetentionPolicy";
    public static final String TAG_ACCESS_LATENCY = "AccessLatency";

    // Similar to (but not the same as) similarly named constants in
    // PnfsCreateEntryMessage.
    public static final EnumSet INVALID_CREATE_DIRECTORY_ATTRIBUTES =
          EnumSet.of(CACHECLASS, CHECKSUM, CREATION_TIME, FLAGS, HSM,
                LOCATIONS, NLINK, PNFSID, RETENTION_POLICY, SIMPLE_TYPE,
                SIZE, STORAGECLASS, STORAGEINFO, TYPE);
    public static final EnumSet INVALID_CREATE_FILE_ATTRIBUTES =
          EnumSet.of(CACHECLASS, CREATION_TIME, NLINK, PNFSID, STORAGECLASS,
                STORAGEINFO, SIMPLE_TYPE, TYPE);
    public static final EnumSet INVALID_CREATE_SYM_LINK_ATTRIBUTES =
          EnumSet.of(ACCESS_LATENCY, CACHECLASS, CHECKSUM, CREATION_TIME,
                FLAGS, HSM, LOCATIONS, NLINK, PNFSID, RETENTION_POLICY,
                SIZE, STORAGECLASS, STORAGEINFO, SIMPLE_TYPE, TYPE);

    private FileSystemProvider _fs;
    private ChimeraStorageInfoExtractable _extractor;

    private static final Logger LOGGER = LoggerFactory.getLogger(ChimeraNameSpaceProvider.class);

    private boolean _inheritFileOwnership;
    private boolean _verifyAllLookups;
    private boolean _aclEnabled;
    private boolean _allowMoveToDirectoryWithDifferentStorageClass;
    private PermissionHandler _permissionHandler;
    private String _uploadDirectory;
    private String _uploadSubDirectory;

    private final ThreadLocal<Integer> threadId = new ThreadLocal<Integer>() {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        protected Integer initialValue() {
            return counter.getAndIncrement();
        }
    };
    private final RequestCounters<Method> _counters =
          new RequestCounters<>(ChimeraNameSpaceProvider.class.getSimpleName());
    private final RequestExecutionTimeGauges<Method> _gauges =
          new RequestExecutionTimeGauges<>(ChimeraNameSpaceProvider.class.getSimpleName());

    @Required
    public void setExtractor(ChimeraStorageInfoExtractable extractor) {
        _extractor = extractor;
    }

    @Required
    public void setInheritFileOwnership(boolean inherit) {
        _inheritFileOwnership = inherit;
    }

    @Required
    public void setVerifyAllLookups(boolean verify) {
        _verifyAllLookups = verify;
    }

    @Required
    public void setAllowMoveToDirectoryWithDifferentStorageClass(boolean allow) {
        _allowMoveToDirectoryWithDifferentStorageClass = allow;
    }

    @Required
    public void setPermissionHandler(PermissionHandler handler) {
        _permissionHandler = handler;
    }

    @Required
    public void setFileSystem(FileSystemProvider fs) {
        _fs = MonitoringProxy.decorateWithMonitoringProxy(new Class[]{FileSystemProvider.class}, fs,
              _counters,
              _gauges);
    }

    @Required
    public void setAclEnabled(boolean isEnabled) {
        _aclEnabled = isEnabled;
    }

    /**
     * Base directory for temporary upload directories. If not an absolute path, the directory is
     * relative to the user's root directory.
     */
    public void setUploadDirectory(String path) {
        _uploadDirectory = path;
    }

    /**
     * Sub directory in the upload directory in which to create temporary upload directories.
     * <p>
     * May be parametrised by a thread id by inserting %d into the string. This allows Chimera lock
     * contention on the base directory to be reduced. If used it is important that the same set
     * threads call into the provider repeatedly as otherwise a large number of base directories
     * will be created.
     */
    public void setUploadSubDirectory(String path) {
        _uploadSubDirectory = path;
    }

    private void checkLookupPermissions(Subject subject, List<FsInode> inodes, String path)
          throws ChimeraFsException, CacheException {
        for (FsInode inode : inodes) {
            if (inode.isDirectory()) {
                FileAttributes attributes = getFileAttributesForPermissionHandler(inode);
                if (_permissionHandler.canLookup(subject, attributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + path);
                }
            }
        }
    }

    private List<FsInode> findDeepestParent(String path) throws ChimeraFsException {
        FsPath target = FsPath.create(path);

        do {
            target = target.parent();
            try {
                return _fs.path2inodes(target.toString());
            } catch (FileNotFoundChimeraFsException ignored) {
                // Continue onto next parent.
            }
        } while (target != FsPath.ROOT);

        throw new RuntimeException("Unable to find inode for ROOT");
    }

    private ExtendedInode pathToInode(Subject subject, String path)
          throws ChimeraFsException, CacheException {
        if (Subjects.isExemptFromNamespaceChecks(subject)) {
            return new ExtendedInode(_fs, _fs.path2inode(path));
        }

        List<FsInode> inodes;
        try {
            inodes = _fs.path2inodes(path);
        } catch (FileNotFoundChimeraFsException e) {
            // Do not leak whether a file exists if user cannot 'cd' into the parent directory.
            inodes = findDeepestParent(path);
            checkLookupPermissions(subject, inodes, path);
            throw e;
        }

        if (_verifyAllLookups) {
            checkLookupPermissions(subject, inodes.subList(0, inodes.size() - 1), path);
        } else {
            for (FsInode inode : Iterables.skip(Lists.reverse(inodes), 1)) {
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
        return new ExtendedInode(_fs, inodes.get(inodes.size() - 1));
    }

    private int defaultUid(Subject subject, ExtendedInode parent)
          throws UncheckedIOException {
        try {
            if (Subjects.isNobody(subject) || _inheritFileOwnership) {
                return parent.statCache().getUid();
            } else {
                return (int) Subjects.getUid(subject);
            }
        } catch (ChimeraFsException e) {
            throw new UncheckedIOException(e);
        }
    }

    private int defaultGid(Subject subject, ExtendedInode parent)
          throws UncheckedIOException {
        try {
            if (Subjects.isNobody(subject) || _inheritFileOwnership) {
                return parent.statCache().getGid();
            } else {
                return (int) Subjects.getPrimaryGid(subject);
            }
        } catch (ChimeraFsException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public FileAttributes createFile(Subject subject, String path,
          FileAttributes assignAttributes, Set<FileAttribute> requestedAttributes)
          throws CacheException {
        checkArgument(assignAttributes.isUndefined(INVALID_CREATE_FILE_ATTRIBUTES),
              "Illegal assign attributes: %s", assignAttributes.getDefinedAttributes());
        File newEntryFile = new File(path);
        String parentPath = newEntryFile.getParent();
        try {
            if (parentPath == null) {
                throw new FileExistsCacheException("File exists: " + path);
            }
            ExtendedInode parent = pathToInode(subject, parentPath);

            if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                FileAttributes attributes
                      = getFileAttributesForPermissionHandler(parent);
                if (_permissionHandler.canCreateFile(subject, attributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + path);
                }
            }

            ExtendedInode inode;
            try {
                int uid = assignAttributes.getOwnerIfPresent()
                      .orElseGet(() -> defaultUid(subject, parent));
                int gid = assignAttributes.getGroupIfPresent()
                      .orElseGet(() -> defaultGid(subject, parent));
                int mode = assignAttributes.getModeIfPresent()
                      .orElse(parent.statCache().getMode() & UMASK_FILE);
                assignAttributes.undefine(OWNER, OWNER_GROUP, MODE);

                inode = parent.create(newEntryFile.getName(), uid, gid, mode);
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }

            FileAttributes fileAttributes;
            if (assignAttributes.getDefinedAttributes().isEmpty()) {
                fileAttributes = getFileAttributes(inode, requestedAttributes);
            } else {
                fileAttributes = setFileAttributes(subject, inode.getPnfsId(),
                      assignAttributes, requestedAttributes);
            }

            if (parent.getTags().containsKey(TAG_EXPECTED_SIZE)) {
                ImmutableList<String> size = parent.getTag(TAG_EXPECTED_SIZE);
                if (!size.isEmpty()) {
                    fileAttributes.setSize(Long.parseLong(size.get(0)));
                }
            }
            return fileAttributes;
        } catch (NotDirChimeraException e) {
            throw new NotDirCacheException("Not a directory: " + parentPath);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such directory: " + parentPath);
        } catch (FileExistsChimeraFsException e) {
            throw new FileExistsCacheException("File exists: " + path);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    @Override
    public PnfsId createDirectory(Subject subject, String path, FileAttributes attributes)
          throws CacheException {
        checkArgument(attributes.isUndefined(INVALID_CREATE_DIRECTORY_ATTRIBUTES),
              "Illegal assign attributes: %s", attributes.getDefinedAttributes());
        try {
            File newEntryFile = new File(path);
            String parentPath = newEntryFile.getParent();
            if (parentPath == null) {
                throw new FileExistsCacheException("File exists: " + path);
            }
            ExtendedInode parent = pathToInode(subject, parentPath);

            ExtendedInode inode;
            try {
                int uid = attributes.getOwnerIfPresent()
                      .orElseGet(() -> defaultUid(subject, parent));
                int gid = attributes.getGroupIfPresent()
                      .orElseGet(() -> defaultGid(subject, parent));
                int mode = attributes.getModeIfPresent()
                      .orElse(parent.statCache().getMode() & UMASK_DIR);
                attributes.undefine(OWNER, OWNER_GROUP, MODE);

                inode = mkdir(subject, parent, newEntryFile.getName(), uid, gid,
                      mode);
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }

            if (!attributes.getDefinedAttributes().isEmpty()) {
                setFileAttributes(subject, inode.getPnfsId(), attributes,
                      EnumSet.noneOf(FileAttribute.class));
            }
            return inode.getPnfsId();
        } catch (NotDirChimeraException e) {
            throw new NotDirCacheException("Not a directory: " + path);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        } catch (FileExistsChimeraFsException e) {
            throw new FileExistsCacheException("File exists: " + path);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    @Override
    public PnfsId createSymLink(Subject subject, String path, String dest,
          FileAttributes assignAttributes)
          throws CacheException {
        checkArgument(assignAttributes.isUndefined(INVALID_CREATE_SYM_LINK_ATTRIBUTES),
              "Illegal assign attributes: %s", assignAttributes.getDefinedAttributes());
        try {
            File newEntryFile = new File(path);
            String parentPath = newEntryFile.getParent();
            if (parentPath == null) {
                throw new FileExistsCacheException("File exists: " + path);
            }
            ExtendedInode parent = pathToInode(subject, parentPath);

            if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                FileAttributes attributes
                      = getFileAttributesForPermissionHandler(parent);
                if (_permissionHandler.canCreateFile(subject, attributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + path);
                }
            }

            FsInode inode;
            try {
                int uid = assignAttributes.getOwnerIfPresent()
                      .orElseGet(() -> defaultUid(subject, parent));
                int gid = assignAttributes.getGroupIfPresent()
                      .orElseGet(() -> defaultGid(subject, parent));
                int mode = assignAttributes.getModeIfPresent().orElse(SYMLINK_MODE);
                assignAttributes.undefine(OWNER, OWNER_GROUP, MODE);

                inode = _fs.createLink(parent, newEntryFile.getName(), uid, gid,
                      mode, dest.getBytes(UTF_8));
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }

            PnfsId pnfsid = new PnfsId(inode.getId());

            if (!assignAttributes.getDefinedAttributes().isEmpty()) {
                setFileAttributes(subject, pnfsid, assignAttributes,
                      EnumSet.noneOf(FileAttribute.class));
            }
            return pnfsid;
        } catch (NotDirChimeraException e) {
            throw new NotDirCacheException("Not a directory: " + path);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        } catch (FileExistsChimeraFsException e) {
            throw new FileExistsCacheException("File exists: " + path);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    private void checkAllowed(Set<FileType> allowed,
          ExtendedInode inode)
          throws ChimeraFsException, NotDirCacheException, NotFileCacheException {
        FileType type = inode.getFileType();
        if (!allowed.contains(type)) {
            StringBuilder sb = new StringBuilder("Path exists and has type ")
                  .append(type).append(", which is not ");
            if (allowed.size() == 1) {
                FileType allowedType = allowed.iterator().next();
                sb.append(allowedType);
            } else {
                String description = allowed.stream()
                      .map(FileType::toString)
                      .collect(Collectors.joining(", ", "{", "}"));
                sb.append("one of ").append(description);
            }

            if (allowed.contains(FileType.DIR)) {
                throw new NotDirCacheException(sb.toString());
            } else {
                throw new NotFileCacheException(sb.toString());
            }
        }
    }

    private boolean canDelete(Subject subject, ExtendedInode parent, ExtendedInode inode)
          throws ChimeraFsException, CacheException {
        FileAttributes parentAttributes = getFileAttributesForPermissionHandler(parent);
        FileAttributes fileAttributes = getFileAttributesForPermissionHandler(inode);

        if (inode.isDirectory()) {
            if (_permissionHandler.canDeleteDir(subject,
                  parentAttributes,
                  fileAttributes) != ACCESS_ALLOWED) {
                return false;
            }
        } else {
            if (_permissionHandler.canDeleteFile(subject,
                  parentAttributes,
                  fileAttributes) != ACCESS_ALLOWED) {
                return false;
            }
        }
        return true;
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed, PnfsId pnfsId,
          Set<FileAttribute> attr) throws CacheException {
        try {
            ExtendedInode inode = new ExtendedInode(_fs, pnfsId, STAT);

            checkAllowed(allowed, inode);

            if (!Subjects.isExemptFromNamespaceChecks(subject) && !canDelete(subject,
                  inode.getParent(), inode)) {
                throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
            }

            _fs.remove(inode);

            return getFileAttributes(inode, attr);
        } catch (FileNotFoundChimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        } catch (DirNotEmptyChimeraFsException e) {
            throw new CacheException("Directory is not empty: " + pnfsId);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
          String path, Set<FileAttribute> attr) throws CacheException {
        try {
            File filePath = new File(path);

            String parentPath = filePath.getParent();
            if (parentPath == null) {
                throw new CacheException("Cannot delete file system root.");
            }

            ExtendedInode parent = pathToInode(subject, parentPath);
            String name = filePath.getName();

            ExtendedInode inode = parent.inodeOf(name, STAT);
            checkAllowed(allowed, inode);

            if (!Subjects.isExemptFromNamespaceChecks(subject) && !canDelete(subject, parent,
                  inode)) {
                throw new PermissionDeniedCacheException("Access denied: " + path);
            }

            _fs.remove(parent, name, inode);

            return getFileAttributes(inode, attr);
        } catch (FileNotFoundChimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        } catch (DirNotEmptyChimeraFsException e) {
            throw new CacheException("Directory is not empty: " + path);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
          PnfsId pnfsId, String path, Set<FileAttribute> attr) throws CacheException {
        try {
            File filePath = new File(path);

            String parentPath = filePath.getParent();
            if (parentPath == null) {
                throw new CacheException("Cannot delete file system root.");
            }

            ExtendedInode parent = pathToInode(subject, parentPath);
            String name = filePath.getName();
            ExtendedInode inode = parent.inodeOf(name, STAT);

            if (!inode.getPnfsId().equals(pnfsId)) {
                throw new FileNotFoundCacheException(
                      "PNFSID does not correspond to provided file.");
            }

            checkAllowed(allowed, inode);

            if (!Subjects.isExemptFromNamespaceChecks(subject) && !canDelete(subject, parent,
                  inode)) {
                throw new PermissionDeniedCacheException("Access denied: " + path);
            }

            _fs.remove(parent, name, inode);

            return getFileAttributes(inode, attr);
        } catch (FileNotFoundChimeraFsException fnf) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        } catch (DirNotEmptyChimeraFsException e) {
            throw new CacheException("Directory is not empty: " + path);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void rename(Subject subject, @Nullable PnfsId pnfsId,
          String sourcePath, String destinationPath, boolean overwrite)
          throws CacheException {
        try {
            /* Resolve the source directory.
             */
            File source = new File(sourcePath);
            ExtendedInode sourceDir = pathToInode(subject, source.getParent());
            Set<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
            attributes.addAll(_permissionHandler.getRequiredAttributes());
            if (!_allowMoveToDirectoryWithDifferentStorageClass) {
                attributes.add(FileAttribute.STORAGECLASS);
                attributes.add(FileAttribute.CACHECLASS);
            }
            FileAttributes sourceDirAttributes = getFileAttributes(sourceDir, attributes);

            ExtendedInode inode;
            if (pnfsId != null) {
                inode = new ExtendedInode(_fs, pnfsId, STAT);
            } else {
                if (!Subjects.isExemptFromNamespaceChecks(subject) &&
                      _permissionHandler.canLookup(subject, sourceDirAttributes)
                            != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + sourcePath);
                }
                inode = sourceDir.inodeOf(source.getName(), STAT);
            }

            /* Resolve the target directory.
             */
            File dest = new File(destinationPath);
            ExtendedInode destDir;
            FileAttributes destDirAttributes;
            try {
                if (dest.getParent().equals(source.getParent())) {
                    destDir = sourceDir;
                    destDirAttributes = sourceDirAttributes;
                } else {
                    destDir = pathToInode(subject, dest.getParent());
                    destDirAttributes = getFileAttributes(destDir, attributes);
                    if (!_allowMoveToDirectoryWithDifferentStorageClass) {
                        FileAttributes sourceAttributes = sourceDirAttributes;
                        if (inode.isDirectory()) {
                            sourceAttributes =
                                  getFileAttributes(new ExtendedInode(_fs, inode), attributes);
                        }
                        if (!(nullToEmpty(destDirAttributes.getStorageClass()).
                              equals(nullToEmpty(sourceAttributes.getStorageClass())) &&
                              nullToEmpty(destDirAttributes.getCacheClass()).
                                    equals(nullToEmpty(sourceAttributes.getCacheClass())))) {
                            throw new PermissionDeniedCacheException("Mv denied: " +
                                  dest.getParent() +
                                  " has different storage tags; use cp.");
                        }
                    }
                }
            } catch (FileNotFoundChimeraFsException e) {
                throw new NotDirCacheException("No such directory: " +
                      dest.getParent());
            }

            /* Permission checks.
             */
            if (!Subjects.isExemptFromNamespaceChecks(subject) || !overwrite) {
                if (!Subjects.isExemptFromNamespaceChecks(subject) &&
                      _permissionHandler.canRename(subject,
                            sourceDirAttributes,
                            destDirAttributes,
                            inode.isDirectory()) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
                }

                try {
                    ExtendedInode destInode = destDir.inodeOf(dest.getName(), STAT);
                    if (!overwrite) {
                        throw new FileExistsCacheException("File exists:" + destinationPath);
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
                            throw new PermissionDeniedCacheException(
                                  "Access denied: " + destinationPath);
                        }
                    } else {
                        if (_permissionHandler.canDeleteFile(subject,
                              destDirAttributes,
                              destAttributes) != ACCESS_ALLOWED) {
                            throw new PermissionDeniedCacheException(
                                  "Access denied: " + destinationPath);
                        }
                    }
                } catch (FileNotFoundChimeraFsException e) {
                    /* Destination doesn't exist and we can move the file;
                     * unfortunately there is no way to test this with
                     * Chimera without throwing an exception.
                     */
                }
            }

            _fs.rename(inode, sourceDir, source.getName(), destDir, dest.getName());
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: "
                  + pnfsId);
        } catch (FileExistsChimeraFsException e) {
            /* With the current implementation of Chimera, I don't
             * expect this to be thrown. Instead Chimera insists on
             * overwriting the destination file.
             */
            throw new FileExistsCacheException("File exists:" + destinationPath);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    @Override
    public void addCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation)
          throws CacheException {

        LOGGER.debug("add cache location {} for {}", cacheLocation, pnfsId);

        try {
            ExtendedInode inode = new ExtendedInode(_fs, pnfsId, NO_STAT);
            _fs.addInodeLocation(inode, StorageGenericLocation.DISK, cacheLocation);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file: " + pnfsId);
        } catch (ChimeraFsException e) {
            LOGGER.error("Exception in addCacheLocation {}", e.getMessage());
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public List<String> getCacheLocation(Subject subject, PnfsId pnfsId) throws CacheException {

        try {
            List<String> locations = new ArrayList<>();

            ExtendedInode inode = new ExtendedInode(_fs, pnfsId, NO_STAT);
            List<StorageLocatable> localyManagerLocations = _fs.getInodeLocations(inode,
                  StorageGenericLocation.DISK);

            for (StorageLocatable location : localyManagerLocations) {
                locations.add(location.location());
            }

            return locations;
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void clearCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation,
          boolean removeIfLast) throws CacheException {

        LOGGER.debug("clearCacheLocation : {} for {}", cacheLocation, pnfsId);

        try {
            ExtendedInode inode = new ExtendedInode(_fs, pnfsId, NO_STAT);

            _fs.clearInodeLocation(inode, StorageGenericLocation.DISK, cacheLocation);

            if (removeIfLast) {
                List<StorageLocatable> locations = _fs.getInodeLocations(inode,
                      StorageGenericLocation.DISK);
                if (locations.isEmpty()) {
                    LOGGER.debug("last location cleaned. removing file {}", inode);
                    _fs.remove(inode);
                }
            }
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId, e);
        } catch (ChimeraFsException e) {
            LOGGER.error("Exception in clearCacheLocation for {} : {}", pnfsId, e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public String pnfsidToPath(Subject subject, PnfsId pnfsId) throws CacheException {
        try {
            ExtendedInode inode = new ExtendedInode(_fs, pnfsId, STAT);
            if (!inode.exists()) {
                throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
            }
            return _fs.inode2path(inode);
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public PnfsId pathToPnfsid(Subject subject, String path, boolean followLink)
          throws CacheException {
        try {
            return pathToInode(subject, path).getPnfsId();
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory " + path);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void removeFileAttribute(Subject subject, PnfsId pnfsId, String attribute)
          throws CacheException {
        try {
            ExtendedInode inode = new ExtendedInode(_fs, pnfsId, NO_STAT).getLevel(2);
            ChimeraCacheInfo info = new ChimeraCacheInfo(inode);
            ChimeraCacheInfo.CacheFlags flags = info.getFlags();
            flags.remove(attribute);
            info.writeCacheInfo(inode);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory " + pnfsId);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    @Override
    public void removeChecksum(Subject subject, PnfsId pnfsId, ChecksumType type)
          throws CacheException {
        try {
            _fs.removeInodeChecksum(new ExtendedInode(_fs, pnfsId, NO_STAT), type.getType());
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    @Override
    public CellInfo getCellInfo(CellInfo info) {
        return info;
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.append("Acl Enabled: ").println(_aclEnabled);
        pw.append(_fs.getInfo());
        pw.println("Statistics:");
        pw.println(_gauges);
        pw.println(_counters);
    }


    @Command(name = "reset chimera stats", hint="reset chimera statistics", description = "Reset"
            + " the counters and gauge statistics describing the interaction with Chimera.")
    public class ResetStatsCommand implements Callable<String> {
        @Override
        public String call() {
            _gauges.reset();
            _counters.reset();
            return "";
        }
    }

    @Override
    public Collection<Link> find(Subject subject, PnfsId pnfsId) throws CacheException {
        try {
            ExtendedInode target = new ExtendedInode(_fs, pnfsId, NO_STAT);

            Collection<Link> locations = _fs.find(target).stream()
                  .flatMap(l -> {
                      try {
                          ExtendedInode parent = new ExtendedInode(_fs, l.getParent());
                          PnfsId parentId = parent.getPnfsId();
                          return Stream.of(new Link(parentId, l.getName()));
                      } catch (ChimeraFsException e) {
                          LOGGER.error("Failed to find PnfsId of parent {}: {}",
                                l.getParent(), e.toString());
                          return Stream.of();
                      }
                  })
                  .collect(Collectors.toList());
            if (locations.isEmpty()) {
                throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
            }
            return locations;
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    private FileAttributes getFileAttributesForPermissionHandler(@Nullable FsInode inode)
          throws ChimeraFsException, CacheException {
        return (inode == null)
              ? null
              : getFileAttributesForPermissionHandler(new ExtendedInode(_fs, inode));
    }

    private FileAttributes getFileAttributesForPermissionHandler(@Nonnull ExtendedInode inode)
          throws ChimeraFsException, CacheException {
        return getFileAttributes(inode, _permissionHandler.getRequiredAttributes());
    }

    private FileAttributes getFileAttributes(ExtendedInode inode, Set<FileAttribute> attr)
          throws ChimeraFsException, CacheException {
        if (!inode.exists()) {
            throw FileNotFoundChimeraFsException.of(inode);
        }

        FileAttributes attributes = new FileAttributes();
        Stat stat;

        for (FileAttribute attribute : attr) {
            switch (attribute) {
                case ACL:
                    if (_aclEnabled) {
                        attributes.setAcl(inode.getAcl());
                    } else {
                        attributes.setAcl(null);
                    }
                    break;
                case ACCESS_LATENCY:
                    AccessLatency accessLatency = _extractor.getAccessLatency(inode);
                    if (accessLatency != null) {
                        attributes.setAccessLatency(accessLatency);
                    }
                    break;
                case ACCESS_TIME:
                    stat = inode.statCache();
                    attributes.setAccessTime(stat.getATime());
                    break;
                case RETENTION_POLICY:
                    RetentionPolicy retentionPolicy = _extractor.getRetentionPolicy(inode);
                    if (retentionPolicy != null) {
                        attributes.setRetentionPolicy(retentionPolicy);
                    }
                    break;
                case SIZE:
                    stat = inode.statCache();
		    // REVISIT when we have another way to detect new files
		    ExtendedInode level2 = inode.getLevel(2);
		    boolean isNew = (stat.getSize() == 0) &&
			!level2.exists() &&
			inode.getLocations().isEmpty();
		    if (!isNew) {
			attributes.setSize(stat.getSize());
		    }
                    break;
                case CHANGE_TIME:
                    stat = inode.statCache();
                    attributes.setChangeTime(stat.getCTime());
                    break;
                case CREATION_TIME:
                    stat = inode.statCache();
                    attributes.setCreationTime(stat.getCrTime());
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
                    attributes.setChecksums(Sets.newHashSet(inode.getChecksums()));
                    break;
                case LOCATIONS:
		    attributes.setLocations(Lists.newArrayList(inode.getLocations(StorageGenericLocation.DISK)));
                    break;
                case FLAGS:
		    attributes.setFlags(Maps.newHashMap(inode.getFlags()));
		    break;
                case SIMPLE_TYPE:
                case TYPE:
                    attributes.setFileType(inode.getFileType());
                    break;
                case MODE:
                    stat = inode.statCache();
                    attributes.setMode(stat.getMode() & UnixPermission.S_PERMS);
                    break;
                case PNFSID:
                    attributes.setPnfsId(inode.getPnfsId());
                    break;
                case STORAGEINFO:
                case STORAGECLASS:
                case CACHECLASS:
                case HSM:
                    if (!attributes.isDefined(FileAttribute.STORAGEINFO)) {
                        StorageInfo storageInfo = _extractor.getStorageInfo(inode);
                        attributes.setStorageInfo(storageInfo);
                        attributes.setStorageClass(storageInfo.getStorageClass());
                        attributes.setCacheClass(storageInfo.getCacheClass());
                        attributes.setHsm(storageInfo.getHsm());
                    }
                    break;
                case NLINK:
                    stat = inode.statCache();
                    attributes.setNlink(stat.getNlink());
                    break;
                case XATTR:
                    Set<String> names = _fs.listXattrs(inode);
                    Map<String, String> xattrs = new HashMap<>(names.size());
                    for (String name : names) {
                        byte[] data = _fs.getXattr(inode, name);
                        xattrs.put(name, new String(data, StandardCharsets.UTF_8));
                    }
                    attributes.setXattrs(xattrs);
                    break;
                case LABELS:
                    attributes.setLabels(_fs.getLabels(inode));
                    break;
                default:
                    throw new UnsupportedOperationException(
                          "Attribute " + attribute + " not supported yet.");
            }
        }
        return attributes;
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
          Set<FileAttribute> attr)
          throws CacheException {
        try {
            ExtendedInode inode = new ExtendedInode(_fs, pnfsId, STAT);

            if (Subjects.isExemptFromNamespaceChecks(subject)) {
                return getFileAttributes(inode, attr);
            }

            /* If we have to authorize the check then we fetch
             * permission handler attributes in addition to the
             * attributes requested by the caller.
             */
            Set<FileAttribute> required = EnumSet.noneOf(FileAttribute.class);
            required.addAll(_permissionHandler.getRequiredAttributes());
            required.addAll(attr);
            FileAttributes fileAttributes =
                  getFileAttributes(inode, required);

            /* The permission check is performed after we fetched the
             * attributes to avoid fetching the attributes twice.
             */
            if (_permissionHandler.canGetAttributes(subject, fileAttributes, attr)
                  != ACCESS_ALLOWED) {
                throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
            }
            return fileAttributes;
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    @Override
    public FileAttributes setFileAttributes(Subject subject, PnfsId pnfsId,
          FileAttributes attr, Set<FileAttribute> acquire)
          throws CacheException {
        LOGGER.debug("File attributes update: {}", attr.getDefinedAttributes());

        try {
            ExtendedInode inode = new ExtendedInode(_fs, pnfsId,
                  Subjects.isExemptFromNamespaceChecks(subject) ? NO_STAT : STAT);

            if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                FileAttributes attributes =
                      getFileAttributesForPermissionHandler(inode);

                if (_permissionHandler.canSetAttributes(subject, attributes,
                      attr) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + pnfsId);
                }
            }

            /* Update the t_inodes row first (the Stat object) to acquire a FOR UPDATE / FOR NO KEY UPDATE
             * first. If the inserts into secondary table referring t_inodes would be done first, the
             * referential integrity check would obtain a FOR SHARE / FOR KEY SHARE on the t_inodes row which
             * latter would have to be upgraded (potentially leading to deadlocks if that's not possible).
             */
            Stat stat = new Stat();
            for (FileAttribute attribute : attr.getDefinedAttributes()) {
                switch (attribute) {
                    case LOCATIONS:
                        // new location with size indicates upload
                        // REVISIT: may be we need an explicit indication from pool
                        if (attr.isDefined(SIZE)) {
                            stat.setState(FileState.STORED);
                        }
                        break;
                    case SIZE:
                        // REVISIT: pool shouldn't update the files size on flush, but this is required due to space manager accounting
                        if (!attr.isDefined(STORAGEINFO) || !attr.getStorageInfo()
                              .isSetAddLocation()) {
                            stat.setSize(attr.getSize());
                        }
                        break;
                    case MODE:
                        stat.setMode(attr.getMode());
                        break;
                    case CREATION_TIME:
                        stat.setCrTime(attr.getCreationTime());
                        break;
                    case CHANGE_TIME:
                        stat.setCTime(attr.getChangeTime());
                        break;
                    case MODIFICATION_TIME:
                        stat.setMTime(attr.getModificationTime());
                        break;
                    case ACCESS_TIME:
                        stat.setATime(attr.getAccessTime());
                        break;
                    case OWNER:
                        stat.setUid(attr.getOwner());
                        break;
                    case OWNER_GROUP:
                        stat.setGid(attr.getGroup());
                        break;
                    case CHECKSUM:
                        break;
                    case ACCESS_LATENCY:
                        stat.setAccessLatency(attr.getAccessLatency());
                        break;
                    case RETENTION_POLICY:
                        stat.setRetentionPolicy(attr.getRetentionPolicy());
                        break;
                    case FLAGS:
                        break;
                    case ACL:
                        break;
                    case STORAGEINFO:
                        _extractor.setStorageInfo(inode, attr.getStorageInfo());
                        break;
                    case XATTR:
                        break;
                    case LABELS:
                        break;
                    default:
                        throw new UnsupportedOperationException(
                              "Attribute " + attribute + " not supported yet.");
                }
            }
            if (stat.isDefinedAny()) {
                inode.setStat(stat);
            }

            if (attr.isDefined(XATTR)) {
                Map<String, String> xattrs = attr.getXattrs();

                for (Map.Entry<String, String> e : xattrs.entrySet()) {
                    _fs.setXattr(inode, e.getKey(),
                          e.getValue().getBytes(StandardCharsets.UTF_8),
                          SetXattrMode.EITHER);
                }
            }

            if (attr.isDefined(LABELS)) {
                for (String label : attr.getLabels()) {
                    _fs.addLabel(inode, label);
                }
            }

            if (attr.isDefined(FileAttribute.LOCATIONS)) {
                for (String location : attr.getLocations()) {
                    _fs.addInodeLocation(inode, StorageGenericLocation.DISK, location);
                }
            }

            if (attr.isDefined(FileAttribute.CHECKSUM)) {
                for (Checksum newChecksum : attr.getChecksums()) {
                    ChecksumType type = newChecksum.getType();

                    Optional<Checksum> existingChecksum = _fs.getInodeChecksums(inode).stream()
                          .filter(c -> c.getType() == type)
                          .findFirst();

                    if (existingChecksum.isPresent()) {
                        Checksum existing = existingChecksum.get();
                        if (!existing.equals(newChecksum)) {
                            throw new FileCorruptedCacheException(existing, newChecksum);
                        }
                    } else {
                        _fs.setInodeChecksum(inode, type.getType(), newChecksum.getValue());
                    }
                }
            }

            if (attr.isDefined(FileAttribute.FLAGS)) {
                FsInode level2 = new ExtendedInode(_fs, pnfsId, NO_STAT).getLevel(2);
                ChimeraCacheInfo cacheInfo = new ChimeraCacheInfo(level2);
                for (Map.Entry<String, String> flag : attr.getFlags().entrySet()) {
                    cacheInfo.getFlags().put(flag.getKey(), flag.getValue());
                }
                cacheInfo.writeCacheInfo(level2);
            }

            if (attr.isDefined(FileAttribute.ACL)) {
                ACL acl = attr.getAcl();
                _fs.setACL(inode, acl.getList());
            }

            return getFileAttributes(inode, acquire);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
        } catch (IOException e) {
            LOGGER.error("Exception in setFileAttributes: {}", e.getMessage());
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }

    }

    @Override
    public void list(Subject subject, String path, Glob glob, Range<Integer> range,
          Set<FileAttribute> attrs, ListHandler handler)
          throws CacheException {
        try {
            Pattern pattern = (glob == null) ? null : glob.toPattern();
            ExtendedInode dir = pathToInode(subject, path);
            if (!dir.isDirectory()) {
                throw new NotDirCacheException("Not a directory: " + path);
            }

            if (!Subjects.isExemptFromNamespaceChecks(subject)) {
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
            try (DirectoryStreamB<ChimeraDirectoryEntry> dirStream = dir
                  .newDirectoryStream()) {
                for (ChimeraDirectoryEntry entry : dirStream) {
                    try {
                        String name = entry.getName();
                        if (!name.equals(".") && !name.equals("..") &&
                              (pattern == null || pattern.matcher(name)
                                    .matches()) &&
                              range.contains(counter++)) {
                            // FIXME: actually, ChimeraDirectoryEntry
                            // already contains most of attributes
                            FileAttributes fa =
                                  attrs.isEmpty()
                                        ? null
                                        : getFileAttributes(
                                              new ExtendedInode(_fs, entry.getInode()), attrs);
                            handler.addEntry(name, fa);
                        }
                    } catch (FileNotFoundChimeraFsException e) {
                        /* Not an error; files may be deleted during the
                         * list operation.
                         */
                    }
                }
            }

        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        } catch (IOException e) {
            LOGGER.error("Exception in list: {}", e.getMessage());
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }


    @Override
    public void listVirtualDirectory(Subject subject, String path, Range<Integer> range,
                     Set<FileAttribute> attrs, ListHandler handler)
            throws CacheException
    {
        try {
            int counter = 0;
            try (DirectoryStreamB<ChimeraDirectoryEntry> dirStream = FsInode.getRoot(_fs)
                    .virtualDirectoryStream(path)) {
                for (ChimeraDirectoryEntry entry : dirStream) {
                    try {
                        String name = entry.getName();
                        if (!name.equals(".") && !name.equals("..") &&
                                range.contains(counter++)) {
                            // FIXME: actually, ChimeraDirectoryEntry
                            // already contains most of attributes
                            FileAttributes fa =
                                    attrs.isEmpty()
                                            ? null
                                            : getFileAttributes(new ExtendedInode(_fs, entry.getInode()), attrs);
                            handler.addEntry(name, fa);
                        }
                    } catch (FileNotFoundChimeraFsException e) {
                        /* Not an error; files may be deleted during the
                         * list operation.
                         */
                    }
                }
            }

        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        } catch (IOException e) {
            LOGGER.error("Exception in list: {}", e.getMessage());
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    private ExtendedInode mkdir(Subject subject, ExtendedInode parent, String name, int uid,
          int gid, int mode)
          throws ChimeraFsException, CacheException {
        if (!Subjects.isExemptFromNamespaceChecks(subject)) {
            FileAttributes attributesOfParent
                  = getFileAttributesForPermissionHandler(parent);
            if (_permissionHandler.canCreateSubDir(subject, attributesOfParent) != ACCESS_ALLOWED) {
                throw new PermissionDeniedCacheException(
                      "Access denied: " + parent.getPath().child(name));
            }
        }
        return parent.mkdir(name, uid, gid, mode);
    }

    private ExtendedInode installSystemDirectory(FsPath path, int mode, List<ACE> acl,
          Map<String, byte[]> tags)
          throws ChimeraFsException, CacheException {
        ExtendedInode inode;
        try {
            inode = lookupDirectory(Subjects.ROOT, path);
        } catch (FileNotFoundCacheException e) {
            ExtendedInode parentOfPath = installDirectory(Subjects.ROOT, path.parent(), mode);
            try {
                inode = parentOfPath.mkdir(path.name(), 0, 0, mode, acl, tags);
            } catch (FileExistsChimeraFsException e1) {
                /* Concurrent directory creation. Current transaction is invalid.
                 */
                throw new LockedCacheException(
                      "Concurrent access prevented this operation from completing. Please retry.");
            }
        }
        return inode;
    }

    private ExtendedInode installDirectory(Subject subject, FsPath path, int mode)
          throws ChimeraFsException, CacheException {
        ExtendedInode inode;
        try {
            inode = lookupDirectory(subject, path);
        } catch (FileNotFoundCacheException e) {
            ExtendedInode parentOfPath = installDirectory(subject, path.parent(), mode);
            try {
                int uid = defaultUid(subject, parentOfPath);
                int gid = defaultGid(subject, parentOfPath);
                inode = mkdir(subject, parentOfPath, path.name(), uid, gid,
                      mode == INHERIT_MODE ? (parentOfPath.statCache().getMode() & UMASK_DIR)
                            : mode);
            } catch (FileExistsChimeraFsException e1) {
                /* Concurrent directory creation. Current transaction is invalid.
                 */
                throw new LockedCacheException(
                      "Concurrent access prevented this operation from completing. Please retry.");
            }
        }
        return inode;
    }

    private ExtendedInode lookupDirectory(Subject subject, FsPath path)
          throws ChimeraFsException, CacheException {
        try {
            ExtendedInode inode = pathToInode(subject, path.toString());
            if (!inode.isDirectory()) {
                throw new NotDirCacheException("Not a directory: " + path);
            }
            return inode;
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file or directory: " + path, e);
        }
    }

    @Override
    public FsPath createUploadPath(Subject subject, FsPath path, FsPath rootPath,
          Long size, AccessLatency al, RetentionPolicy rp, String spaceToken,
          Set<CreateOption> options)
          throws CacheException {
        checkState(_uploadDirectory != null, "Upload directory is not configured.");

        try {
            /* Parent directory must exist.
             */
            ExtendedInode parentOfPath =
                  options.contains(CreateOption.CREATE_PARENTS)
                        ? installDirectory(subject, path.parent(), INHERIT_MODE)
                        : lookupDirectory(subject, path.parent());

            FileAttributes attributesOfParent =
                  !Subjects.isExemptFromNamespaceChecks(subject)
                        ? getFileAttributesForPermissionHandler(parentOfPath)
                        : null;

            /* File must not exist unless overwrite is enabled.
             */
            try {
                ExtendedInode inodeOfPath = parentOfPath.inodeOf(path.name(), STAT);
                if (!options.contains(CreateOption.OVERWRITE_EXISTING) ||
                      (inodeOfPath.statCache().getMode() & UnixPermission.S_TYPE)
                            != UnixPermission.S_IFREG) {
                    throw new FileExistsCacheException("File exists: " + path);
                }
                /* User must be authorized to delete existing file.
                 */
                if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                    FileAttributes attributesOfPath =
                          getFileAttributesForPermissionHandler(inodeOfPath);
                    if (_permissionHandler.canDeleteFile(subject,
                          attributesOfParent,
                          attributesOfPath) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + path);
                    }
                }
            } catch (FileNotFoundChimeraFsException ignored) {
            }

            /* User must be authorized to create file.
             */
            if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                if (_permissionHandler.canCreateFile(subject, attributesOfParent)
                      != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + path);
                }
            }

            /* Attributes are inherited from real parent directory.
             */
            int mode = parentOfPath.statCache().getMode() & UnixPermission.S_PERMS;
            int gid;
            if ((mode & UnixPermission.S_ISGID) != 0) {
                gid = parentOfPath.statCache().getGid();
            } else if (Subjects.isNobody(subject) || _inheritFileOwnership) {
                gid = parentOfPath.statCache().getGid();
            } else {
                gid = Ints.checkedCast(Subjects.getPrimaryGid(subject));
            }
            int uid;
            if (Subjects.isNobody(subject) || _inheritFileOwnership) {
                uid = parentOfPath.statCache().getUid();
            } else {
                uid = Ints.checkedCast(Subjects.getUid(subject));
            }

            /* ACLs are copied from real parent to the temporary upload directory
             * such that the upload is allowed (in case write permissions rely
             * on ACLs) and such that the file will inherit the correct ACLs.
             */
            List<ACE> acl = _fs.getACL(parentOfPath);

            /* The temporary upload directory has the same tags as the real parent,
             * except target file specific properties are stored as tags local to
             * the upload directory.
             */
            Map<String, byte[]> tags = Maps.newHashMap(parentOfPath.getTags());
            if (spaceToken != null) {
                tags.put(TAG_WRITE_TOKEN, spaceToken.getBytes(UTF_8));

                /* If client provides space token to upload to, the access latency and
                 * retention policy tags of the upload directory must be disregarded.
                 */
                tags.remove(TAG_ACCESS_LATENCY);
                tags.remove(TAG_RETENTION_POLICY);
            }
            if (al != null) {
                tags.put(TAG_ACCESS_LATENCY, al.toString().getBytes(UTF_8));
            }
            if (rp != null) {
                tags.put(TAG_RETENTION_POLICY, rp.toString().getBytes(UTF_8));
            }
            if (size != null) {
                tags.put(TAG_EXPECTED_SIZE, size.toString().getBytes(UTF_8));
            }
            tags.put(TAG_PATH, path.toString().getBytes(UTF_8));

            /* Upload directory may optionally be relative to the user's root path. Whether
             * that's the case depends on if the configured upload directory is an absolute
             * or relative path.
             */
            FsPath uploadDirectory = rootPath.resolve(_uploadDirectory);
            if (_uploadSubDirectory != null) {
                uploadDirectory = uploadDirectory.chroot(
                      String.format(_uploadSubDirectory, threadId.get()));
            }

            /* Upload directory must exist and have the right permissions.
             */
            ExtendedInode inodeOfUploadDir = installSystemDirectory(uploadDirectory, 0711,
                  Collections.emptyList(), Collections.emptyMap());
            if (inodeOfUploadDir.statCache().getUid() != 0) {
                LOGGER.error("Owner must be root: {}", uploadDirectory);
                throw new CacheException("Owner must be root: " + uploadDirectory);
            }
            if ((inodeOfUploadDir.statCache().getMode() & UnixPermission.S_PERMS) != 0711) {
                LOGGER.error("File mode must be 0711: {}", uploadDirectory);
                throw new CacheException("File mode must be 0711: " + uploadDirectory);
            }

            /* Use cryptographically strong pseudo random UUID to create temporary upload directory.
             */
            UUID uuid = UUID.randomUUID();
            _fs.mkdir(inodeOfUploadDir, uuid.toString(), uid, gid, mode, acl, tags);

            return uploadDirectory.child(uuid.toString()).child(path.name());
        } catch (ChimeraFsException e) {
            LOGGER.error("Problem with database: {}", e.getMessage());
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    protected void checkIsTemporaryDirectory(FsPath temporaryPath, FsPath temporaryDir)
          throws NotFileCacheException, InvalidMessageCacheException {
        checkState(_uploadDirectory != null, "Upload directory is not configured.");
        FsPath temporaryDirContainer = getParentOfFile(temporaryDir);
        if (_uploadDirectory.startsWith("/")) {
            if (!temporaryDirContainer.hasPrefix(FsPath.create(_uploadDirectory))) {
                throw new InvalidMessageCacheException(
                      temporaryPath + " is not part of the " + _uploadDirectory + " tree.");
            }
        } else {
            if (!temporaryDirContainer.contains(_uploadDirectory)) {
                throw new InvalidMessageCacheException(
                      temporaryPath + " is not part of the " + _uploadDirectory + " tree.");
            }
        }
        if (temporaryDir.isRoot()) {
            throw new InvalidMessageCacheException(
                  "A temporary upload path cannot be in the root directory.");
        }
    }

    @Override
    public FileAttributes commitUpload(Subject subject, FsPath temporaryPath, FsPath finalPath,
          Set<CreateOption> options, Set<FileAttribute> attributesToFetch)
          throws CacheException {
        try {
            FsPath temporaryDir = getParentOfFile(temporaryPath);
            FsPath finalDir = getParentOfFile(finalPath);

            checkIsTemporaryDirectory(temporaryPath, temporaryDir);

            /* File must have been created...
             */
            ExtendedInode uploadDirInode;
            ExtendedInode temporaryDirInode;
            ExtendedInode inodeOfFile;
            try {
                uploadDirInode = new ExtendedInode(_fs,
                      _fs.path2inode(temporaryDir.parent().toString()));
                temporaryDirInode = uploadDirInode.inodeOf(temporaryDir.name(), STAT);
                inodeOfFile = temporaryDirInode.inodeOf(temporaryPath.name(), STAT);
            } catch (FileNotFoundChimeraFsException e) {
                throw new FileNotFoundCacheException("No such file or directory: " + temporaryPath,
                      e);
            }

            /* ...and upload must have completed...
             */
            ImmutableList<StorageLocatable> locations = inodeOfFile.getLocations();
            if (locations.isEmpty()) {
                throw new FileIsNewCacheException("Upload has not completed.");
            }

            /* ...and it must have the correct size.
             */
            ImmutableList<String> size = inodeOfFile.getTag(TAG_EXPECTED_SIZE);
            if (!size.isEmpty()) {
                long expectedSize = Long.parseLong(size.get(0));
                long actualSize = inodeOfFile.statCache().getSize();
                if (expectedSize != actualSize) {
                    throw new FileCorruptedCacheException(expectedSize, actualSize);
                }
            }

            /* Target directory must exist.
             */
            ExtendedInode finalDirInode;
            try {
                finalDirInode = new ExtendedInode(_fs, _fs.path2inode(finalDir.toString()));
            } catch (FileNotFoundChimeraFsException e) {
                throw new FileNotFoundCacheException("No such file or directory: " + finalDir, e);
            }

            /* File must not exist unless overwrite is enabled.
             */
            try {
                ExtendedInode inodeOfExistingFile = finalDirInode.inodeOf(finalPath.name(), STAT);
                if (!options.contains(CreateOption.OVERWRITE_EXISTING)) {
                    throw new FileExistsCacheException("File exists: " + finalPath);
                }
                /* User must be authorized to delete existing file.
                 */
                if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                    FileAttributes attributesOfParent =
                          getFileAttributesForPermissionHandler(finalDirInode);
                    FileAttributes attributesOfFile =
                          getFileAttributesForPermissionHandler(inodeOfExistingFile);
                    if (_permissionHandler.canDeleteFile(subject,
                          attributesOfParent,
                          attributesOfFile) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Overwrite denied: " + finalPath);
                    }
                }
            } catch (FileNotFoundChimeraFsException ignored) {
            }

            /* Read file attributes before moving the file. Otherwise the cached parent will
             * be gone.
             */
            FileAttributes attributes = getFileAttributes(inodeOfFile, attributesToFetch);

            /* File is moved to correct directory.
             */
            _fs.rename(inodeOfFile, temporaryDirInode, temporaryPath.name(), finalDirInode,
                  finalPath.name());

            /* Delete temporary upload directory and any files in it.
             */
            removeRecursively(uploadDirInode, temporaryDir.name(),
                  temporaryDirInode, i -> {
                  });

            return attributes;
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        } catch (NumberFormatException e) {
            throw new FileCorruptedCacheException("Failed to commit file: " + e.getMessage());
        }
    }

    @Override
    public Collection<FileAttributes> cancelUpload(Subject subject, FsPath temporaryPath,
          FsPath finalPath, Set<FileAttribute> requested, String explanation)
          throws CacheException {
        List<FileAttributes> deleted = new ArrayList();
        try {
            FsPath temporaryDir = getParentOfFile(temporaryPath);

            checkIsTemporaryDirectory(temporaryPath, temporaryDir);

            /* Temporary upload directory must exist.
             */
            ExtendedInode uploadDirInode;
            try {
                uploadDirInode = new ExtendedInode(_fs,
                      _fs.path2inode(temporaryDir.parent().toString()));
            } catch (FileNotFoundChimeraFsException e) {
                throw new FileNotFoundCacheException("No such file or directory: " + temporaryDir,
                      e);
            }

            /* Delete temporary upload directory and any files in it.
             */
            String name = temporaryPath.parent().name();
            removeRecursively(uploadDirInode, name, uploadDirInode.inodeOf(name, STAT),
                  i -> {
                      try {
                          if (i.getFileType() == FileType.REGULAR) {
                              deleted.add(getFileAttributes(i, requested));
                          }
                      } catch (CacheException | ChimeraFsException e) {
                          LOGGER.info("Unable to identify deleted file for upload cancellation: {}",
                                e.toString());
                      }
                  });
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
        return deleted;
    }

    @Override
    public void updateFsStat() throws CacheException {
        try {
            _fs.updateFsStat();
        } catch (ChimeraFsException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }
    }

    private void removeRecursively(ExtendedInode parent, String name, ExtendedInode inode,
          Consumer<ExtendedInode> deleted) throws ChimeraFsException, CacheException {
        try {
            if (inode.isDirectory() && inode.stat().getNlink() > 2) {
                try (DirectoryStreamB<ChimeraDirectoryEntry> list = _fs.newDirectoryStream(inode)) {
                    for (ChimeraDirectoryEntry entry : list) {
                        String child = entry.getName();
                        if (!child.equals(".") && !child.equals("..")) {
                            ExtendedInode childInode = new ExtendedInode(_fs, entry.getInode());
                            removeRecursively(inode, child, childInode, deleted);
                        }
                    }
                }
            }
            deleted.accept(inode);
            _fs.remove(parent, name, inode);
        } catch (ChimeraFsException e) {
            throw e;
        } catch (IOException e) {
            throw new ChimeraFsException("Failed to delete directory recursively: " + e);
        }
    }

    private FsPath getParentOfFile(FsPath path) throws NotFileCacheException {
        try {
            return path.parent();
        } catch (IllegalStateException e) {
            throw new NotFileCacheException("Not a file: " + path);
        }
    }

    public byte[] readExtendedAttribute(Subject subject, FsPath path, String name)
          throws CacheException {
        try {
            ExtendedInode target = pathToInode(subject, path.toString());

            if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                FileAttributes attributes = getFileAttributesForPermissionHandler(target);
                if (target.isDirectory()) {
                    if (_permissionHandler.canListDir(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied");
                    }
                } else {
                    if (_permissionHandler.canReadFile(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied");
                    }
                }
            }

            return _fs.getXattr(target, name);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file " + path);
        } catch (ChimeraFsException e) {
            throw new CacheException("Failed to list extended attributes: "
                  + Exceptions.messageOrClassName(e), e);
        }
    }

    public void writeExtendedAttribute(Subject subject, FsPath path, String name,
          byte[] value, SetExtendedAttributeMode mode) throws CacheException {
        try {
            ExtendedInode target = pathToInode(subject, path.toString());

            if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                FileAttributes attributes = getFileAttributesForPermissionHandler(target);
                if (target.isDirectory()) {
                    if (_permissionHandler.canCreateFile(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied");
                    }
                } else {
                    if (_permissionHandler.canWriteFile(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied");
                    }
                }
            }

            FileSystemProvider.SetXattrMode m;
            switch (mode) {
                case CREATE:
                    m = FileSystemProvider.SetXattrMode.CREATE;
                    break;
                case REPLACE:
                    m = FileSystemProvider.SetXattrMode.REPLACE;
                    break;
                case EITHER:
                    m = FileSystemProvider.SetXattrMode.EITHER;
                    break;
                default:
                    throw new RuntimeException();
            }

            _fs.setXattr(target, name, value, m);
        } catch (NoXdataChimeraException e) {
            throw new NoAttributeCacheException(e.getMessage(), e);
        } catch (FileExistsChimeraFsException e) {
            throw new AttributeExistsCacheException(e.getMessage(), e);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file " + path);
        } catch (ChimeraFsException e) {
            throw new CacheException("Failed to list extended attributes: "
                  + Exceptions.messageOrClassName(e), e);
        }
    }

    public Set<String> listExtendedAttributes(Subject subject, FsPath path)
          throws CacheException {
        try {
            ExtendedInode target = pathToInode(subject, path.toString());

            if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                FileAttributes attributes = getFileAttributesForPermissionHandler(target);
                if (target.isDirectory()) {
                    if (_permissionHandler.canListDir(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied");
                    }
                } else {
                    if (_permissionHandler.canReadFile(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied");
                    }
                }
            }

            return _fs.listXattrs(target);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file " + path);
        } catch (ChimeraFsException e) {
            throw new CacheException("Failed to list extended attributes: "
                  + Exceptions.messageOrClassName(e), e);
        }
    }

    public void removeExtendedAttribute(Subject subject, FsPath path, String name)
          throws CacheException {
        try {
            ExtendedInode target = pathToInode(subject, path.toString());

            if (!Subjects.isExemptFromNamespaceChecks(subject)) {
                FileAttributes attributes = getFileAttributesForPermissionHandler(target);
                if (target.isDirectory()) {
                    if (_permissionHandler.canCreateFile(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied");
                    }
                } else {
                    if (_permissionHandler.canWriteFile(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied");
                    }
                }
            }

            _fs.removeXattr(target, name);
        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file " + path);
        } catch (NoXdataChimeraException e) {
            throw new NoAttributeCacheException("No attribute " + name + " for file " + path,
                  e);
        } catch (ChimeraFsException e) {
            throw new CacheException("Failed to list extended attributes: "
                  + Exceptions.messageOrClassName(e), e);
        }
    }

    /**
     * @param subject The user making the request. Remove a label from a file.
     * @param path    The file from which the label  is deleted.
     * @param label   The name of the label to remove.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to remove the label.
     * @throws CacheException                 a generic failure in removing the labe.
     **/
    public void removeLabel(Subject subject, FsPath path, String label) throws CacheException {
        try {
            ExtendedInode target = pathToInode(subject, path.toString());

            if (!Subjects.isRoot(subject)) {

                FileAttributes attributes = getFileAttributesForPermissionHandler(target);

                if (target.isDirectory()) {

                    throw new NotFileCacheException("Directory object cannot have a label.");

                } else {
                    if (_permissionHandler.canWriteFile(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied");
                    }
                }
            }
            _fs.removeLabel(target, label);

        } catch (FileNotFoundChimeraFsException e) {
            throw new FileNotFoundCacheException("No such file " + path);
        } catch (ChimeraFsException e) {
            throw new CacheException("Failed to remove the label: "
                  + Exceptions.messageOrClassName(e), e);
        }

    }
}
