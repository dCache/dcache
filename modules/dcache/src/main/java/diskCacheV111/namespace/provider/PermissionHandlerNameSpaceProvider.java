package diskCacheV111.namespace.provider;

import java.util.Set;
import java.util.List;
import java.util.Arrays;
import java.util.EnumSet;
import java.io.File;
import java.io.FileFilter;
import javax.security.auth.Subject;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.namespace.NameSpaceProvider;
import org.dcache.util.Checksum;
import org.dcache.util.Glob;
import org.dcache.util.Interval;
import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;
import org.dcache.namespace.PermissionHandler;
import org.dcache.vehicles.FileAttributes;
import static org.dcache.acl.enums.AccessType.*;
import static org.dcache.namespace.FileType.*;
import static org.dcache.namespace.FileAttribute.*;

import org.springframework.beans.factory.annotation.Required;

/**
 * A decorator for NameSpaceProvider which acts as a policy
 * enforcement point. A PermissionHandler is used as a policy decision
 * point.
 *
 * Only a subset of the methods of NameSpaceProvider are protected:
 *
 *    createEntry       maps to canCreateSubDir/canCreateFile
 *    deleteEntry       maps to canDeleteDir/canDeleteFile
 *    renameEntry       maps to canRename
 *    pathToPnfsid      maps to canLookup
 *    getFileAttributes maps to canGetAttributes
 *    list              maps to canListDir
 */
public class PermissionHandlerNameSpaceProvider
    extends AbstractNameSpaceProviderDecorator
{
    private PermissionHandler _handler;

    public PermissionHandlerNameSpaceProvider(NameSpaceProvider inner)
    {
        super(inner);
    }

    @Required
    public void setPermissionHandler(PermissionHandler handler)
    {
        _handler = handler;
    }

    private FileAttributes getFileAttributesForPermissionHandler(PnfsId id)
        throws CacheException
    {
        return _inner.getFileAttributes(Subjects.ROOT, id,
                                        _handler.getRequiredAttributes());
    }

    private FileAttributes
        getFileAttributesForPermissionHandler(PnfsId id, FileAttribute ... extra)
        throws CacheException
    {
        Set<FileAttribute> attr = EnumSet.noneOf(FileAttribute.class);
        attr.addAll(_handler.getRequiredAttributes());
        attr.addAll(Arrays.asList(extra));
        return _inner.getFileAttributes(Subjects.ROOT, id, attr);
    }

    @Override
    public PnfsId createEntry(Subject subject, String path,
                              int uid, int gid, int mode, boolean isDirectory)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            File file = new File(path);
            File parent = file.getParentFile();

            if (parent != null) {
                PnfsId parentId =
                    pathToPnfsid(subject, parent.toString(), true);
                FileAttributes attributes =
                    getFileAttributesForPermissionHandler(parentId);
                if (isDirectory) {
                    if (_handler.canCreateSubDir(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + path);
                    }
                } else {
                    if (_handler.canCreateFile(subject, attributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + path);
                    }
                }
            }
        }
        return super.createEntry(subject, path, uid, gid, mode, isDirectory);
    }

    @Override
    public void deleteEntry(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            PnfsId parentId = super.getParentOf(subject, pnfsId);
            FileAttributes parentAttributes =
                getFileAttributesForPermissionHandler(parentId);
            FileAttributes fileAttributes =
                getFileAttributesForPermissionHandler(pnfsId, TYPE);

            if (fileAttributes.getFileType() == DIR) {
                if (_handler.canDeleteDir(subject,
                                          parentAttributes,
                                          fileAttributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + pnfsId.toString());
                }
            } else {
                if (_handler.canDeleteFile(subject,
                                           parentAttributes,
                                           fileAttributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + pnfsId.toString());
                }
            }
        }
        super.deleteEntry(subject, pnfsId);
    }

    @Override
    public void deleteEntry(Subject subject, String path)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            File file = new File(path);
            File parent = file.getParentFile();

            if (parent != null) {
                PnfsId fileId =
                    pathToPnfsid(subject, file.toString(), false);
                PnfsId parentId =
                    super.pathToPnfsid(subject, parent.toString(), true);

                FileAttributes parentAttributes =
                    getFileAttributesForPermissionHandler(parentId);
                FileAttributes fileAttributes =
                    getFileAttributesForPermissionHandler(fileId, TYPE);

                if (fileAttributes.getFileType() == DIR) {
                    if (_handler.canDeleteDir(subject,
                                              parentAttributes,
                                              fileAttributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + path);
                    }
                } else {
                    if (_handler.canDeleteFile(subject,
                                               parentAttributes,
                                               fileAttributes) != ACCESS_ALLOWED) {
                        throw new PermissionDeniedCacheException("Access denied: " + path);
                    }
                }
            }
        }
        super.deleteEntry(subject, path);
    }

    @Override
    public void renameEntry(Subject subject, PnfsId pnfsId,
                            String newName, boolean overwrite)
        throws CacheException
    {
        if (Subjects.isRoot(subject)) {
            super.renameEntry(subject, pnfsId, newName, overwrite);
            return;
        }

        /* We must perform permission checking for non-root subjects.
         * For rename, this is complicated by the fact that the
         * destination name may exist and we may be requested to
         * overwrite it. Hence we need to check both rename and delete
         * permissions, however the delete permissions must only be
         * checked if the target actually exists.
         *
         * We assume that the common case is that the target does not
         * exist. Hence our strategy is to try the rename without
         * overwriting, and if that fails, then we check the delete
         * permissions on the destination name.
         */
        PnfsId parentId = super.getParentOf(subject, pnfsId);

        File newFile = new File(newName);
        File newParent = newFile.getParentFile();
        PnfsId newParentId;
        try {
            newParentId =
                pathToPnfsid(subject, newParent.toString(), true);
        } catch (FileNotFoundCacheException e) {
            throw new NotDirCacheException("No such directory: " +
                                           newParent.toString());
        }

        FileAttributes parentAttributes =
            getFileAttributesForPermissionHandler(parentId);
        FileAttributes newParentAttributes =
            getFileAttributesForPermissionHandler(newParentId);

        FileAttributes fileAttributes =
            _inner.getFileAttributes(Subjects.ROOT, pnfsId, EnumSet.of(TYPE));
        boolean isDir = (fileAttributes.getFileType() == DIR);

        if (_handler.canRename(subject,
                               parentAttributes,
                               newParentAttributes,
                               isDir) != ACCESS_ALLOWED) {
            throw new PermissionDeniedCacheException("Access denied: " +
                                                     pnfsId.toString());
        }

        try {
            super.renameEntry(subject, pnfsId, newName, false);
        } catch (FileExistsCacheException e) {
            if (!overwrite) {
                throw e;
            }

            /* Destination name exists and we were requested to
             * overwrite it.  Thus the subject must have delete
             * permission for the destination name.
             */
            PnfsId destId =
                pathToPnfsid(subject, newName, false);
            FileAttributes destAttributes =
                getFileAttributesForPermissionHandler(destId, TYPE);
            if (destAttributes.getFileType() == DIR) {
                if (_handler.canDeleteDir(subject,
                                          newParentAttributes,
                                          destAttributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " +
                                                             newName);
                }
            } else {
                if (_handler.canDeleteFile(subject,
                                           newParentAttributes,
                                           destAttributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " +
                                                             newName);
                }
            }

            /* Now we know the subject has delete permissions for
             * the destination name, so let's try to rename with
             * overwrite enabled.
             */
            super.renameEntry(subject, pnfsId, newName, true);
        }
    }

    @Override
    public PnfsId pathToPnfsid(Subject subject, String path, boolean followLinks)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            File file = new File(path);
            File parent = file.getParentFile();

            if (parent != null) {
                PnfsId parentId =
                    super.pathToPnfsid(subject, parent.toString(), true);

                FileAttributes attributes =
                    getFileAttributesForPermissionHandler(parentId);
                if (_handler.canLookup(subject, attributes) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + path);
                }
            }
        }

        return super.pathToPnfsid(subject, path, followLinks);
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
                                            Set<FileAttribute> attr)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            Set<FileAttribute> required = EnumSet.noneOf(FileAttribute.class);
            required.addAll(_handler.getRequiredAttributes());
            required.addAll(attr);
            FileAttributes fileAttributes =
                super.getFileAttributes(subject, pnfsId, required);

            /* The permission check is performed after we fetched the
             * attributes to avoid fetching the attributes twice.
             */
            try {
                PnfsId parentId = _inner.getParentOf(subject, pnfsId);

                FileAttributes parent =
                    getFileAttributesForPermissionHandler(parentId);
                if (_handler.canGetAttributes(subject, parent, fileAttributes, attr) != ACCESS_ALLOWED) {
                    throw new PermissionDeniedCacheException("Access denied: " + pnfsId.toString());
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
        }

        return super.getFileAttributes(subject, pnfsId, attr);
    }

    @Override
    public void setFileAttributes(Subject subject, PnfsId pnfsId,
                                  FileAttributes attr)
            throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            PnfsId parentId = super.getParentOf(subject, pnfsId);
            FileAttributes parentAttributes =
                getFileAttributesForPermissionHandler(parentId);
            FileAttributes attributes =
                getFileAttributesForPermissionHandler(pnfsId);

            if (_handler.canSetAttributes(subject, parentAttributes, attributes,
                                          attr.getDefinedAttributes()) != ACCESS_ALLOWED) {
                throw new PermissionDeniedCacheException("Access denied: " +
                                                         pnfsId.toString());
            }
        }

        super.setFileAttributes(subject, pnfsId, attr);
    }

    @Override
    public void list(Subject subject, String path, Glob glob, Interval range,
                     Set<FileAttribute> attrs, ListHandler handler)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            PnfsId pnfsId = pathToPnfsid(subject, path, true);
            FileAttributes attributes =
                getFileAttributesForPermissionHandler(pnfsId, TYPE);
            if (attributes.getFileType() != DIR) {
                throw new NotDirCacheException("Not a directory");
            } else if (_handler.canListDir(subject, attributes) != ACCESS_ALLOWED) {
                throw new PermissionDeniedCacheException("Access denied: " +
                                                         path);
            }
        }
        super.list(subject, path, glob, range, attrs, handler);
    }
}