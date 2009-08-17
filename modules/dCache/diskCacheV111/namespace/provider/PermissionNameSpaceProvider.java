package diskCacheV111.namespace.provider;

import java.util.Set;
import java.util.List;
import java.io.File;
import java.io.FileFilter;
import javax.security.auth.Subject;

import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.namespace.NameSpaceProvider;
import org.dcache.util.Checksum;
import org.dcache.util.Glob;
import org.dcache.util.Interval;
import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;

/**
 *
 */
public class PermissionNameSpaceProvider
    extends AbstractNameSpaceProviderDecorator
{
    public PermissionNameSpaceProvider(NameSpaceProvider inner)
    {
        super(inner);
    }

    @Override
    public PnfsId createEntry(Subject subject, String path,
                              FileMetaData metaData, boolean isDirectory)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            File file = new File(path);
            File parent = file.getParentFile();

            PnfsId parentId =
                super.pathToPnfsid(subject, parent.toString(), true);

            FileMetaData meta = super.getFileMetaData(subject, parentId);
            if (!canWriteExecute(subject, meta)) {
                throw new PermissionDeniedCacheException(path);
            }
        }
        return super.createEntry(subject, path, metaData, isDirectory);
    }

    @Override
    public void deleteEntry(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            PnfsId parentId = super.getParentOf(subject, pnfsId);
            FileMetaData parentMeta =
                super.getFileMetaData(subject, parentId);
            FileMetaData fileMeta =
                super.getFileMetaData(subject, pnfsId);

            if (!canWriteExecute(subject, parentMeta) ||
                !canWrite(subject, fileMeta)) {
                throw new PermissionDeniedCacheException(pnfsId.toString());
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

            PnfsId fileId =
                super.pathToPnfsid(subject, file.toString(), false);
            PnfsId parentId =
                super.pathToPnfsid(subject, parent.toString(), true);

            FileMetaData fileMeta =
                super.getFileMetaData(subject, fileId);
            FileMetaData parentMeta =
                super.getFileMetaData(subject, parentId);

            if (!canWriteExecute(subject, parentMeta) ||
                !canWrite(subject, fileMeta)) {
                throw new PermissionDeniedCacheException(path);
            }
        }
        super.deleteEntry(subject, path);
    }

    @Override
    public void renameEntry(Subject subject, PnfsId pnfsId, String newName)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            PnfsId parentId = super.getParentOf(subject, pnfsId);

            File newFile = new File(newName);
            File newParent = newFile.getParentFile();
            PnfsId newParentId =
                super.pathToPnfsid(subject, newParent.toString(), true);

            FileMetaData parentMeta =
                super.getFileMetaData(subject, parentId);
            FileMetaData newParentMeta =
                super.getFileMetaData(subject, newParentId);

            if (!canWriteExecute(subject, parentMeta) ||
                !canWriteExecute(subject, newParentMeta)) {
                throw new PermissionDeniedCacheException(pnfsId.toString());
            }
        }

        super.renameEntry(subject, pnfsId, newName);
    }

    @Override
    public PnfsId pathToPnfsid(Subject subject, String path, boolean followLinks)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            File file = new File(path);
            File parent = file.getParentFile();

            PnfsId parentId =
                super.pathToPnfsid(subject, parent.toString(), true);

            FileMetaData meta = super.getFileMetaData(subject, parentId);
            if (!canExecute(subject, meta)) {
                throw new PermissionDeniedCacheException(path);
            }
        }

        return super.pathToPnfsid(subject, path, followLinks);
    }

    @Override
    public StorageInfo getStorageInfo(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            FileMetaData meta = super.getFileMetaData(subject, pnfsId);

            if (!canRead(subject, meta)) {
                throw new PermissionDeniedCacheException(pnfsId.toString());
            }
        }

        return super.getStorageInfo(subject, pnfsId);
    }

    @Override
    public void list(Subject subject, String path, Glob glob, Interval range,
                     Set<FileAttribute> attrs, ListHandler handler)
        throws CacheException
    {
        if (!Subjects.isRoot(subject)) {
            PnfsId pnfsId = super.pathToPnfsid(subject, path, true);
            FileMetaData meta = super.getFileMetaData(subject, pnfsId);

            if (!canRead(subject, meta)) {
                throw new PermissionDeniedCacheException(path);
            }
        }

        super.list(subject, path, glob, range, attrs, handler);
    }

    private boolean canWriteExecute(Subject subject, FileMetaData metadata)
    {
        if (Subjects.hasUid(subject, metadata.getUid())) {
            return
                metadata.getUserPermissions().canWrite() &&
                metadata.getUserPermissions().canExecute();
        }

        if (Subjects.hasGid(subject, metadata.getGid())) {
            return
                metadata.getGroupPermissions().canWrite() &&
                metadata.getGroupPermissions().canExecute();
        }

        return
            metadata.getWorldPermissions().canWrite() &&
            metadata.getWorldPermissions().canExecute();
    }

    private boolean canExecute(Subject subject, FileMetaData metadata)
    {
        if (Subjects.hasUid(subject, metadata.getUid())) {
            return metadata.getUserPermissions().canExecute();
        }

        if (Subjects.hasGid(subject, metadata.getGid())) {
            return metadata.getGroupPermissions().canExecute();
        }

        return metadata.getWorldPermissions().canExecute();
    }

    private boolean canWrite(Subject subject, FileMetaData metadata)
    {
        if (Subjects.hasUid(subject, metadata.getUid())) {
            return metadata.getUserPermissions().canWrite();
        }

        if (Subjects.hasGid(subject, metadata.getGid())) {
            return metadata.getGroupPermissions().canWrite();
        }

        return metadata.getWorldPermissions().canWrite();
    }

    private boolean canRead(Subject subject, FileMetaData metadata)
    {
        if (Subjects.hasUid(subject, metadata.getUid())) {
            return metadata.getUserPermissions().canRead();
        }

        if (Subjects.hasGid(subject, metadata.getGid())) {
            return metadata.getGroupPermissions().canRead();
        }

        return metadata.getWorldPermissions().canRead();
    }
}