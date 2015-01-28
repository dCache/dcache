package org.dcache.namespace;

import javax.security.auth.Subject;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import org.dcache.acl.enums.AccessType;
import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.acl.enums.AccessType.ACCESS_ALLOWED;
import static org.dcache.acl.enums.AccessType.ACCESS_DENIED;
import static org.dcache.chimera.UnixPermission.*;
import static org.dcache.namespace.FileAttribute.*;

/**
 * A PermissionHandler implementing the POSIX.1 permission model.
 *
 * Notice that there is no concept of a ROOT owner in this
 * PermissionHandler. That is, ROOT is just a regular user.
 */
public class PosixPermissionHandler implements PermissionHandler
{
    private Set<FileAttribute> _requiredAttributes =
        Collections.unmodifiableSet(EnumSet.of(OWNER, OWNER_GROUP, MODE));

    @Override
    public Set<FileAttribute> getRequiredAttributes()
    {
        return _requiredAttributes;
    }

    private boolean isSet(int mode, int flag)
    {
        return (mode & flag) == flag;
    }

    @Override
    public AccessType canReadFile(Subject subject, FileAttributes attr)
    {
        int mode = attr.getMode();
        if (Subjects.hasUid(subject, attr.getOwner())) {
            return AccessType.valueOf(isSet(mode, S_IRUSR));
        }

        if (Subjects.hasGid(subject, attr.getGroup())) {
            return AccessType.valueOf(isSet(mode, S_IRGRP));
        }

        return AccessType.valueOf(isSet(mode, S_IROTH));
    }

    @Override
    public AccessType canWriteFile(Subject subject, FileAttributes attr)
    {
        int mode = attr.getMode();
        if (Subjects.hasUid(subject, attr.getOwner())) {
            return AccessType.valueOf(isSet(mode, S_IWUSR));
        }

        if (Subjects.hasGid(subject, attr.getGroup())) {
            return AccessType.valueOf(isSet(mode, S_IWGRP));
        }

        return AccessType.valueOf(isSet(mode, S_IWOTH));
    }

    @Override
    public AccessType canCreateSubDir(Subject subject, FileAttributes parentAttr)
    {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }
        int mode = parentAttr.getMode();
        if (Subjects.hasUid(subject, parentAttr.getOwner())) {
            return AccessType.valueOf(isSet(mode, S_IWUSR | S_IXUSR));
        }

        if (Subjects.hasGid(subject, parentAttr.getGroup())) {
            return AccessType.valueOf(isSet(mode, S_IWGRP | S_IXGRP));
        }

        return AccessType.valueOf(isSet(mode, S_IWOTH | S_IXOTH));
    }

    @Override
    public AccessType canCreateFile(Subject subject, FileAttributes parentAttr)
    {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }
        int mode = parentAttr.getMode();
        if (Subjects.hasUid(subject, parentAttr.getOwner())) {
            return AccessType.valueOf(isSet(mode, S_IWUSR | S_IXUSR));
        }

        if (Subjects.hasGid(subject, parentAttr.getGroup())) {
            return AccessType.valueOf(isSet(mode, S_IWGRP | S_IXGRP));
        }

        return AccessType.valueOf(isSet(mode, S_IWOTH | S_IXOTH));
    }

    @Override
    public AccessType canDeleteFile(Subject subject,
                                    FileAttributes parentAttr,
                                    FileAttributes childAttr)
    {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }
        int mode = parentAttr.getMode();

        if (Subjects.hasUid(subject, parentAttr.getOwner())) {
            return AccessType.valueOf(isSet(mode, S_IWUSR | S_IXUSR));
        }

        if (Subjects.hasGid(subject, parentAttr.getGroup())) {
            return AccessType.valueOf(isSet(mode, S_IWGRP | S_IXGRP));
        }

        return AccessType.valueOf(isSet(mode, S_IWOTH | S_IXOTH));
    }

    @Override
    public AccessType canDeleteDir(Subject subject,
                                   FileAttributes parentAttr,
                                   FileAttributes childAttr)
    {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }
        int mode = parentAttr.getMode();

        if (Subjects.hasUid(subject, parentAttr.getOwner())) {
            return AccessType.valueOf(isSet(mode, S_IWUSR | S_IXUSR));
        }

        if (Subjects.hasGid(subject, parentAttr.getGroup())) {
            return AccessType.valueOf(isSet(mode, S_IWGRP | S_IXGRP));
        }

        return AccessType.valueOf(isSet(mode, S_IWOTH | S_IXOTH));
    }

    @Override
    public AccessType canListDir(Subject subject, FileAttributes attr)
    {
        int mode = attr.getMode();

        if (Subjects.hasUid(subject, attr.getOwner())) {
            return AccessType.valueOf(isSet(mode, S_IRUSR | S_IXUSR));
        }

        if (Subjects.hasGid(subject, attr.getGroup())) {
            return AccessType.valueOf(isSet(mode, S_IRGRP | S_IXGRP));
        }

        return AccessType.valueOf(isSet(mode, S_IROTH | S_IXOTH));
    }

    @Override
    public AccessType canLookup(Subject subject, FileAttributes attr)
    {
        int mode = attr.getMode();

        if (Subjects.hasUid(subject, attr.getOwner())) {
            return AccessType.valueOf(isSet(mode, S_IXUSR));
        }

        if (Subjects.hasGid(subject, attr.getGroup())) {
            return AccessType.valueOf(isSet(mode, S_IXGRP));
        }

        return AccessType.valueOf(isSet(mode, S_IXOTH));
    }

    @Override
    public AccessType canRename(Subject subject,
                                FileAttributes parentAttr,
                                FileAttributes newParentAttr,
                                boolean isDirectory)
    {
        if (parentAttr == null || newParentAttr == null) {
            return ACCESS_DENIED;
        }

        int parentMode = parentAttr.getMode();
        int newParentMode = newParentAttr.getMode();
        AccessType result;

        if (Subjects.hasUid(subject, parentAttr.getOwner())) {
            result = AccessType.valueOf(isSet(parentMode, S_IWUSR | S_IXUSR));
        } else if (Subjects.hasGid(subject, parentAttr.getGroup())) {
            result = AccessType.valueOf(isSet(parentMode, S_IWGRP | S_IXGRP));
        } else {
            result = AccessType.valueOf(isSet(parentMode, S_IWOTH | S_IXOTH));
        }

        if (result != ACCESS_ALLOWED) {
            return ACCESS_DENIED;
        }

        if (Subjects.hasUid(subject, newParentAttr.getOwner())) {
            return AccessType.valueOf(isSet(newParentMode, S_IWUSR | S_IXUSR));
        } else if (Subjects.hasGid(subject, newParentAttr.getGroup())) {
            return AccessType.valueOf(isSet(newParentMode, S_IWGRP | S_IXGRP));
        }
        return AccessType.valueOf(isSet(newParentMode, S_IWOTH | S_IXOTH));
    }

    @Override
    public AccessType canSetAttributes(Subject subject,
                                       FileAttributes attr,
                                       Set<FileAttribute> attributes)
    {
        /* Some flags can only be changed by the owner of the file.
        */
        if (attributes.contains(OWNER) ||
            attributes.contains(OWNER_GROUP) ||
            attributes.contains(MODE) ||
            attributes.contains(PERMISSION) ||
            attributes.contains(ACL)    ) {

            if (!Subjects.hasUid(subject, attr.getOwner())) {
                return AccessType.ACCESS_DENIED;
            }
        }

        /* Other flags can be changed by whoever got write permission.
         */
        int mode = attr.getMode();
        if (Subjects.hasUid(subject, attr.getOwner())) {
            // posix allows owner of file to set any attribute.
            return AccessType.ACCESS_ALLOWED;
        }

        if (Subjects.hasGid(subject, attr.getGroup())) {
            return AccessType.valueOf(isSet(mode, S_IWGRP));
        }

        return AccessType.valueOf(isSet(mode, S_IWOTH));
    }

    @Override
    public AccessType canGetAttributes(Subject subject,
                                       FileAttributes attr,
                                       Set<FileAttribute> attributes)
    {
        // posix always allowes to read attributes
        return ACCESS_ALLOWED;
    }
}
