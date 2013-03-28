package org.dcache.namespace;

import java.util.EnumSet;
import java.util.Set;
import java.util.Collections;
import javax.security.auth.Subject;

import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;

import org.dcache.acl.ACL;
import org.dcache.acl.Permission;
import org.dcache.acl.Owner;
import org.dcache.acl.mapper.AclMapper;
import org.dcache.acl.matcher.AclNFSv4Matcher;
import org.dcache.acl.enums.Action;
import org.dcache.acl.enums.AccessType;
import org.dcache.auth.Origin;
import static org.dcache.acl.enums.Action.*;
import static org.dcache.acl.enums.AccessType.*;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.chimera.UnixPermission.*;

/**
 * A PermissionHandler using the ACL module as a PDP.
 */
public class ACLPermissionHandler implements PermissionHandler
{
    private Set<FileAttribute> _requiredAttributes =
        Collections.unmodifiableSet(EnumSet.of(ACL, OWNER, OWNER_GROUP));

    @Override
    public Set<FileAttribute> getRequiredAttributes()
    {
        return _requiredAttributes;
    }

    private Permission getPermission(Subject subject, FileAttributes attr)
    {
        ACL acl = attr.getAcl();
        Owner owner = new Owner(attr.getOwner(), attr.getGroup());
        Origin origin = Subjects.getOrigin(subject);
        return AclMapper.getPermission(subject, origin, owner, acl);
    }

    @Override
    public AccessType canReadFile(Subject subject, FileAttributes attr)
    {
        Permission permission = getPermission(subject, attr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission, READ));
    }

    @Override
    public AccessType canWriteFile(Subject subject, FileAttributes attr)
    {
        Permission permission = getPermission(subject, attr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission, WRITE));
    }

    @Override
    public AccessType canCreateSubDir(Subject subject, FileAttributes parentAttr)
    {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }
        Permission permission = getPermission(subject, parentAttr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission, CREATE, true));
    }

    @Override
    public AccessType canCreateFile(Subject subject, FileAttributes parentAttr)
    {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }
        Permission permission = getPermission(subject, parentAttr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission, CREATE, false));
    }

    @Override
    public AccessType canDeleteFile(Subject subject,
                                    FileAttributes parentAttr,
                                    FileAttributes childAttr)
    {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }

        Permission permissionParent = getPermission(subject, parentAttr);
        Permission permissionChild = getPermission(subject, childAttr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permissionParent,
                                                            permissionChild,
                                                            REMOVE,
                                                            false));
    }

    @Override
    public AccessType canDeleteDir(Subject subject,
                                   FileAttributes parentAttr,
                                   FileAttributes childAttr)
    {
        Permission permissionParent = getPermission(subject, parentAttr);
        Permission permissionChild = getPermission(subject, childAttr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permissionParent,
                                                            permissionChild,
                                                            REMOVE,
                                                            true));
    }

    @Override
    public AccessType canListDir(Subject subject, FileAttributes attr)
    {
        Permission permission = getPermission(subject, attr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission,
                                                            READDIR));
    }

    @Override
    public AccessType canLookup(Subject subject, FileAttributes attr)
    {
        Permission permission = getPermission(subject, attr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission,
                                                            LOOKUP));
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

        Permission permission1 = getPermission(subject, parentAttr);
        Permission permission2 = getPermission(subject, newParentAttr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission1,
                                                            permission2,
                                                            RENAME,
                                                            isDirectory));
    }

    @Override
    public AccessType canSetAttributes(Subject subject,
                                       FileAttributes parentAttr,
                                       FileAttributes attr,
                                       Set<FileAttribute> attributes)
    {
        Permission permission = getPermission(subject, attr);
        return canSetGetAttributes(permission, attributes, SETATTR);
    }

    @Override
    public AccessType canGetAttributes(Subject subject,
                                       FileAttributes parentAttr,
                                       FileAttributes attr,
                                       Set<FileAttribute> attributes)
    {
        Permission permission = getPermission(subject, attr);
        return canSetGetAttributes(permission, attributes, GETATTR);
    }

    /**
     * Determines if the action is allowed on the set of file
     * attributes. Returns ACCESS_DENIED if the action is denied for
     * one or more of the attributes. Returns ACCESS_ALLOWED if the
     * action is allowed for all attributes. Returns ACCESS_UNDEFINED
     * otherwise.
     */
    private AccessType canSetGetAttributes(Permission permission,
                                           Set<FileAttribute> attributes,
                                           Action action)
    {
        boolean allAllowed = true;
        for (FileAttribute a: attributes) {
            org.dcache.acl.enums.FileAttribute nfs4 = a.toNfs4Attribute();
            AccessType allowed;
            if (nfs4 == null) {
                /* REVISIT: Temporary workaround to resolve a
                 * regression in 1.9.6 and 1.9.7. The problem is that
                 * not all dCache attributes have obvious mappings to
                 * NFS4 attributes. Thus with the current Matcher API,
                 * we cannot check whether we would be allowed to read
                 * those attributes.
                 */
//                 allowed =
//                     AccessType.ACCESS_UNDEFINED;
                allowed =
                    AccessType.ACCESS_ALLOWED;
            } else {
                allowed =
                    AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission,
                                                                 action,
                                                                 nfs4));
            }
            switch (allowed) {
            case ACCESS_DENIED:
                return AccessType.ACCESS_DENIED;
            case ACCESS_UNDEFINED:
                allAllowed = false;
                break;
            case ACCESS_ALLOWED:
                break;
            }
        }
        return allAllowed ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_UNDEFINED;
    }
}