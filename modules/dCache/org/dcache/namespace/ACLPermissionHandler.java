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
import org.dcache.acl.Origin;
import org.dcache.acl.mapper.AclMapper;
import org.dcache.acl.matcher.AclNFSv4Matcher;
import org.dcache.acl.enums.Action;
import org.dcache.acl.enums.AccessType;
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

    public AccessType canReadFile(Subject subject, FileAttributes attr)
    {
        Permission permission = getPermission(subject, attr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission, READ));
    }

    public AccessType canWriteFile(Subject subject, FileAttributes attr)
    {
        Permission permission = getPermission(subject, attr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission, WRITE));
    }

    public AccessType canCreateSubDir(Subject subject, FileAttributes attr)
    {
        Permission permission = getPermission(subject, attr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission, CREATE, true));
    }

    public AccessType canCreateFile(Subject subject, FileAttributes attr)
    {
        Permission permission = getPermission(subject, attr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission, CREATE, false));
    }

    public AccessType canDeleteFile(Subject subject,
                                    FileAttributes parentAttr,
                                    FileAttributes childAttr)
    {
        Permission permissionParent = getPermission(subject, parentAttr);
        Permission permissionChild = getPermission(subject, childAttr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permissionParent,
                                                            permissionChild,
                                                            REMOVE,
                                                            false));
    }

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

    public AccessType canListDir(Subject subject, FileAttributes attr)
    {
        Permission permission = getPermission(subject, attr);
        return AccessType.valueOf(AclNFSv4Matcher.isAllowed(permission,
                                                            READDIR));
    }

    public AccessType canLookup(Subject subject, FileAttributes attr)
    {
        return AccessType.ACCESS_UNDEFINED;
    }

    public AccessType canRename(Subject subject,
                                FileAttributes parentAttr,
                                FileAttributes newParentAttr)
    {
        return AccessType.ACCESS_UNDEFINED;
    }

    public AccessType canSetAttributes(Subject subject,
                                       FileAttributes parentAttr,
                                       FileAttributes attr,
                                       Set<FileAttribute> attributes)
    {
        Permission permission = getPermission(subject, attr);
        return canSetGetAttributes(permission, attributes, SETATTR);
    }

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
                allowed =
                    AccessType.ACCESS_UNDEFINED;
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