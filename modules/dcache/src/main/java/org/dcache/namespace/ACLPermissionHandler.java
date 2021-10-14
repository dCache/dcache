package org.dcache.namespace;

import static org.dcache.acl.enums.AccessMask.ADD_FILE;
import static org.dcache.acl.enums.AccessMask.ADD_SUBDIRECTORY;
import static org.dcache.acl.enums.AccessMask.DELETE;
import static org.dcache.acl.enums.AccessMask.DELETE_CHILD;
import static org.dcache.acl.enums.AccessMask.EXECUTE;
import static org.dcache.acl.enums.AccessMask.LIST_DIRECTORY;
import static org.dcache.acl.enums.AccessMask.READ_ACL;
import static org.dcache.acl.enums.AccessMask.READ_ATTRIBUTES;
import static org.dcache.acl.enums.AccessMask.READ_DATA;
import static org.dcache.acl.enums.AccessMask.WRITE_ACL;
import static org.dcache.acl.enums.AccessMask.WRITE_ATTRIBUTES;
import static org.dcache.acl.enums.AccessMask.WRITE_DATA;
import static org.dcache.acl.enums.AccessType.ACCESS_ALLOWED;
import static org.dcache.acl.enums.AccessType.ACCESS_DENIED;
import static org.dcache.acl.enums.AccessType.ACCESS_UNDEFINED;
import static org.dcache.namespace.FileAttribute.ACL;
import static org.dcache.namespace.FileAttribute.OWNER;
import static org.dcache.namespace.FileAttribute.OWNER_GROUP;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.acl.ACL;
import org.dcache.acl.Owner;
import org.dcache.acl.Permission;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.mapper.AclMapper;
import org.dcache.acl.matcher.AclMatcher;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;

/**
 * A PermissionHandler using the ACL module as a PDP.
 */
public class ACLPermissionHandler implements PermissionHandler {

    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES =
          Collections.unmodifiableSet(EnumSet.of(ACL, OWNER, OWNER_GROUP));

    @Override
    public Set<FileAttribute> getRequiredAttributes() {
        return REQUIRED_ATTRIBUTES;
    }

    private Permission getPermission(Subject subject, FileAttributes attr) {
        ACL acl = attr.getAcl();
        Owner owner = new Owner(attr.getOwner(), attr.getGroup());
        Origin origin = Subjects.getOrigin(subject);
        return AclMapper.getPermission(subject, origin, owner, acl);
    }

    @Override
    public AccessType canReadFile(Subject subject, FileAttributes attr) {
        Permission permission = getPermission(subject, attr);
        return AclMatcher.isAllowed(permission, READ_DATA);
    }

    @Override
    public AccessType canWriteFile(Subject subject, FileAttributes attr) {
        Permission permission = getPermission(subject, attr);
        return AclMatcher.isAllowed(permission, WRITE_DATA);
    }

    @Override
    public AccessType canCreateSubDir(Subject subject, FileAttributes parentAttr) {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }
        Permission permission = getPermission(subject, parentAttr);
        return AclMatcher.isAllowed(permission, ADD_SUBDIRECTORY);
    }

    @Override
    public AccessType canCreateFile(Subject subject, FileAttributes parentAttr) {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }
        Permission permission = getPermission(subject, parentAttr);
        return AclMatcher.isAllowed(permission, ADD_FILE);
    }

    @Override
    public AccessType canDeleteFile(Subject subject,
          FileAttributes parentAttr,
          FileAttributes childAttr) {
        if (parentAttr == null) {
            return ACCESS_DENIED;
        }

        Permission permissionParent = getPermission(subject, parentAttr);
        AccessType ofParent = AclMatcher.isAllowed(permissionParent,
              DELETE_CHILD);
        if (ofParent == ACCESS_ALLOWED) {
            return ofParent;
        }

        Permission permissionChild = getPermission(subject, childAttr);
        AccessType ofChild = AclMatcher.isAllowed(permissionChild,
              DELETE);

        if (ofChild == ACCESS_ALLOWED) {
            return ofChild;
        }

        if (ofParent == ACCESS_DENIED
              || ofChild == ACCESS_DENIED) {
            return ACCESS_DENIED;
        }

        return ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canDeleteDir(Subject subject,
          FileAttributes parentAttr,
          FileAttributes childAttr) {
        return canDeleteFile(subject, parentAttr, childAttr);
    }

    @Override
    public AccessType canListDir(Subject subject, FileAttributes attr) {
        Permission permission = getPermission(subject, attr);
        return AclMatcher.isAllowed(permission,
              LIST_DIRECTORY);
    }

    @Override
    public AccessType canLookup(Subject subject, FileAttributes attr) {
        Permission permission = getPermission(subject, attr);
        return AclMatcher.isAllowed(permission,
              EXECUTE);
    }

    @Override
    public AccessType canRename(Subject subject,
          FileAttributes parentAttr,
          FileAttributes newParentAttr,
          boolean isDirectory) {
        if (parentAttr == null || newParentAttr == null) {
            return ACCESS_DENIED;
        }

        Permission permission1 = getPermission(subject, parentAttr);
        Permission permission2 = getPermission(subject, newParentAttr);

        AccessType ofSrcParent = AclMatcher.isAllowed(permission1, DELETE_CHILD);
        AccessType ofDestParent = AclMatcher.isAllowed(permission2, ADD_FILE);

        if (ofDestParent == ofSrcParent) {
            return ofSrcParent;
        }

        if (ofSrcParent == ACCESS_DENIED ||
              ofDestParent == ACCESS_DENIED) {
            return ACCESS_DENIED;
        }

        return ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canSetAttributes(Subject subject,
          FileAttributes currentAttributes,
          FileAttributes desiredAttributes) {
        Permission permission = getPermission(subject, currentAttributes);
        return canSetAttributes(permission, desiredAttributes);
    }

    @Override
    public AccessType canGetAttributes(Subject subject,
          FileAttributes attr,
          Set<FileAttribute> attributes) {
        Permission permission = getPermission(subject, attr);
        return canGetAttributes(permission, attributes);
    }

    /**
     * Determines if the action is allowed on the get of file attributes. Returns ACCESS_DENIED if
     * the action is denied for one or more of the attributes. Returns ACCESS_ALLOWED if the action
     * is allowed for all attributes. Returns ACCESS_UNDEFINED otherwise.
     */
    private AccessType canGetAttributes(Permission permission, Set<FileAttribute> attributes) {
        if (attributes.contains(ACL)) {
            return AclMatcher.isAllowed(permission, READ_ACL);
        }

        return AclMatcher.isAllowed(permission, READ_ATTRIBUTES);
    }

    /**
     * Determines if the action is allowed on the set of file attributes. Returns ACCESS_DENIED if
     * the action is denied for one or more of the attributes. Returns ACCESS_ALLOWED if the action
     * is allowed for all attributes. Returns ACCESS_UNDEFINED otherwise.
     */
    private AccessType canSetAttributes(Permission permission, FileAttributes desiredAttributes) {
        if (desiredAttributes.isDefined(ACL)) {
            return AclMatcher.isAllowed(permission, WRITE_ACL);
        }

        return AclMatcher.isAllowed(permission, WRITE_ATTRIBUTES);
    }
}
