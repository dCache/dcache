package org.dcache.namespace;

import java.util.Set;
import java.util.List;
import java.util.EnumSet;
import java.util.Collections;
import java.util.Arrays;
import javax.security.auth.Subject;

import org.dcache.acl.enums.AccessType;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.acl.enums.AccessType.*;

import dmg.util.CollectionFactory;

/**
 * PermissionHandler which delegates calls to a chain of permission
 * handler. For each policy decision, the permission handler delegates
 * the call to each chained permission handler until the first one
 * which returns a result different from ACCESS_UNDEFINED. That result
 * will be returned. If all chained permission handlers return
 * ACCESS_UNDEFINED, then this permission handler also returns
 * ACCESS_UNDEFINED.
 */
public class ChainedPermissionHandler implements PermissionHandler
{
    private final List<PermissionHandler> _chain =
        CollectionFactory.newArrayList();

    public ChainedPermissionHandler(List<PermissionHandler> chain)
    {
        _chain.addAll(chain);
    }

    public ChainedPermissionHandler(PermissionHandler ... chain)
    {
        _chain.addAll(Arrays.asList(chain));
    }

    @Override
    public Set<FileAttribute> getRequiredAttributes()
    {
        Set<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
        for (PermissionHandler handler: _chain) {
            attributes.addAll(handler.getRequiredAttributes());
        }
        return attributes;
    }

    @Override
    public AccessType canReadFile(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canReadFile(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED)
                return res;
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canWriteFile(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canWriteFile(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED)
                return res;
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canCreateSubDir(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canCreateSubDir(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED)
                return res;
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canCreateFile(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canCreateFile(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED)
                return res;
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canDeleteFile(Subject subject,
                                    FileAttributes parentAttr,
                                    FileAttributes childAttr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canDeleteFile(subject, parentAttr,
                                                   childAttr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED)
                return res;
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canDeleteDir(Subject subject,
                                   FileAttributes parentAttr,
                                   FileAttributes childAttr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canDeleteDir(subject, parentAttr,
                                                  childAttr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED)
                return res;
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canRename(Subject subject,
                                FileAttributes existingParentAttr,
                                FileAttributes newParentAttr,
                                boolean isDirectory)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canRename(subject, existingParentAttr,
                                               newParentAttr, isDirectory);
            if (res != null && res != AccessType.ACCESS_UNDEFINED)
                return res;
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canListDir(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canListDir(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED)
                return res;
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canLookup(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canLookup(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED)
                return res;
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    private AccessType canSetAttribute(Subject subject,
                                       FileAttributes parentAttrs,
                                       FileAttributes attrs,
                                       FileAttribute attribute)
    {
        Set<FileAttribute> set = Collections.singleton(attribute);
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canSetAttributes(subject,
                                                      parentAttrs,
                                                      attrs,
                                                      set);
            switch (res) {
            case ACCESS_DENIED:
                return ACCESS_DENIED;
            case ACCESS_ALLOWED:
                return ACCESS_ALLOWED;
            case ACCESS_UNDEFINED:
                break;
            }
        }
        return ACCESS_UNDEFINED;
    }

    private AccessType canGetAttribute(Subject subject,
                                       FileAttributes parentAttrs,
                                       FileAttributes attrs,
                                       FileAttribute attribute)
    {
        Set<FileAttribute> set = Collections.singleton(attribute);
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canGetAttributes(subject,
                                                      parentAttrs,
                                                      attrs,
                                                      set);
            switch (res) {
            case ACCESS_DENIED:
                return ACCESS_DENIED;
            case ACCESS_ALLOWED:
                return ACCESS_ALLOWED;
            case ACCESS_UNDEFINED:
                break;
            }
        }
        return ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canSetAttributes(Subject subject,
                                       FileAttributes parentAttrs,
                                       FileAttributes attrs,
                                       Set<FileAttribute> attributes)
    {
        boolean allAllowed = true;
        for (FileAttribute attribute: attributes) {
            AccessType res =
                canSetAttribute(subject, parentAttrs, attrs, attribute);
            switch (res) {
            case ACCESS_DENIED:
                return ACCESS_DENIED;
            case ACCESS_UNDEFINED:
                allAllowed = false;
                break;
            case ACCESS_ALLOWED:
                break;
            }
        }
        return allAllowed ? ACCESS_ALLOWED : ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canGetAttributes(Subject subject,
                                       FileAttributes parentAttrs,
                                       FileAttributes attrs,
                                       Set<FileAttribute> attributes)
    {
        boolean allAllowed = true;
        for (FileAttribute attribute: attributes) {
            AccessType res =
                canGetAttribute(subject, parentAttrs, attrs, attribute);
            switch (res) {
            case ACCESS_DENIED:
                return ACCESS_DENIED;
            case ACCESS_UNDEFINED:
                allAllowed = false;
                break;
            case ACCESS_ALLOWED:
                break;
            }
        }
        return allAllowed ? ACCESS_ALLOWED : ACCESS_UNDEFINED;
    }
}