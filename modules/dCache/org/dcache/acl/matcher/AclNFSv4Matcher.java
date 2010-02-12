package org.dcache.acl.matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.acl.Permission;
import org.dcache.acl.enums.AccessAttribute;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.Action;
import org.dcache.acl.enums.FileAttribute;
import org.dcache.acl.enums.OpenType;

/**
 * Component matches a access request and the access masks defMsk and allowMsk and returns true if
 * access is allowed.
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class AclNFSv4Matcher extends AclMatcher {

    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + AclNFSv4Matcher.class.getName());

    private static AclNFSv4Matcher _SINGLETON;
    static {
        _SINGLETON = new AclNFSv4Matcher();
    }

    private AclNFSv4Matcher() {
    }

    public static AclNFSv4Matcher instance() {
        return _SINGLETON;
    }

    public static void refresh() {
        _SINGLETON = new AclNFSv4Matcher();
    }

    /**
     * @param perm1
     *            If Action is:
     *            <ul>
     *            <li type=circle>RENAME, then <code>perm1</code> for a source directory
     *            <li type=circle>REMOVE, then <code>perm1</code> for a parent directory
     *            </ul>
     *
     * @param perm2
     *            If Action is:
     *            <ul>
     *            <li type=circle>RENAME, then <code>perm2</code> for destination directory
     *            <li type=circle>REMOVE, then <code>perm2</code> for child file/directory.
     *            </ul>
     *
     * @param action
     *            Action RENAME or REMOVE
     *
     * @param isDir
     *            used only for RENAME action: <code>TRUE</code> if action applied to directory,
     *            otherwise <code>FALSE</code>.
     * @return
     *            <li><code>TRUE</code> if access is allowed
     *            <li><code>FALSE</code> if access is denied
     *            <li><code>null</code> if there is no access right definition
     */
    public static Boolean isAllowed(Permission perm1, Permission perm2, Action action, Boolean isDir) {
        Boolean allowed;
        switch (action) {
        case RENAME:
            if ( isDir == null )
                throw new IllegalArgumentException("Argument isDir is NULL, action: " + action);

            allowed = isAllowed(perm1.getDefMsk(), perm1.getAllowMsk(), AccessMask.DELETE_CHILD.getValue());
            if ( allowed == null || allowed.equals( Boolean.TRUE ) ) {
                Boolean allowed_destination = isAllowed(perm2.getDefMsk(), perm2.getAllowMsk(), isDir ? AccessMask.ADD_SUBDIRECTORY.getValue() : AccessMask.ADD_FILE.getValue());
                if ( allowed_destination == null || allowed_destination.equals( Boolean.FALSE ) )
                    allowed = allowed_destination;
            }
            break;

        case REMOVE:
            allowed = isAllowed(perm1.getDefMsk(), perm1.getAllowMsk(), AccessMask.DELETE_CHILD.getValue());
            if ( allowed == null || allowed.equals( Boolean.TRUE ) ) {
                Boolean allowed_child = isAllowed(perm2.getDefMsk(), perm2.getAllowMsk(), AccessMask.DELETE.getValue());
                if ( allowed_child == null || allowed_child.equals( Boolean.FALSE ) )
                    allowed = allowed_child;
            }
            break;

        default:
            throw new IllegalArgumentException("Illegal usage, action: " + action);
        }

        if ( logger.isDebugEnabled() )
            logResult(action, null, isDir, allowed);

        return allowed;
    }

    /**
     * Only used by OPEN action
     *
     * @param perm
     *            Permission which contains defMask and allowMask
     *
     * @param opentype
     *            OpenType attribute
     *
     * @return
     * <li><code>TRUE</code> if access is allowed
     * <li><code>FALSE</code> if access is denied
     * <li><code>null</code> if there is no access right definition
     */
    public static Boolean isAllowed(Permission perm, Action action, OpenType opentype) {
        if ( action != Action.OPEN )
            throw new IllegalArgumentException("Illegal usage, action: " + action);

        if ( opentype == null )
            throw new IllegalArgumentException("opentype is NULL, action: " + Action.OPEN);

        Boolean allowed, isDir;
        int accessMask = 0;

        int defMask = perm.getDefMsk(), allowMask = perm.getAllowMsk();

        switch (opentype) {
        case OPEN4_CREATE:
            isDir = Boolean.TRUE;
            accessMask = AccessMask.ADD_FILE.getValue();
            break;

        case OPEN4_NOCREATE:
            isDir = Boolean.FALSE;
            allowed = isAllowed(defMask, allowMask, AccessMask.READ_DATA.getValue());
            if ( allowed != null ) {
                if ( logger.isDebugEnabled() )
                    logResult(action, opentype.toString(), isDir, allowed);
                return allowed;
            }
            accessMask = AccessMask.EXECUTE.getValue();
            break;

//            accessMask = AccessMask.WRITE_DATA.getValue(); // TODO: check specification for WRITE_DATA on OPEN.OPEN4_NONCREATE
//            if ( allowed == null ) {
//                accessMask |= AccessMask.EXECUTE.getValue();
//
//            } else if ( allowed == Boolean.FALSE ) {
//                if ( logger.isDebugEnabled() )
//                    logResult(action, opentype.toString(), isDir, allowed);
//                return allowed;
//            }
//            break;

        default:
            throw new IllegalArgumentException("Illegal open type: " + opentype);
        }

        allowed = isAllowed(defMask, allowMask, accessMask);
        if ( logger.isDebugEnabled() )
            logResult(action, opentype.toString(), isDir, allowed);

        return allowed;
    }

    /**
     * @param perm
     *            Permission which contains defMask and allowMask
     * @param action
     *            LINK / LOOKUPP / LOOKUP / READER / READLINK / WRITE
     * @return
     *            <li><code>TRUE</code> if access is allowed
     *            <li><code>FALSE</code> if access is denied
     *            <li><code>null</code> if there is no access right definition
     */
    public static Boolean isAllowed(Permission perm, Action action) {
        Boolean allowed;
        int accessMask;

        int defMask = perm.getDefMsk(), allowMask = perm.getAllowMsk();

        switch (action) {
        case LINK:
            accessMask = AccessMask.ADD_FILE.getValue();
            break;

        case LOOKUP: // not used ?
        case LOOKUPP:
            accessMask = AccessMask.EXECUTE.getValue();
            break;

        case READ:
            allowed = isAllowed(defMask, allowMask, AccessMask.READ_DATA.getValue());
            if ( allowed != null ) {
                if ( logger.isDebugEnabled() )
                    logResult(action, null, null, allowed);
                return allowed;
            }
            accessMask = AccessMask.EXECUTE.getValue();
            break;

        case READDIR:
            accessMask = AccessMask.LIST_DIRECTORY.getValue();
            break;

        case READLINK: // not used !
            accessMask = AccessMask.EXECUTE.getValue();
            break;

        case WRITE:
            accessMask = AccessMask.WRITE_DATA.getValue();
            break;

        default:
            throw new IllegalArgumentException("Illegal usage, action: " + action);
        }

        allowed = isAllowed(defMask, allowMask, accessMask);
        if ( logger.isDebugEnabled() )
            logResult(action, null, null, allowed);

        return allowed;
    }

    /**
     * @param perm
     *            Permission which contains defMask and allowMask
     *
     * @param action
     *            Action CREATE
     *
     * @param isDir
     *            <code>TRUE</code> if CREATE applied to directory, otherwise <code>FALSE</code>.
     *
     * @return
     * <li><code>TRUE</code> if access is allowed
     * <li><code>FALSE</code> if access is denied
     * <li><code>null</code> if there is no access right definition
     */
    public static Boolean isAllowed(Permission perm, Action action, Boolean isDir) throws IllegalArgumentException {
        Boolean allowed;
        int accessMask;
        int defMask = perm.getDefMsk(), allowMask = perm.getAllowMsk();

        switch (action) {
        case CREATE:
            accessMask = isDir ? AccessMask.ADD_SUBDIRECTORY.getValue() : AccessMask.ADD_FILE.getValue();
            break;

        default:
            throw new IllegalArgumentException("Illegal usage, action: " + action);
        }

        allowed = isAllowed(defMask, allowMask, accessMask);
        if ( logger.isDebugEnabled() )
            logResult(action, null, isDir, allowed);
        return allowed;
    }

    /**
     * Only used by ACCESS action
     *
     * @param defMask
     * @param allowMask
     * @param attribute
     *            Access Attribute
     * @param isDir
     *            <code>TRUE</code> if ACCESS action applied to directory, otherwise
     *            <code>FALSE</code>.
     *
     * @return
     * <li><code>TRUE</code> if access is allowed
     * <li><code>FALSE</code> if access is denied
     * <li><code>null</code> if there is no access right definition
     */
    public static Boolean isAllowed(int defMask, int allowMask, AccessAttribute attribute, Boolean isDir) {
        Boolean allowed = null;
        int accessMask = 0;

        switch (attribute) {

        case ACCESS4_READ:
            if ( isDir == null )
                throw new IllegalArgumentException("Argument isDir is NULL, attribute: " + attribute);

            if ( isDir == Boolean.FALSE ) {
                allowed = isAllowed(defMask, allowMask, AccessMask.READ_DATA.getValue());
                if ( allowed != null ) {
                    if ( logger.isDebugEnabled() )
                        logResult(Action.ACCESS, attribute.toString(), isDir, allowed);
                    return allowed;
                }
                accessMask = AccessMask.EXECUTE.getValue();

            } else
                accessMask = AccessMask.LIST_DIRECTORY.getValue();

            break;

        case ACCESS4_LOOKUP:
            if ( isDir == null )
                throw new IllegalArgumentException("Argument isDir is NULL, attribute: " + attribute);

            if ( isDir )
                accessMask = AccessMask.LIST_DIRECTORY.getValue();
            break;

        case ACCESS4_MODIFY:
            if ( isDir == null )
                throw new IllegalArgumentException("Argument isDir is NULL, attribute: " + attribute);

            if ( isDir )
                accessMask = AccessMask.ADD_SUBDIRECTORY.getValue();
            break;

        case ACCESS4_EXTEND:
            if ( isDir == null )
                throw new IllegalArgumentException("Argument isDir is NULL, attribute: " + attribute);

            accessMask = isDir ? AccessMask.ADD_FILE.getValue() : AccessMask.WRITE_DATA.getValue();
            break;

        case ACCESS4_DELETE:
            accessMask = AccessMask.DELETE.getValue();
            break;

        case ACCESS4_EXECUTE:
            if ( isDir == null )
                throw new IllegalArgumentException("Argument isDir is NULL, attribute: " + attribute);

            if ( isDir == Boolean.FALSE )
                accessMask = AccessMask.EXECUTE.getValue();
            break;

        default:
            throw new IllegalArgumentException("Unsupported access attribute: " + attribute);
        }

        if ( accessMask != 0 )
            allowed = isAllowed(defMask, allowMask, accessMask);

        if ( logger.isDebugEnabled() ) // TODO: switched temporary off
            logResult(Action.ACCESS, attribute.toString(), isDir, allowed);

        return allowed;
    }

    /**
     * @param perm
     *            Permission which contains defMask and allowMask
     * @param action
     *            Action GETATTR / SETATTR
     * @param attribute
     *            File mandatory attribute
     *
     * @return
     * <li><code>TRUE</code> if access is allowed
     * <li><code>FALSE</code> if access is denied
     * <li><code>null</code> if there is no access right definition
     */
    public static Boolean isAllowed(Permission perm, Action action, FileAttribute attribute) {
        Boolean allowed;
        int accessMask;

        int defMask = perm.getDefMsk(), allowMask = perm.getAllowMsk();

        switch (action) {
        case GETATTR:
            accessMask = (FileAttribute.FATTR4_ACL == attribute) ? AccessMask.READ_ACL.getValue() : AccessMask.READ_ATTRIBUTES.getValue();
            break;

        case SETATTR:
            switch (attribute) {
            case FATTR4_SIZE:
                accessMask = AccessMask.WRITE_DATA.getValue();
                break;

            case FATTR4_ACL:
            case FATTR4_MODE:
                accessMask = AccessMask.WRITE_ACL.getValue();
                break;

            case FATTR4_OWNER:
            case FATTR4_OWNER_GROUP:
                accessMask = AccessMask.WRITE_OWNER.getValue();
                break;

            case FATTR4_ARCHIVE:
            case FATTR4_HIDDEN:
            case FATTR4_MIMETYPE:
            case FATTR4_SYSTEM:
            case FATTR4_TIME_ACCESS_SET:
            case FATTR4_TIME_BACKUP:
            case FATTR4_TIME_CREATE:
            case FATTR4_TIME_MODIFY_SET:
            case FATTR4_SUPPORTED_ATTRS:
                accessMask = AccessMask.WRITE_ATTRIBUTES.getValue();
                break;

            default:
                throw new IllegalArgumentException("Unsupported file attribute: " + attribute);
            }
            break;

        default:
            throw new IllegalArgumentException("Usage failure. Invalid action: " + action);
        }

        allowed = isAllowed(defMask, allowMask, accessMask);
        if ( logger.isDebugEnabled() )
            logResult(action, attribute.toString(), null, allowed);

        return allowed;
    }

    private static void logResult(Action action, String attributes, Boolean isDir, Boolean allowed) {
        StringBuilder sb = new StringBuilder();
        sb.append(action);

        if ( isDir != null )
            sb.append(isDir ? ":DIR" : ":FILE");

        if ( attributes != null )
            sb.append(":").append(attributes);

        sb.append(" - ").append((allowed == null) ? UNDEFINED : (allowed ? ALLOWED : DENIED));

        logger.debug("AclNFSv4Matcher Results: " + sb.toString());
    }

}
