package diskCacheV111.services.acl;

import org.dcache.acl.ACLException;
import org.dcache.acl.Origin;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.FileAttribute;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import javax.security.auth.Subject;

public class GrantAllPermissionHandler implements PermissionHandler
{
    public AccessType canReadFile(PnfsId pnfsId, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canReadFile(String pnfsPath, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canWriteFile(PnfsId pnfsId, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canWriteFile(String pnfsPath, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canCreateDir(PnfsId pnfsId, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canCreateDir(String pnfsPath, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canCreateFile(PnfsId pnfsId, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canCreateFile(String pnfsPath, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canDeleteFile(PnfsId pnfsId, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canDeleteFile(String pnfsPath, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canDeleteDir(PnfsId pnfsId, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canDeleteDir(String pnfsPath, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canListDir(PnfsId pnfsId, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canListDir(String pnfsPath, Subject subject, Origin origin)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canSetAttributes(PnfsId pnfsId, Subject subject, Origin userOrigin, FileAttribute attribute)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canSetAttributes(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public AccessType canGetAttributes(PnfsId pnfsId, Subject subject, Origin userOrigin, FileAttribute attribute)
    {
        return AccessType.ACCESS_ALLOWED;
    }

    public String toUnixACL(PnfsId pnfsId)
    {
        return "";
    }
}
