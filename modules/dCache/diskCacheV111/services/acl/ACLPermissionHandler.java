package diskCacheV111.services.acl;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.dcache.chimera.acl.ACL;
import org.dcache.chimera.acl.ACLException;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Owner;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.AccessType;
import org.dcache.chimera.acl.enums.Action;
import org.dcache.chimera.acl.enums.FileAttribute;
import org.dcache.chimera.acl.handler.singleton.AclHandler;
import org.dcache.chimera.acl.mapper.AclMapper;
import org.dcache.chimera.acl.mapper.AclUnixMapper;
import org.dcache.chimera.acl.matcher.AclNFSv4Matcher;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAdapter;
import dmg.util.Args;

/**
 * ACLPermissionHandler
 *
 * @author David Melkumyan, Irina Kozlova
 *
 */
public class ACLPermissionHandler extends AbstractPermissionHandler {

    private static final Logger _logger = Logger.getLogger("logger.org.dcache.authorization." + ACLPermissionHandler.class.getName());

    public ACLPermissionHandler(CellAdapter cell) throws ACLException {
        super(cell);
        if (AclHandler.getAclConfig() == null){
           Args args = cell.getArgs();
           Properties aclProperties = getAclProperties(args);

           AclHandler.setAclConfig(aclProperties);
        }
    }

    private  Properties getAclProperties(Args args) {
        Properties props = new Properties();
        getAclProperty("aclTable", props, args);
        getAclProperty("aclConnDriver", props, args);
        getAclProperty("aclConnUrl", props, args);
        getAclProperty("aclConnUser", props, args);
        getAclProperty("aclConnPswd", props, args);
        return props;
    }

    private void getAclProperty(String key, Properties props, Args args) {
            String value = args.getOpt(key);
            if (value != null) {
                props.setProperty(key, value);
            }
    }

    public String toUnixACL(PnfsId pnfsId) throws ACLException, CacheException {
        ACL acl = AclHandler.getACL(pnfsId.toString());
        return (acl == null)  ? null : AclUnixMapper.map(acl).toUnixString();
    }

    public AccessType canReadFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canReadWriteFile(pnfsId, subject, origin, false);
    }

    public AccessType canWriteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canReadWriteFile(pnfsId, subject, origin, true);
    }

    public AccessType canCreateDir(PnfsId pnfsParentId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canCreate(pnfsParentId, subject, origin, Boolean.TRUE);
    }

    public AccessType canCreateFile(PnfsId pnfsParentId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canCreate(pnfsParentId, subject, origin, Boolean.FALSE);
    }

    public AccessType canDeleteDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canDelete(pnfsId, subject, origin, Boolean.TRUE);
    }

    public AccessType canDeleteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canDelete(pnfsId, subject, origin, Boolean.FALSE);
    }

    public AccessType canListDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
        final String OPERATION = "ACLPermissionHandler.canListDir ";
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + args2String(pnfsId, subject, origin));

        final FileMetaData metadata = _metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("Directory Metadata: " + metadata.toString());

        if ( metadata.isDirectory() == false )
            throw new NotDirCacheException(pnfsId.toString());

        Boolean allowed = AclNFSv4Matcher.isAllowed(AclMapper.getPermission(subject, origin,
                new Owner(metadata.getUid(), metadata.getGid()), AclHandler.getACL(pnfsId.toString())), Action.READDIR);

        AccessType res = allowed == null ? AccessType.ACCESS_UNDEFINED : (allowed ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED);
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + res);

        return res;
    }

    public AccessType canGetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) throws CacheException, ACLException {
        return canGetSetAttributes(pnfsId, subject, origin, attribute, true);
    }

    public AccessType canSetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) throws CacheException, ACLException {
        return canGetSetAttributes(pnfsId, subject, origin, attribute, false);
    }

    // Low level checks
    // ///////////////////////////////////////////////////////////////////////////////

    private AccessType canReadWriteFile(PnfsId pnfsId, Subject subject, Origin origin, Boolean write) throws CacheException, ACLException {
        final String OPERATION = "ACLPermissionHandler.can" + (write ? "Write" : "Read") + "File ";
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + args2String(pnfsId, subject, origin));

        final FileMetaData metadata = _metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( metadata.isRegularFile() == false )
            throw new NotFileCacheException(pnfsId.toString());

        Boolean allowed = AclNFSv4Matcher.isAllowed(AclMapper.getPermission(subject, origin,
                new Owner(metadata.getUid(), metadata.getGid()), AclHandler.getACL(pnfsId.toString())),
                        (write ? Action.WRITE : Action.READ));

        AccessType res = allowed == null ? AccessType.ACCESS_UNDEFINED : (allowed ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED);
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + res);

        return res;
    }

    /**
    * @param pnfsParentId
    *            PNFS ID of parent directory
    * @param subject
    * @param origin
    * @param isDir
    *            TRUE if created directory, otherwise FALSE
    * @return AccessType
    * @throws CacheException
    * @throws ACLException
    */
    private AccessType canCreate(PnfsId pnfsParentId, Subject subject, Origin origin, Boolean isDir) throws CacheException, ACLException {
        final String OPERATION = "ACLPermissionHandler.canCreate" + (isDir ? "Dir " : "File ");
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + args2String(pnfsParentId, subject, origin));

        final FileMetaData metadataParent = _metadataSource.getMetaData(pnfsParentId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("Parent Metadata: " + metadataParent.toString());

        if ( metadataParent.isDirectory() == false )
            throw new NotDirCacheException(pnfsParentId.toString());

        Boolean allowed = AclNFSv4Matcher.isAllowed(AclMapper.getPermission(subject, origin, new Owner(metadataParent.getUid(), metadataParent.getGid()),
                AclHandler.getACL(pnfsParentId.toString())), Action.CREATE, isDir);

        AccessType res = allowed == null ? AccessType.ACCESS_UNDEFINED : (allowed ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED);
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + res);

        return res;
    }

    private AccessType canDelete(PnfsId pnfsId, Subject subject, Origin origin, Boolean isDir) throws CacheException, ACLException {
        final String OPERATION = "ACLPermissionHandler.canDelete" + (isDir ? "Dir " : "File ");
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + args2String(pnfsId, subject, origin));

        PnfsId pnfsParentId = getParentId(pnfsId);

        final FileMetaData metadataParent = _metadataSource.getMetaData(pnfsParentId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("Parent Metadata: " + metadataParent.toString());

        if ( metadataParent.isDirectory() == false )
            throw new NotDirCacheException(pnfsParentId.toString());

        final FileMetaData metadata = _metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( isDir ) {
            if ( metadata.isDirectory() == false )
                throw new NotDirCacheException(pnfsId.toString());

        } else if ( metadata.isRegularFile() == false )
            throw new NotFileCacheException(pnfsId.toString());

        Boolean allowed = AclNFSv4Matcher.isAllowed(AclMapper.getPermission(subject, origin, new Owner(metadataParent.getUid(), metadataParent.getGid()),
                AclHandler.getACL(pnfsParentId.toString())), AclMapper.getPermission(subject, origin, new Owner(metadata.getUid(), metadata.getGid()),
                AclHandler.getACL(pnfsId.toString())), Action.REMOVE, isDir);

        AccessType res = allowed == null ? AccessType.ACCESS_UNDEFINED : (allowed ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED);
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + res);

        return res;
    }

    private AccessType canGetSetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute, boolean get) throws CacheException,
            ACLException {
        final String OPERATION = "ACLPermissionHandler.can" + (get ? "Get" : "Set") + "Attributes ";
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + args2String(pnfsId, subject, origin, attribute));

        final FileMetaData metadata =_metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        Boolean allowed = AclNFSv4Matcher.isAllowed(AclMapper.getPermission(subject, origin,
                new Owner(metadata.getUid(), metadata.getGid()), AclHandler.getACL(pnfsId.toString())),
                        (get ? Action.GETATTR : Action.SETATTR), attribute);

        AccessType res = allowed == null ? AccessType.ACCESS_UNDEFINED : (allowed ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED);
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + res);

        return res;
    }

}
