package diskCacheV111.services.acl;

import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.acl.ACL;
import org.dcache.acl.ACLException;
import org.dcache.acl.Owner;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.Action;
import org.dcache.acl.enums.FileAttribute;
import org.dcache.acl.handler.singleton.AclHandler;
import org.dcache.acl.mapper.AclMapper;
import org.dcache.acl.mapper.AclUnixMapper;
import org.dcache.acl.matcher.AclNFSv4Matcher;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellEndpoint;
import dmg.util.Args;

import javax.security.auth.Subject;
import org.dcache.auth.Origin;

/**
 * ACLPermissionHandler
 *
 * @author David Melkumyan, Irina Kozlova
 *
 */
public class ACLPermissionHandler extends AbstractPermissionHandler {

    private static final Logger _logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + ACLPermissionHandler.class.getName());

    public ACLPermissionHandler(CellEndpoint cell) throws ACLException {
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

    public AccessType canReadFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
        PnfsId pnfsId = _pnfsHandler.getPnfsIdByPath(pnfsPath);
        return canReadWriteFile(pnfsId, subject, origin, false);
    }

    public AccessType canWriteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canReadWriteFile(pnfsId, subject, origin, true);
    }

    public AccessType canWriteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
        PnfsId pnfsId = _pnfsHandler.getPnfsIdByPath(pnfsPath);
        return canReadWriteFile(pnfsId, subject, origin, true);
    }

   /**
    * checks whether the user can create a sub-directory
    * in this directory (given by its pnfsId: pnfsParentId)
    */
    public AccessType canCreateDir(PnfsId pnfsParentId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canCreate(pnfsParentId, subject, origin, Boolean.TRUE);
    }

   /**
    * checks whether the user can create a sub-directory
    * in this directory (given by its pnfsPath, like /pnfs/sample.com/data/directory)
    */
    public AccessType canCreateDir(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
        PnfsId pnfsId = _pnfsHandler.getPnfsIdByPath(pnfsPath);
        return canCreate(pnfsId, subject, origin, Boolean.TRUE);
    }

   /**
    * checks whether the user can create a file
    * in this directory (given by its pnfsId: pnfsParentId)
    */
    public AccessType canCreateFile(PnfsId pnfsParentId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canCreate(pnfsParentId, subject, origin, Boolean.FALSE);
    }

    /**
    * checks whether the user can create a file
    * in this directory (given by its pnfsPath, like /pnfs/sample.com/data/directory)
    */
    public AccessType canCreateFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
        PnfsId pnfsId = _pnfsHandler.getPnfsIdByPath(pnfsPath);
        return canCreate(pnfsId, subject, origin, Boolean.FALSE);
    }

    /**
    * checks whether the user can delete directory given by its pnfsId
    */
    public AccessType canDeleteDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canDelete(pnfsId, subject, origin, Boolean.TRUE);
    }

    /**
     * checks whether the user can delete directory given by its pnfsPath
     */
     public AccessType canDeleteDir(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
        return canDelete(pnfsPath, subject, origin, Boolean.TRUE);
     }

    /**
    * checks whether the user can delete file given by its pnfsId
    */
    public AccessType canDeleteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
        return canDelete(pnfsId, subject, origin, Boolean.FALSE);
    }

    /**
    * checks whether the user can delete this file
    * given by its pnfsPath (like /pnfs/sample.com/data/file)
    */
    public AccessType canDeleteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
        return canDelete(pnfsPath, subject, origin, Boolean.FALSE);
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

    /**
    * checks whether the user can list this directory (given by its pnfsPath,
    * like /pnfs/sample.com/data/directory)
    */
    public AccessType canListDir(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
        final String OPERATION = "ACLPermissionHandler.canListDir ";
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + args2String(pnfsPath, subject, origin));

        final FileMetaData metadata = _metadataSource.getMetaData(pnfsPath);
        if ( _logger.isDebugEnabled() )
            _logger.debug("Directory Metadata: " + metadata.toString());

        if ( metadata.isDirectory() == false )
            throw new NotDirCacheException(pnfsPath);

        PnfsId pnfsId = _pnfsHandler.getPnfsIdByPath(pnfsPath);

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

    public AccessType canSetAttributes(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute) throws CacheException, ACLException {
        PnfsId pnfsId = _pnfsHandler.getPnfsIdByPath(pnfsPath);
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
    * This method asks for permission to create an object (file or sub-directory)
    * in this directory (given by its pnfsId: pnfsParentId).
    * @param pnfsParentId
    *            PNFS ID of parent directory
    * @param subject
    * @param origin
    * @param isDir
    *            TRUE if object to be created is a sub-directory, otherwise FALSE
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

    /**
    * This method asks for permission to delete this object (file or sub-directory)
    * given by its pnfsId
    * @param pnfsId
    *            PNFS ID of the object to be deleted
    * @param subject
    * @param origin
    * @param isDir
    *            TRUE if the object to be deleted is a directory, otherwise FALSE
    * @return AccessType
    * @throws CacheException
    * @throws ACLException
    */
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

   /**
    * This method asks for permission to delete this object (file or sub-directory)
    * given by its pnfsPath
    * @param pnfsPath
    *            PNFS PATH of the object to be deleted
    * @param subject
    * @param origin
    * @param isDir
    *            TRUE if the object to be deleted is a directory, otherwise FALSE
    * @return AccessType
    * @throws CacheException
    * @throws ACLException
    */
    private AccessType canDelete(String pnfsPath, Subject subject, Origin origin, Boolean isDir) throws CacheException, ACLException {
        final String OPERATION = "ACLPermissionHandler.canDelete" + (isDir ? "Dir " : "File ");
        if ( _logger.isDebugEnabled() )
            _logger.debug(OPERATION + args2String(pnfsPath, subject, origin));

        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        final FileMetaData metadataParent = _metadataSource.getMetaData(parent);
        if ( _logger.isDebugEnabled() )
            _logger.debug("Parent Metadata: " + metadataParent.toString());

        if ( metadataParent.isDirectory() == false )
            throw new NotDirCacheException(parent);

        final FileMetaData metadata = _metadataSource.getMetaData(pnfsPath);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( isDir ) {
            if ( metadata.isDirectory() == false )
                throw new NotDirCacheException(pnfsPath);

        } else if ( metadata.isRegularFile() == false )
            throw new NotFileCacheException(pnfsPath);

        PnfsId pnfsParentId = _pnfsHandler.getPnfsIdByPath(parent);
        PnfsId pnfsId = _pnfsHandler.getPnfsIdByPath(pnfsPath);

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
