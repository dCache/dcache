package diskCacheV111.services.acl;

import java.io.File;
import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.acl.ACLException;
import org.dcache.acl.Origin;
import org.dcache.acl.enums.FileAttribute;

import diskCacheV111.services.FileMetaDataSource;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;

import javax.security.auth.Subject;

/**
 * Abstract class that implements interface PermissionHandler.
 *
 * @author David Melkumyan, Irina Kozlova
 *
 */
public abstract class AbstractPermissionHandler implements PermissionHandler {

    private static final Logger _logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + AbstractPermissionHandler.class.getName());

    private static final long DEFAULT_PNFS_TIMEOUT = 60 * 1000L;

    protected FileMetaDataSource _metadataSource;

    protected final PnfsHandler _pnfsHandler;

    protected AbstractPermissionHandler(CellEndpoint cell) throws ACLException {
        if (cell == null)
            throw new ACLException("Initialize Permission Handler failed: Argument 'cell' is NULL.");

        final Args args = cell.getArgs();
        try {
        String metadataProvider = args.getOpt("meta-data-provider");
        if ( metadataProvider == null || metadataProvider.length() == 0 )
            metadataProvider = "diskCacheV111.services.PnfsManagerFileMetaDataSource";

            _logger.debug("Loading metadata provider: " + metadataProvider);
            Class<?>[] argClass = { CellEndpoint.class };
            Constructor<?> constructor = Class.forName(metadataProvider).getConstructor(argClass);
            Object[] init_args = { cell };
            _metadataSource = (FileMetaDataSource) constructor.newInstance(init_args);

            String pnfsManager = args.getOpt("pnfsManager");
            if (pnfsManager == null || pnfsManager.length() == 0)
                pnfsManager = "PnfsManager";

            _logger.debug("Initializing PnfsHandler from: " + pnfsManager);
            _pnfsHandler = new PnfsHandler(cell, new CellPath(pnfsManager));
            long msec = DEFAULT_PNFS_TIMEOUT;
            String tmpstr = null;
            try {
                tmpstr = args.getOpt("pnfsTimeout");
                if(tmpstr == null || tmpstr.length() == 0)  {
                    msec = DEFAULT_PNFS_TIMEOUT;
                } else {
                    int secTimeout = Integer.parseInt(tmpstr);
                    if (secTimeout < 0) {
                        throw new IllegalArgumentException("Incorrect configuraion of pnfsTimeout (negative number): "+ tmpstr);
                    }
                    msec = secTimeout * 1000L;
                }
            } catch (java.lang.NumberFormatException e) {
                throw new IllegalArgumentException("Incorrect configuraion of pnfsTimeout: "+ tmpstr);
            }
            _pnfsHandler.setPnfsTimeout(msec);

        } catch (Exception e) {
            throw new ACLException("Failed to Initialize ACL ", e);
        }
    }

    public FileMetaDataSource getMetadataSource() {
        return _metadataSource;
    }

    public void setMetadataSource(FileMetaDataSource metadataSource) {
        _metadataSource = metadataSource;
    }

    protected PnfsId getParentId(PnfsId pnfsId) throws CacheException {

        PnfsId parentPnfsId = _pnfsHandler.getParentOf(pnfsId);
        if (_logger.isDebugEnabled()) {
            _logger.debug("Getting parent pnfsId of: " + pnfsId);
            _logger.debug("parentPnfsId = " + parentPnfsId);
        }

        return parentPnfsId;
    }

    protected String getParentPath(String pnfsPath) {
        return (new File(pnfsPath)).getParent();
    }

    protected String args2String(String pnfsPath) {
        return "Args:\n" + "pnfsPath: " + pnfsPath + "\n";
    }

    protected String args2String(PnfsId pnfsId) {
        return "Args:\n" + "pnfsId: " + pnfsId.toString() + "\n";
    }

    protected String args2String(PnfsId pnfsId, Subject subject, Origin origin) {
        StringBuilder sb = new StringBuilder("Args:\n");
        sb.append("pnfsId: ").append(pnfsId).append("\n");
        sb.append("subject: ").append(subject.toString()).append("\n");
        sb.append("origin: ").append(origin).append("\n");
        return sb.toString();
    }

    protected String args2String(String pnfsPath, Subject subject, Origin origin) {
        StringBuilder sb = new StringBuilder("Args:\n");
        sb.append("pnfsPath: ").append(pnfsPath).append("\n");
        sb.append("subject: ").append(subject.toString()).append("\n");
        sb.append("origin: ").append(origin).append("\n");
        return sb.toString();
    }

    protected String args2String(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) {
        StringBuilder sb = new StringBuilder(args2String(pnfsId, subject, origin));
        sb.append("attributes: ").append(attribute.toString()).append("\n");
        return sb.toString();
    }

    protected String args2String(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute) {
        StringBuilder sb = new StringBuilder(args2String(pnfsPath, subject, origin));
        sb.append("attributes: ").append(attribute.toString()).append("\n");
        return sb.toString();
    }
}
