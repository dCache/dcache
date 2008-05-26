package diskCacheV111.services.acl;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.log4j.Logger;
import org.dcache.chimera.acl.ACLException;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.FileAttribute;
import org.dcache.chimera.acl.handler.AclFsHandler;

import diskCacheV111.services.FileMetaDataSource;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAdapter;
import dmg.util.Args;

/**
 * @author mdavid, irinak
 *
 */
public abstract class AbstractPermissionHandler implements PermissionHandlerInterface {

	private static final Logger logger = Logger.getLogger("logger.org.dcache.authorization." + AbstractPermissionHandler.class.getName());

	protected final FileMetaDataSource metadataSource ;

	protected AbstractPermissionHandler(CellAdapter cell) throws ACLException {
		final Args args = cell.getArgs();

		String metadataProvider = args.getOpt("meta-data-provider");
		if ( metadataProvider == null || metadataProvider.length() == 0 )
			metadataProvider = "diskCacheV111.services.PnfsManagerFileMetaDataSource";

		logger.debug("Loading metadata provider: " + metadataProvider);

		try {

    		Class<?>[] argClass = { dmg.cells.nucleus.CellAdapter.class };
    		Constructor<?> constructor = Class.forName(metadataProvider).getConstructor(argClass);
    		Object[] init_args = { cell };
    		metadataSource = (FileMetaDataSource) constructor.newInstance(init_args);

		}catch(IllegalArgumentException e) {
		    throw new ACLException("Failed to initialize ACL", e);
		}catch(NoSuchMethodException e) {
		    throw new ACLException("Failed to initialize ACL", e);
		}catch(ClassNotFoundException e) {
		    throw new ACLException("Failed to initialize ACL", e);
		}catch(InvocationTargetException e) {
		    throw new ACLException("Failed to initialize ACL", e);
		}catch(IllegalAccessException e) {
		    throw new ACLException("Failed to initialize ACL", e);
		}catch(InstantiationException e) {
		    throw new ACLException("Failed to initialize ACL", e);
		}
	}

	protected PnfsId getParentId(PnfsId pnfsId) {
		return null; // TODO: implement
	}

	protected String getParentPath(String pnfsPath) {
		return (new File(pnfsPath)).getParent();
	}

	protected String getParentPath(FsPath fsPath) {
		fsPath.add(".."); // go one level up
		return fsPath.toString();
	}

	protected String args2String(String pnfsPath) {
		return "Args:\n" + "pnfsPath: " + pnfsPath + "\n";
	}
	protected String args2String(PnfsId pnfsId) {
		return "Args:\n" + "pnfsId: " + pnfsId.toString() + "\n";
	}

	protected String args2String(String pnfsPath, Subject subject, Origin origin) {
		StringBuilder sb = new StringBuilder("Args:\n");
		sb.append("pnfsPath: ").append(pnfsPath).append("\n");
		sb.append("subject: ").append(subject).append("\n");
		sb.append("origin: ").append(origin).append("\n");
		return sb.toString();
	}
	protected String args2String(PnfsId pnfsId, Subject subject, Origin origin) {
		StringBuilder sb = new StringBuilder("Args:\n");
		sb.append("pnfsId: ").append(pnfsId).append("\n");
		sb.append("subject: ").append(subject).append("\n");
		sb.append("origin: ").append(origin).append("\n");
		return sb.toString();
	}

	protected String args2String(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute) {
		StringBuilder sb = new StringBuilder(args2String(pnfsPath, subject, origin));
		sb.append("attributes: ").append(attribute.toString()).append("\n");
		return sb.toString();
	}
	protected String args2String(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) {
		StringBuilder sb = new StringBuilder(args2String(pnfsId, subject, origin));
		sb.append("attributes: ").append(attribute.toString()).append("\n");
		return sb.toString();
	}

}
