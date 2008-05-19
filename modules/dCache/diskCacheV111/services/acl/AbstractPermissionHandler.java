package diskCacheV111.services.acl;

import java.io.File;
import java.lang.reflect.Constructor;

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

	protected AclFsHandler aclHandler = null;
	protected FileMetaDataSource metadataSource = null;

	protected AbstractPermissionHandler(CellAdapter cell) throws ACLException, Exception {
		final Args args = cell.getArgs();

		String acl_props = args.getOpt("acl-permission-handler-config");
		if ( acl_props == null || acl_props.length() == 0 )
			throw new IllegalArgumentException("acl-permission-handler-config option not defined");

		aclHandler = new AclFsHandler(acl_props);

		String metadataProvider = args.getOpt("meta-data-provider");
		if ( metadataProvider == null || metadataProvider.length() == 0 )
			metadataProvider = "diskCacheV111.services.PnfsManagerFileMetaDataSource";

		logger.debug("Loading metadata provider: " + metadataProvider);

		Class<?>[] argClass = { dmg.cells.nucleus.CellAdapter.class };
		Constructor<?> constructor = Class.forName(metadataProvider).getConstructor(argClass);
		Object[] init_args = { cell };
		metadataSource = (FileMetaDataSource) constructor.newInstance(init_args);
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
