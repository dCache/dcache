//______________________________________________________________________________
//
// $Id: TransferManagerHandlerBackup.java,v 1.2 2006-10-06 20:36:49 litvinse Exp $
// $Author: litvinse $
//
// created 05/06 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________


package diskCacheV111.services;


public class TransferManagerHandlerBackup {
	private String pnfsPath;
	boolean store;
	boolean created;
	private String          pnfsIdString;
	private String          remoteUrl;
	transient boolean locked;
	private String pool;
	private int state;
	private long id;
	private Integer moverId;
	private long creationTime;
	private long lifeTime;
	private Long credentialId;

	private TransferManagerHandlerBackup() {
	}
	public TransferManagerHandlerBackup(TransferManagerHandler handler) {

		creationTime = handler.getCreationTime();
		lifeTime     = handler.getLifeTime();
		id           = handler.getId();
		pnfsPath     = handler.getPnfsPath();
		pnfsIdString = handler.getPnfsIdString();
		pool         = handler.getPool();
		store        = handler.getStore();
		created      = handler.getCreated();
		locked       = handler.getLocked();
		remoteUrl    = handler.getRemoteUrl();
		moverId      = handler.getMoverId();
		state        = handler.getState();
		credentialId = handler.getCredentialId();

	}
}
