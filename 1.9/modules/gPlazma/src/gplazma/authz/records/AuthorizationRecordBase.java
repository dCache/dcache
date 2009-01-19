//package diskCacheV111.services.authorization.authz.records;
package gplazma.authz.records;

import java.io.Serializable;

public abstract class AuthorizationRecordBase implements Serializable
{
    private static final long serialVersionUID = -8465745392264779539L;
    private String Username;
    private int priority;
    private int UID;
    private int[] GIDs;//=new int[]{-1};
    private int GID;//=-1; //for backward compatibility
    private String Home;
    private String Root;
    private String FsRoot;
    private boolean ReadOnly;

    public AuthorizationRecordBase(String user, boolean readOnly, int priority, int uid, int[] gids, String home,
			String root, String fsroot)
    {
	Username = user;
	ReadOnly = readOnly;
  this.priority = priority;
	UID = uid;
	GIDs = (gids != null) ? gids : new int[]{-1};
  GID = GIDs[0];
  Home = home;
	Root = root;
	FsRoot = fsroot;
    }

    public AuthorizationRecordBase() {
        this( null, true, 0, -1, null, "", "", "");
    }

    public boolean isReadOnly() {
	return ReadOnly;
    }

    public String readOnlyStr() {
	if(ReadOnly) {
	    return "read-only";
	} else {
	    return "read-write";
	}
    }

    public String getUsername() {
        return Username;
    }

    public int getPriority() {
        return priority;
    }

    public int getUID() {
        return UID;
    }

    public int[] getGIDs() {
        return GIDs;
    }

    public int getGID() {
        return GID;
    }

    public String getHome() {
        return Home;
    }

    public String getRoot() {
        return Root;
    }

    public String getFsRoot() {
        return FsRoot;
    }

    //abstract public boolean isAnonymous();
    //abstract public boolean isWeak();
	
	
}
