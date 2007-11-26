//package diskCacheV111.services.authorization.gplazmalite.storageauthzdbService;
package gplazma.gplazmalite.storageauthzdbService;

import java.util.*;

public abstract class StorageAuthorizationBase extends Object
{
    public final String Username;
    public final int priority;
    public final int UID;
    public final int GID;
    public final String Home;
    public final String Root;
    public final String FsRoot;
    public final boolean ReadOnly;

    public StorageAuthorizationBase(String user, boolean readOnly, int priority, int uid, int gid, String home,
			String root, String fsroot)
    {
	Username = user;
	ReadOnly = readOnly;
  this.priority = priority;
	UID = uid;
	GID = gid;
	Home = home;
	Root = root;
	FsRoot = fsroot;
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

    abstract public boolean isAnonymous();
    abstract public boolean isWeak();
	
	
}
