package org.dcache.srm.unixfs;

public abstract class UserAuthBase
{
    public String Username;
    public int UID = -1;
    public int GID = -1;
    public String Home;
    public String Root;
    public String FsRoot;
    public boolean ReadOnly;

    public UserAuthBase(String user, boolean readOnly, int uid, int gid, String home, 
			String root, String fsroot)
    {
	Username = user;
	ReadOnly = readOnly;
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
