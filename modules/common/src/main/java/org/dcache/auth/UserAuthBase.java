package org.dcache.auth;


public abstract class UserAuthBase implements java.io.Serializable {
    static final long serialVersionUID = -7700110348980815506L;

    public transient long id;
    public String Username = null;
    public String DN = null;
    private FQAN fqan = null;
    public int priority = 0;
    public int UID = -1;
    public int GID = -1;
    public String Home = null;
    public String Root = null;
    public String FsRoot = null;
    public boolean ReadOnly = false;

    public UserAuthBase(String user, String DN, String fqan, boolean readOnly,
                        int priority, int uid, int gid, String home,
                        String root, String fsroot) {
        Username = user;
        this.DN = DN;
        if(fqan != null) {
            this.fqan = new FQAN(fqan);
        } else if(user != null) {
            this.fqan = new FQAN(user);
        }
        ReadOnly = readOnly;
        this.priority = priority;
        UID = uid;
        GID = gid;
        Home = home;
        Root = root;
        FsRoot = fsroot;
    }

    public UserAuthBase(String user, boolean readOnly, int uid, int gid,
                        String home, String root, String fsroot) {
        this(user, null, null, readOnly, 0, uid, gid, home, root, fsroot);
    }

    /**
     * non-private default constructor to satisfy the JPA requirements
     */
    public UserAuthBase() {
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

    public FQAN getFqan() {
        return fqan;
    }
}
