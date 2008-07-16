package org.dcache.auth;

//import org.glite.security.voms.FQAN;

import diskCacheV111.util.*;
import java.util.*;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Basic;
import javax.persistence.Id;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Column;

@Entity
@Inheritance(strategy=InheritanceType.SINGLE_TABLE)
public abstract class UserAuthBase extends Object implements java.io.Serializable {
    static final long serialVersionUID = -7700110348980815506L;
    
    @Id
    @GeneratedValue(strategy=GenerationType.SEQUENCE, generator="USER_AUTH_SEQ")
    @Column(name="id")
    public transient long id;
    @Basic
    public String Username = null;
    @Basic
    public String DN = null;
    @Basic
    private FQAN fqan = null;
    @Basic
    public int priority = 0;
    @Basic
    public int UID = -1;
    @Basic
    public int GID = -1;
    @Basic
    public String Home = null;
    @Basic
    public String Root = null;
    @Basic
    public String FsRoot = null;
    @Basic
    public boolean ReadOnly = false;
    
    public UserAuthBase(String user, String DN, String fqan, boolean readOnly, int priority, int uid, int gid, String home,
        String root, String fsroot) {
        Username = user;
        this.DN = DN;
        if(fqan != null) {
            this.fqan =  new FQAN(fqan);
        }
        ReadOnly = readOnly;
        this.priority = priority;
        UID = uid;
        GID = gid;
        Home = home;
        Root = root;
        FsRoot = fsroot;
    }
    
    public UserAuthBase(String user, String fqan, boolean readOnly, int priority, int uid, int gid, String home,
        String root, String fsroot) {
        Username = user;
        if(fqan != null) {
            this.fqan =  new FQAN(fqan);
        }
        ReadOnly = readOnly;
        this.priority = priority;
        UID = uid;
        GID = gid;
        Home = home;
        Root = root;
        FsRoot = fsroot;
    }
    
    public UserAuthBase(String user, String fqan, boolean readOnly, int uid, int gid, String home,
        String root, String fsroot) {
        Username = user;
        if(fqan != null) {
            this.fqan =  new FQAN(fqan);
        }
        ReadOnly = readOnly;
        UID = uid;
        GID = gid;
        Home = home;
        Root = root;
        FsRoot = fsroot;
    }
    
    public UserAuthBase(String user, boolean readOnly, int priority, int uid, int gid, String home,
        String root, String fsroot) {
        Username = user;
        this.fqan =  new FQAN(user);
        ReadOnly = readOnly;
        this.priority = priority;
        UID = uid;
        GID = gid;
        Home = home;
        Root = root;
        FsRoot = fsroot;
    }
    
    public UserAuthBase(String user, boolean readOnly, int uid, int gid, String home,
        String root, String fsroot) {
        Username = user;
        this.fqan =  new FQAN(user);
        ReadOnly = readOnly;
        UID = uid;
        GID = gid;
        Home = home;
        Root = root;
        FsRoot = fsroot;
    }
    
    /**
     * nonprivate default constructor to sutisfy the JPA requirements
     */
    public UserAuthBase() {
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
    
    
    public String getGroup() {
        if(fqan == null) return null;
        return  fqan.getGroup();
    }
    
    public String getRole() {
        if(fqan == null) return null;
        return fqan.getRole();
    }
    
    public String getCapability() {
        if(fqan == null) return null;
        return fqan.getCapability();
    }
    
    abstract public boolean isAnonymous();
    abstract public boolean isWeak();
    
    public FQAN getFqan() {
        return fqan;
    }
    
    
}
