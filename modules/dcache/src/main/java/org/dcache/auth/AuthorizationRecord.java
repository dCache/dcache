package org.dcache.auth;

import com.google.common.base.Objects;
import org.globus.gsi.jaas.GlobusPrincipal;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.security.auth.Subject;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.FetchType.EAGER;

//import static javax.persistence.FetchType.LAZY;


/**
 *
 * @author Timur, Ted
 */

@Entity
@Table(name="authrecord")
public class AuthorizationRecord {

    private static final String PRIMARY_ATTRIBUTE_PREFIX_THAT_RETURN_IDENTITY_AS_VO_GROUP = "/Role=";

    /**
     *this is the id of the authorization record that is used as
     * a primary key in the database
     * it is set to a unique value by gPlazma
     * It has nothing to do with user id
     */
    private long id;
    private String identity;
    private String name;
    private int uid;
    private List<GroupList> groupLists;
    private int priority;
    private String home = "/";
    private String root = "/";
    private boolean readOnly;
    private String authn;
    private String authz;

    /** Creates a new instance of AuthorizationRecord */
    public AuthorizationRecord() {
    }


    /**
     * Converts a Subject to an AuthorizationRecord. The the UID
     * (UidPrincipal), GID (GidPrincipal), the mapped user name
     * (UserNamePrincipal), the DN (GlobusPrincipal), and FQAN
     * (FQANPrincipal) will be included in the AuthorizationRecord.
     *
     * Notice that the AuthorizationRecord will represent a subset of
     * the information stored in the subject.
     *
     * All GIDs will become part of the primary group list. The
     * primary GIDs will appear first in the primary group list.
     */
    public AuthorizationRecord(Subject subject)
    {
        this(new LoginReply(subject, Collections.<LoginAttribute>emptySet()));
    }

    /**
     * Converts a LoginReply to an AuthorizationRecord. The the UID
     * (UidPrincipal), GID (GidPrincipal), the mapped user name
     * (UserNamePrincipal), the DN (GlobusPrincipal), and FQAN
     * (FQANPrincipal), home directory (HomeDirectory), root directory
     * (RootDirectory) and read-only (ReadOnly) status will be
     * included in the AuthorizationRecord.
     *
     * Notice that the AuthorizationRecord will represent a subset of
     * the information stored in the LoginReply.
     *
     * All GIDs will become part of the primary group list. The
     * primary GIDs will appear first in the primary group list.
     */
    public AuthorizationRecord(LoginReply login)
    {
        boolean hasUid = false;

        List<GroupList> groupLists = new LinkedList<>();

        GroupList primaryGroupList = new GroupList();
        primaryGroupList.setAuthRecord(this);
        primaryGroupList.setGroups(new ArrayList<Group>());
        groupLists.add(primaryGroupList);

        /* Identity is not allowed to be null. However both user name
         * and group name are optional. Hence we fall back to an empty
         * identity is neither user name nor group name are provided.
         */
        setIdentity("");

        for (Principal principal: login.getSubject().getPrincipals()) {
            if (principal instanceof UidPrincipal) {
                if (hasUid) {
                    throw new IllegalArgumentException("Cannot convert Subject with more than one UID");
                }
                hasUid = true;
                setUid((int) ((UidPrincipal) principal).getUid());
            } else if (principal instanceof FQANPrincipal) {
                FQANPrincipal fqanPrincipal = (FQANPrincipal) principal;
                if (fqanPrincipal.isPrimaryGroup() && primaryGroupList.getAttribute() == null) {
                    primaryGroupList.setAttribute(fqanPrincipal.getName());
                } else {
                    GroupList groupList = new GroupList();
                    groupList.setAuthRecord(this);
                    groupList.setAttribute(fqanPrincipal.getName());
                    groupList.setGroups(new ArrayList<Group>());
                    groupLists.add(groupList);
                }
            } else if (principal instanceof GidPrincipal) {
                GidPrincipal gidPrincipal = (GidPrincipal) principal;
                Group group = new Group();
                group.setGroupList(primaryGroupList);
                group.setGid((int) gidPrincipal.getGid());
                if (gidPrincipal.isPrimaryGroup()) {
                    primaryGroupList.getGroups().add(0, group);
                } else {
                    primaryGroupList.getGroups().add(group);
                }
            } else if (principal instanceof GlobusPrincipal) {
                setName(principal.getName());
            } else if (principal instanceof UserNamePrincipal) {
                identity = principal.getName();
            } else if (principal instanceof GroupNamePrincipal) {
                GroupNamePrincipal group = (GroupNamePrincipal) principal;
                if (identity != null && group.isPrimaryGroup()) {
                    identity = principal.getName();
                }
            }
        }

        if (!hasUid) {
            throw new IllegalArgumentException("Cannot convert Subject without UID");
        }

        setGroupLists(groupLists);

        for (LoginAttribute attribute: login.getLoginAttributes()) {
            if (attribute instanceof RootDirectory) {
                setRoot(((RootDirectory) attribute).getRoot());
            } else if (attribute instanceof HomeDirectory) {
                setHome(((HomeDirectory) attribute).getHome());
            } else if (attribute instanceof ReadOnly) {
                setReadOnly(((ReadOnly) attribute).isReadOnly());
            }
        }

        resetId();
    }

    /**
     * Converts this AuthorizationRecord to a Subject. The Subject
     * will contain the UID (UidPrincipal), any GIDs (GidPrincipal),
     * the mapped user name (UserNamePrincipal), the DN
     * (GlobusPrincipal), and any FQANs (FQANPrincipal) of the
     * AuthorizationRecord object.
     *
     * Note that the Subject will represent a subset of the
     * information stored in this AuthorizationRecord.
     */
    public Subject toSubject()
    {
        Subject subject = new Subject();
        Set<Principal> principals = subject.getPrincipals();
        principals.add(new UidPrincipal(getUid()));

        String identity = getIdentity();
        if (identity != null && !identity.isEmpty()) {
            principals.add(new UserNamePrincipal(identity));
        }

        boolean primary = true;
        for (GroupList list : getGroupLists()) {
            String fqan = list.getAttribute();
            if (fqan != null && !fqan.isEmpty()) {
                principals.add(new FQANPrincipal(fqan, primary));
            }
            for (Group group: list.getGroups()) {
                principals.add(new GidPrincipal(group.getGid(), primary));
                primary = false;
            }
            primary = false;
        }

        String dn = getName();
        if (dn != null && !dn.isEmpty()) {
            principals.add(new GlobusPrincipal(dn));
        }

        return subject;
    }

    /**
     *this is the id getter
     * this id is a property of the authorization record that is used as
     * a primary key in the database
     * it is set to a unique value by gPlazma
     * It has nothing to do with user id
     */
    @Id  // property access is used
    @Column( name="id")
    public long getId() {
        return id;
    }

    /**
     *this is the id setter
     * this id is a property of the authorization record that is used as
     * a primary key in the database
     * it is set to a unique value by gPlazma
     * It has nothing to do with user id
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Set the id to a value computed from getId().
     */
    public void resetId() {
        if (id != 0) {
            return;
        }
        id = computeId(this);
    }

    @Basic
    @Column( name="identity")
    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    @Basic
    @Column( name="uid")
    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    @OneToMany(mappedBy="authRecord",
        fetch=EAGER,
        targetEntity=GroupList.class,
        cascade = {ALL})
    @OrderBy //PK is assumed
    public List<GroupList> getGroupLists() {
        return groupLists;
    }

    public void setGroupLists(List<GroupList> groupLists) {
        this.groupLists = groupLists;
    }

    @Basic
    @Column( name="priority")
    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Basic
    @Column( name="home")
    public String getHome() {
        return home;
    }

    public void setHome(String Home) {
        checkNotNull(Home);
        this.home = Home;
    }

    @Basic
    @Column( name="root")
    public String getRoot() {
        return root;
    }

    public void setRoot(String Root) {
        checkNotNull(Root);
        this.root = Root;
    }

    @Basic
    @Column(name="read_only")
    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean ReadOnly) {
        this.readOnly = ReadOnly;
    }

    @Transient
    public String  getAuthn() {
        if (authn == null) {
            initHashStrings();
        }
        return authn;
    }

    @Transient
    public String  getAuthz() {
        if (authz == null) {
            initHashStrings();
        }
        return authz;
    }

    @Basic
    @Column( name="name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("AR:");
        sb.append(getId());
        sb.append(' ').append(identity);
        sb.append(' ').append( name);
        sb.append(' ').append( uid);
          if(readOnly) {
            sb.append(" read-only ");
          } else {
            sb.append(" read-write ");
          }
        sb.append( priority ).append(' ');
        sb.append( home ).append(' ');
        sb.append( root ).append(" < ");
        if(groupLists != null)
        {
            sb.append(groupLists.size()).append(" groupLists : ");
            for(GroupList groupList : groupLists)
            {
                sb.append("  ").append(groupList).append(';');
            }
        } else {
            sb.append("empty");
        }
        sb.append(" >");
        return sb.toString();
    }

    @Transient
    public String getVoRole() {
        String primaryAttribute = getPrimaryAttribute();
        if( FQAN.isValid( primaryAttribute)) {
            FQAN fqan = new FQAN(primaryAttribute);
            if( fqan.hasRole()) {
                return fqan.getRole();
            }
        }
        return null;
    }

    @Transient
    public String getVoGroup() {
        String primaryAttribute = getPrimaryAttribute();
        if(primaryAttribute != null && !primaryAttribute.isEmpty()) {
            if(FQAN.isValid(primaryAttribute)) {
                return new FQAN(primaryAttribute).getGroup();
            } else {
                if( !primaryAttribute.startsWith(
                        PRIMARY_ATTRIBUTE_PREFIX_THAT_RETURN_IDENTITY_AS_VO_GROUP)) {
                    return primaryAttribute;
                }
            }
        }
        return identity;
    }

    @Transient
    protected String getPrimaryAttribute() {
        GroupList primaryGroupList = getPrimaryGroupList();
        if(primaryGroupList != null) {
             return primaryGroupList.getAttribute();
        }
        return null;
    }

    @Transient
    protected GroupList getPrimaryGroupList() {
        if(groupLists != null && !groupLists.isEmpty() ) {
             return groupLists.get(0);
        }
        return null;
    }

    @Transient
    public int getGid() {
        GroupList primaryGroupList = getPrimaryGroupList();
        if(primaryGroupList != null) {
             Group primaryGroup =  primaryGroupList.getPrimaryGroup();
             if(primaryGroup != null) {
                 return primaryGroup.getGid();
             }
        }
        return -1;
    }

    @Transient
    public long computeId(AuthorizationRecord authrec) {
        long id = authrec.getId();

        if(id != 0 ) {
            return id;
        }

        int authn_hash = getAuthn().hashCode();
        int authz_hash = getAuthz().hashCode();

        id = (((long)authn_hash) <<32) | (authz_hash & 0x0FFFFFFFFL);
        return id;
    }

    private  void initHashStrings() {
        if(this.authn != null && this.authz !=null) {
            return;
        }
        StringBuilder authn = new StringBuilder();
        StringBuilder authz = new StringBuilder();
        authn.append(name);
        authz.append(uid);
        if(groupLists != null) {
            authn.append(',');
            authz.append(' ');
            for(GroupList groupList : groupLists) {
                if (groupList != null) {
                    authn.append(groupList.getAttribute()).append('|');
                    authz.append(groupList.toShortString()).append('|');
                } else {
                    authn.append("null|");
                    authz.append("null|");
                }
            }
            authn.deleteCharAt(authn.length() - 1);
            authz.deleteCharAt(authz.length() - 1);
        }

        if(readOnly) {
            authz.append(" read-only");
        } else {
            authz.append(" read-write");
        }
        authz.append(' ').append( priority );
        authz.append(' ').append( home );
        authz.append(' ').append( root );

        this.authn = authn.toString();
        this.authz = authz.toString();
    }

    @Override
    public boolean equals(Object rec) {
       if ( this == rec ) {
           return true;
       }
       if ( !(rec instanceof AuthorizationRecord) ) {
           return false;
       }
       AuthorizationRecord r =  (AuthorizationRecord) rec;

       return
           Objects.equal(identity, r.identity) &&
           Objects.equal(name, r.name) &&
           uid==r.getUid() &&
           readOnly==r.isReadOnly() &&
           Objects.equal(groupLists, r.groupLists) &&
           priority==r.getPriority() &&
           Objects.equal(home, r.home) &&
           Objects.equal(root, r.root);
    }

    @Override
    public int hashCode(){
        initHashStrings();
        return getAuthn().hashCode()^getAuthz().hashCode();
    }
}
