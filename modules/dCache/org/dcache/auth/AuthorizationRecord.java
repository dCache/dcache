/*
 * AuthorizationRecord.java
 *
 * Created on August 14, 2008, 11:13 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.auth;

import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.Serializable;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Transient;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
//import static javax.persistence.FetchType.LAZY;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.FetchType.EAGER;
import org.dcache.srm.SRMUser;
import javax.security.auth.Subject;
import java.security.Principal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.ReadOnly;
import org.globus.gsi.jaas.GlobusPrincipal;


/**
 *
 * @author Timur, Ted
 */

@Entity
@Table(name="authrecord")
public class AuthorizationRecord implements Serializable, SRMUser{
    private static final long serialVersionUID = 7412538400840464074L;

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
    private int priority = 0;
    private String home = null;
    private String root = null;
    private boolean readOnly = false;
    private int currentGIDindex=0;
    private String authn = null;
    private String authz = null;

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
        this(new LoginReply(subject, Collections.EMPTY_SET));
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

        List<GroupList> groupLists = new LinkedList<GroupList>();

        GroupList primaryGroupList = new GroupList();
        primaryGroupList.setAuthRecord(this);
        primaryGroupList.setGroups(new ArrayList<Group>());
        groupLists.add(primaryGroupList);

        for (Principal principal: login.getSubject().getPrincipals()) {
            if (principal instanceof UidPrincipal) {
                if (hasUid) {
                    throw new IllegalArgumentException("Cannot convert Subject with more than one UID");
                }
                hasUid = true;
                setUid((int) ((UidPrincipal) principal).getUid());
            } else if (principal instanceof FQANPrincipal) {
                FQANPrincipal fqanPrincipal = (FQANPrincipal) principal;
                if (fqanPrincipal.isPrimary() && primaryGroupList.getAttribute() == null) {
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
                setName(((GlobusPrincipal) principal).getName());
            } else if (principal instanceof UserNamePrincipal) {
                setIdentity(((UserNamePrincipal) principal).getName());
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

        setId();
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
    public void setId() {
        if (id != 0) return;
        id = getId(this);
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
        this.home = Home;
    }

    @Basic
    @Column( name="root")
    public String getRoot() {
        return root;
    }

    public void setRoot(String Root) {
        this.root = Root;
    }

    @Basic
    @Column( name="read_only")
    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean ReadOnly) {
        this.readOnly = ReadOnly;
    }

    @Transient
    public String  getAuthn() {
        if (authn == null) initHashStrings();
        return authn;
    }

    @Transient
    public String  getAuthz() {
        if (authz == null) initHashStrings();
        return authz;
    }

    @Transient
    public int getCurrentGIDindex() {
        return currentGIDindex;
    }

    public void setCurrentGIDindex(int currentGIDindex) {
        this.currentGIDindex = currentGIDindex;
    }

    public void incrementGIDindex() {
        currentGIDindex++;
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
        StringBuilder sb = new java.lang.StringBuilder("AR:");
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

    public String hashCodeString() {
        return Integer.toHexString(hashCode());
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
    public long getId(AuthorizationRecord authrec) {
        long id = authrec.getId();

        if(id != 0 ) return id;

        int authn_hash = getAuthn().hashCode();
        int authz_hash = getAuthz().hashCode();

        id = (((long)authn_hash) <<32) | (authz_hash & 0x0FFFFFFFFL);
        return id;
    }

    private  void initHashStrings() {
        if(this.authn != null && this.authz !=null) return;
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
       if ( this == rec ) return true;
       if ( !(rec instanceof AuthorizationRecord) ) return false;
       AuthorizationRecord r =  (AuthorizationRecord) rec;

       return
           identity.equals(r.getIdentity()) &&
           name.equals(r.getName()) &&
           uid==r.getUid() &&
           readOnly==r.isReadOnly() &&
           groupLists.equals(r.getGroupLists()) &&
           priority==r.getPriority() &&
           home.equals(r.getHome()) &&
           root.equals(r.getRoot());
    }

    @Override
    public int hashCode(){
        initHashStrings();
        return getAuthn().hashCode()^getAuthz().hashCode();
    }

    @Transient
    public int[] getGids() {
        List<Integer> gids = new ArrayList<Integer>();
        if(groupLists != null) {
            for(GroupList groupList : groupLists) {
                List<Group> groups = groupList.getGroups();
                if(groups!=null) {
                    for(Group group:groups) {
                        gids.add(group.getGid());
                    }
                }
            }
        }
        if(gids.isEmpty()) {
            return new int[] {-1};
        }

        int [] gidIntArray = new int[gids.size()];
        for(int i=0; i<gidIntArray.length; i++) {
            gidIntArray[i] = gids.get(i);
        }
        return gidIntArray;
    }

    @Transient
    public Subject getSubject() {
        return Subjects.getSubject(this);
    }

    /**
     *
     * @return UserAuthRecord which corresponds to this GroupList
     */
    @Transient
    public UserAuthRecord getUserAuthRecord() {
        return new UserAuthRecord(
                getIdentity(),
                getName(),
                getPrimaryAttribute(),
                isReadOnly(),
                getPriority(),
                getUid(),
                getGids(),
                getHome(),
                getRoot(),
                "/",
                new HashSet<String>());

    }
}
