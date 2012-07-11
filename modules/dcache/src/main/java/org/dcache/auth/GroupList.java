/*
 * GroupSet.java
 *
 * Created on August 14, 2008, 11:21 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.auth;
import java.util.List;
import java.util.HashSet;
import java.io.Serializable;
import javax.persistence.Entity;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Basic;
import javax.persistence.Transient;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.ManyToOne;
import javax.persistence.GeneratedValue;
import javax.persistence.OrderBy;
import javax.persistence.Table;
import javax.persistence.Column;
import javax.persistence.JoinColumn;
import static javax.persistence.GenerationType.AUTO;
import static javax.persistence.FetchType.EAGER;
import static javax.persistence.CascadeType.ALL;

/**
 *
 * @author timur
 */
@Entity
@Table(name="authgrouplist")
public class GroupList implements Serializable{
    private static final long serialVersionUID = 6742997958634986522L;
    private long id;
    private String attribute;
    private List<Group> groups;


    /**
     * this is here to implement bydirectional
     * ManyToOne/OneToMany relationship
     * with AuthorizationRecord
     */
     private AuthorizationRecord authRecord;

    /**
     * Creates a new instance of GroupSet
     */
    public GroupList() {
    }

    @Id // property access here as well
    @GeneratedValue (strategy=AUTO, generator="authsequence")
    @Column( name="id")
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Basic
    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    @OneToMany(mappedBy="groupList",
        fetch=EAGER,
        targetEntity=Group.class,
        cascade = {ALL})
    @OrderBy //PK is assumed
    public List<Group> getGroups() {
        return groups;
    }

    public void setGroups(List<Group> groups) {
        this.groups = groups;
    }

    @ManyToOne( targetEntity=AuthorizationRecord.class,
        fetch=EAGER
        /*,
          optional=false */ )
    @JoinColumn( name="authrecord_id")
    public AuthorizationRecord getAuthRecord() {
        return authRecord;
    }

    public void setAuthRecord(AuthorizationRecord authRecord) {
        this.authRecord = authRecord;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        toStringBuilder(sb);
        return sb.toString();
    }

    public void toStringBuilder(StringBuilder sb) {

        sb.append("GL:");
        sb.append(attribute).append(' ');
        if(groups != null)
        {
           sb.append(groups.size()).append(" groups : [");
            for(Group group : groups)
            {
                group.toStringBuilder(sb);
                sb.append(',');
            }
            sb.append(']');
        } else {
            sb.append(" [empty]");
        }
        /*
         * this was and may be again needed for debug of JPA

        sb.append(Integer.toHexString(hashCode())).append(' ');
        sb.append(" ar=");
        if(authRecord == null) {
            sb.append("null");
        } else {
            sb.append(authRecord.hashCodeString());
        }
         */

    }

    public String toShortString() {
        StringBuilder sb = new StringBuilder();
        sb.append(attribute).append(' ');
        if(groups != null) {
           sb.append('[');
            for(Group group : groups) {
                sb.append(group.getGid()).append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(']');
        } else {
            sb.append("null");
        }

        return sb.toString();
    }

    public String hashCodeString() {
         return Integer.toHexString(hashCode());
    }

    @Transient
    public Group getPrimaryGroup() {
        if(groups != null && ! groups.isEmpty()  ) {
            return groups.get(0);
        }
        return null;
    }

    public boolean equals (Object glist) {
        if ( this == glist ) {
            return true;
        }
        if ( !(glist instanceof GroupList) ) {
            return false;
        }
        GroupList gl =  (GroupList) glist;
        if(attribute==null) {
            if (gl.getAttribute() != null) {
                return false;
            }
        } else {
            if (!attribute.equals(gl.getAttribute())) {
                return false;
            }
        }

        if(groups==null) {
            if (gl.getGroups() != null) {
                return false;
            }
        } else {
            if (!groups.equals(gl.getGroups())) {
                return false;
            }
        }

        return true;
    }

    public int hashCode(){
        return toShortString().hashCode();
    }

    /**
     *
     * @return UserAuthRecord which corresponds to this GroupList
     */
    @Transient
    public UserAuthRecord getUserAuthRecord() {
        int i=0, glsize = groups.size();
        int GIDS[] = (glsize > 0) ? new int[glsize] : null;
        for(Group group : groups) {
             GIDS[i++] = group.getGid();
        }
        return new UserAuthRecord(
                authRecord.getIdentity(),
                authRecord.getName(),
                attribute,
                authRecord.isReadOnly(),
                authRecord.getPriority(),
                authRecord.getUid(),
                GIDS,
                authRecord.getHome(),
                authRecord.getRoot(),
                "/",
                new HashSet<String>());

    }
}
