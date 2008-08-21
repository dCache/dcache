/*
 * AuthorizationRecord.java
 *
 * Created on August 14, 2008, 11:13 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.auth;

import java.util.Collection;
import java.util.Collection;
import java.io.Serializable;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Transient;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.GeneratedValue;
import javax.persistence.OrderBy;
//import static javax.persistence.FetchType.LAZY;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.FetchType.EAGER;
/**
 *
 * @author Timur, Ted
 */

@Entity
@Table(name="authrecord")
public class AuthorizationRecord implements Serializable{
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
    private Collection<GroupList> groupLists;
    private int priority = 0;
    private String home = null;
    private String root = null;
    private boolean readOnly = false;
    private int currentGIDindex=0;
    
    /** Creates a new instance of AuthorizationRecord */
    public AuthorizationRecord() {
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
    public Collection<GroupList> getGroupLists() {
        return groupLists;
    }

    public void setGroupLists(Collection<GroupList> groupLists) {
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
    
    public String toString()
    {
        StringBuilder sb = new java.lang.StringBuilder("AR:");
        sb.append(Integer.toHexString(hashCode()));
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
        sb.append( root ).append("\n< ");
        if(groupLists != null)
        {
            sb.append(groupLists.size()).append(" groupLists :\n");
            for(GroupList groupList : groupLists)
            {
                sb.append("  ").append(groupList).append('\n');
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

}
