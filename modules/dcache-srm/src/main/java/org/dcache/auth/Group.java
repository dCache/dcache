/*
 * Group.java
 *
 * Created on August 14, 2008, 11:54 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.auth;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import java.io.Serializable;

import static javax.persistence.FetchType.EAGER;
import static javax.persistence.GenerationType.AUTO;

/**
 *
 * @author timur
 */
@Entity
@Table(name="authgroup")
public class Group implements Serializable{
    private static final long serialVersionUID = -8679725379384055553L;
    private long id;
    private String name;
    private int gid;
    /**
     * this is here to implement bydirectional
     * ManyToOne/OneToMany relationship
     * with GroupList
     */
    private GroupList groupList;
    /** Creates a new instance of Group */
    public Group() {
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
    @Column( name="gid")
    public int getGid() {
        return gid;
    }

    public void setGid(int gid) {
        this.gid = gid;
    }

    @Basic
    @Column( name="name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ManyToOne(targetEntity=GroupList.class,
        fetch=EAGER
        /*,
          optional=false*/)
    @JoinColumn( name="grouplist_id")
    public GroupList getGroupList() {
        return groupList;
    }

    public void setGroupList(GroupList groupList) {
        this.groupList = groupList;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        toStringBuilder(sb);
        return  sb.toString();
    }

   public void toStringBuilder (StringBuilder sb) {
        if(name != null) {
            sb.append(name).append(' ');
        }
        sb.append(gid);
        /*
         * this was and may be again needed for debug of JPA
        sb.append(Integer.toHexString(hashCode())).append(' ');
        sb.append(" gl=");
        if(groupList == null) {
            sb.append("null");
        } else {
            sb.append(groupList.hashCodeString());
        }
         */
    }

    public String hashCodeSting() {
        return Integer.toHexString(hashCode());
    }

    public boolean equals (Object grp) {
        if ( this == grp ) {
            return true;
        }
        if ( !(grp instanceof Group) ) {
            return false;
        }
        Group g =  (Group) grp;
        return
            gid==g.getGid();
    }

    public int hashCode(){
        return getGid();
    }

}
