// $Id$

/*
 * UnixfsUser.java
 *
 * Created on July 12, 2004, 18:06
 * @author  timur
 * modified for DiskSE: AIK
 */

package org.dcache.srm.unixfs;

import org.dcache.srm.SRMUser;


public class UnixfsUser implements SRMUser
{
  public long id ;
  public static long nextId;
  private String name;
  private String root;

  private int uid = -1;
  private int gid = -1;

    /** Creates a new instance of User. */
  public UnixfsUser(String name, String root, int uid, int gid) {
      synchronized(UnixfsUser.class) {
              id = nextId++;
      }


    if (name == null || root == null) {
        throw new IllegalArgumentException("Null reference value for the string argument 'name' or 'root'");
    }

    this.name = name;
    this.root = root;
    this.uid  = uid;
    this.gid  = gid;
  }


 @Override
 public long getId() { return id; }

 @Override
 public int getPriority() { return 0; }

  /** */
  public String getName() {  return name; }
  /** */
  public String getRoot() {  return root; }
  /** */
  public int getUid()     {  return uid;  }
  /** */
  public int getGid()     {  return gid;  }

  /** */
  public String toString() {
    return "User [name=" + name + ", uid=" + uid +
        ", gid=" + gid + ", root=" + root + "]";
  }

  /** */
  public boolean equals(Object o) {
    if( ! (o instanceof UnixfsUser)) {
        return false;
    }

    UnixfsUser u = (UnixfsUser) o;
    boolean eq = (uid == u.uid) && (gid == u.gid)
        && name.equals(u.name)
        && root.equals(u.root);
    return eq;
  }

  /** */
  public int hashCode() {
    int h = name.hashCode() ^ root.hashCode()
            ^ uid ^ gid;
    return h;
  }

    public String getVoRole() {
        return null;
    }

    public String getVoGroup() {
        return name;
    }

    public boolean isReadOnly() {
        return false;
    }
}
