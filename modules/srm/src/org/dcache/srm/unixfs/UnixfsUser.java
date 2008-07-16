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
  public static long nextId = 0;
  private String name;
  private String root;

  private int uid = -1;
  private int gid = -1;

  private static final long serialVersionUID = -1244019880191669699L;
  
  /** Creates a new instance of User. */
  public UnixfsUser(String name, String root, int uid, int gid) {
      synchronized(UnixfsUser.class) {
              id = nextId++;
      }


    if (name == null || root == null)
      throw new IllegalArgumentException("Null reference value for the string argument 'name' or 'root'");

    this.name = name;
    this.root = root;
    this.uid  = uid;
    this.gid  = gid;
  }
  
  
 public long getId() { return id; }
 
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
    if( ! (o instanceof UnixfsUser))
      return false;

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
}

//
// $Log: not supported by cvs2svn $
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.3  2004/11/09 08:04:48  tigran
// added SerialVersion ID
//
// Revision 1.2  2004/08/06 19:35:26  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.1  2004/07/19 20:44:09  aik
// DiskSEAuthorization.java,
// UnixfsUser.java  -- inital revision
//
// unixfs.java -- Pre-Initial release to cvs
//   so timur can work on main().
// Several methods are not implemeted, as well as some features
//  (e.g. "user" is ignored).
//
// templateStorageElement.java -- template file for SE,
//  all methods throw exception or return error 'not implemented'
//
//
// Initial Version: refer to
// = Log: DCacheUser.java,v =
// = Revision 1.1.2.4  2004/06/18 22:20:51  timur =
//
