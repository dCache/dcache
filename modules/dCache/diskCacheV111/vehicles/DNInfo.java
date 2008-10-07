// $Id: DNInfo.java,v 1.5 2007-03-27 19:20:29 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.4  2007/03/16 22:36:17  tdh
// Propagate requested username.
//
// Revision 1.3.2.1  2006/07/26 18:42:00  tdh
// Backport of recent changes to development branch.
//
// Revision 1.3  2006/07/25 15:35:15  tdh
// Added classes to indicate a cell message is a request for authentication.
//
// Revision 1.1  2006/07/12 19:59:06  tdh
// Holds DN and role information for cell messages.
//

package diskCacheV111.vehicles;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Collection;
import java.util.List;

public class DNInfo implements Serializable {

    static final long serialVersionUID = -5368027758647986126L;

    private String name  = "DNInfo" ;
    private int    minor = 1 ;
    private int    major = 0 ;
    private long id = 0;
    private long sourceId = 0;
    private String dn = "";
    //private String[] fqan = {""};
    private String fqan;
    private String user = null;
    private List<String> FQANs;

  public DNInfo(String dn)
    {
      this.dn = dn;
    }

    public DNInfo(String dn, String user)
    {
      this(dn);
      this.user = user;
    }

    public DNInfo(String dn, String fqan, String user)
    {
      this(dn, user);
      this.fqan = fqan;
    }

    public DNInfo(String dn, List<String> FQANs, String user)
    {
      this(dn, user);
      this.FQANs = FQANs;
    }

    public DNInfo(String dn, String fqan, long id)
    {
      this(dn);
      this.fqan = fqan;
      this.id = id;
    }

    public DNInfo(String dn, List<String> FQANs, long id)
    {
      this(dn);
      this.FQANs = FQANs;
      this.id = id;
    }

    public DNInfo(String dn, String fqan, String user, long id)
    {
      this(dn, fqan, user);
      this.id = id;
    }

    public DNInfo(String dn, List<String> FQANs, String user, long id)
    {
      this(dn, FQANs, user);
      this.id = id;
    }

    public DNInfo(String dn, String fqan, String user, long id, long sourceId)
    {
      this(dn, fqan, user, id);
      this.sourceId = sourceId;
    }

    public DNInfo(String dn, List<String> FQANs, String user, long id, long sourceId)
    {
      this(dn, FQANs, user, id);
      this.sourceId = sourceId;
    }

    public DNInfo(String dn, String fqan, String user, String name, long id, long sourceId)
    {
      this(dn, fqan, user, id, sourceId);
      this.name  = name ;
    }

    public DNInfo(String dn, List<String> FQANs, String user, String name, long id, long sourceId)
    {
      this(dn, FQANs, user, id, sourceId);
      this.name  = name ;
    }

    public DNInfo(String dn,
      String fqan,
      String user,
      String name,
      long id,
      long sourceId,
      int major,
      int minor)
    {
      this(dn, fqan, user, name, id, sourceId);
      this.minor = minor ;
      this.major = major ;
    }

    public DNInfo(String dn,
      List<String> FQANs,
      String user,
      String name,
      long id,
      long sourceId,
      int major,
      int minor)
    {
      this(dn, FQANs, user, name, id, sourceId);
      this.minor = minor ;
      this.major = major ;
    }

    /** Getter for property name.
     * @return Value of property name.
     */
    public String getName()
    {
        return name ;
    }

    /** Getter for property minor.
     * @return Value of property minor.
     */
    public int    getMinorVersion()
    {
      return minor ;
    }

    /** Getter for property major.
     * @return Value of property major.
     */
    public int    getMajorVersion()
    {
      return major ;
    }

    /** Getter for property id.
     * @return Value of property id.
     */
    public long getId() {
        return id;
    }

    /** Getter for property sourceId.
     * @return Value of property sourceId.
     */
    public long getSourceId() {
        return sourceId;
    }

    /** Getter for property dn.
     * @return Value of property dn.
     */
    public String getDN() {
        return dn;
    }

  /** Getter for property fqan.
     * @return Value of property fqan.
     */
    public String getFQAN() {
        return fqan;
    }

  /** Getter for property fqan.
     * @return Value of property fqan.
     */
    public List<String> getFQANs() {
      if (FQANs==null) {
        FQANs = new LinkedList<String> ();
        if(fqan!=null) FQANs.add(fqan);
      }

      return FQANs;
    }

   /** Getter for property user.
    * @return Value of property user.
    */
   public String getUser()
   {
      return user ;
   }
  
}
