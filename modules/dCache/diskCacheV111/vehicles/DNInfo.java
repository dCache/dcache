// $Id: DNInfo.java,v 1.3.2.1 2006-07-26 18:42:00 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.3  2006/07/25 15:35:15  tdh
// Added classes to indicate a cell message is a request for authentication.
//
// Revision 1.1  2006/07/12 19:59:06  tdh
// Holds DN and role information for cell messages.
//

package diskCacheV111.vehicles;

import java.io.Serializable;

public class DNInfo implements Serializable {

    static final long serialVersionUID = -5368027758647986126L;

    private String name  = "DNInfo" ;
    private int    minor = 1 ;
    private int    major = 0 ;
    private long id = 0;
    private long sourceId = 0;
    private String dn = "";
    private String fqan = "";

    public DNInfo(String dn)
    {
      this.dn = dn;
    }

    public DNInfo(String dn, String fqan, long id)
    {
      this.dn = dn;
      this.fqan = fqan;
      this.id = id;
    }

    public DNInfo(String dn, String fqan, long id, long sourceId)
    {
      this.dn = dn;
      this.fqan = fqan;
      this.id = id;
      this.sourceId = sourceId;
    }

    public DNInfo(String dn, String fqan, String name, long id, long sourceId)
    {
      this.dn = dn;
      this.fqan = fqan;
      this.name  = name ;
      this.id = id;
      this.sourceId = sourceId;
    }

    public DNInfo(String fqan,
      String name,
      int major,
      int minor,
      long id,
      long sourceId)
    {
      this.dn = dn;
      this.fqan = fqan;
      this.name  = name ;
      this.minor = minor ;
      this.major = major ;
      this.id = id;
      this.sourceId = sourceId;
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


}
