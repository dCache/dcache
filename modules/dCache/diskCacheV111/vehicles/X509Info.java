package diskCacheV111.vehicles;

import java.io.Serializable;
import java.security.cert.X509Certificate;

public class X509Info implements Serializable {

    static final long serialVersionUID = -5368027758647986126L;

    private String user = null;
    private String name  = "X509Info" ;
    private int    minor = 1 ;
    private int    major = 0 ;
    private long id = 0;
    private long sourceId = 0;
    private X509Certificate[] chain;

    public X509Info(X509Certificate[] chain)
    {
      this.chain = chain;
    }

    public X509Info(X509Certificate[] chain, String user)
    {
      this(chain);
      this.user = user;
    }

    public X509Info(X509Certificate[] chain, String user, long id)
    {
      this(chain, user);
      this.id = id;
    }

    public X509Info(X509Certificate[] chain, String user, long id, long sourceId)
    {
      this(chain, user, id);
      this.sourceId = sourceId;
    }

    public X509Info(X509Certificate[] chain, String user, String name, long id, long sourceId)
    {
      this(chain, user, id, sourceId);
      this.name  = name ;
    }

    public X509Info(X509Certificate[] chain,
      String user,
      String name,
      int major,
      int minor,
      long id,
      long sourceId)
    {
      this(chain, user, name, id, sourceId);
      this.minor = minor ;
      this.major = major ;
    }

   /** Getter for property user.
    * @return Value of property user.
    */
   public String getUser()
   {
      return user ;
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

    /** Getter for property chain.
     * @return Value of property chain.
     */
    public X509Certificate[] getChain() {
        return chain;
    }

}
