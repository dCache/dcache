package diskCacheV111.vehicles;
/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class DCapClientPortAvailableMessage extends Message
{
    long id;
    int port;
    String host;

    private static final long serialVersionUID = 111493398329946124L;


  public DCapClientPortAvailableMessage(String host, int port,long id)
  {
    super();
    this.id = id;
    this.port = port;
    this.host = host;
  }

  /** Getter for property id.
   * @return Value of property id.
   */
  @Override
  public long getId() {
      return id;
  }

  /** Getter for property port.
   * @return Value of property port.
   */
  public int getPort() {
      return port;
  }

  /** Getter for property host.
   * @return Value of property host.
   */
  public String getHost() {
      return host;
  }


}



