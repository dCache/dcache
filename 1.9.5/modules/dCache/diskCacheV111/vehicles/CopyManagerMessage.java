package diskCacheV111.vehicles;
/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class CopyManagerMessage extends Message 
{
  private int    uid = 0;
  private int    gid = 0;
  private int numberOfRetries = 3;
  private int numberOfPerformedRetries = 0;
  private String srcPnfsPath;
  private String dstPnfsPath;
  private int returnCode;
  private String description;
 
  private static final long serialVersionUID = -1490534904266183106L;  
  
  public CopyManagerMessage(int uid, int gid, String srcPnfsPath, String dstPnfsPath,  long id, int bufferSize, int tcpBufferSize)
  {
    super();
    setId(id);
    this.uid = uid;
    this.gid = gid;
    this.srcPnfsPath = srcPnfsPath;
    this.dstPnfsPath = dstPnfsPath;
  }
   
  /** Getter for property uid.
   * @return Value of property uid.
   */
  public int getUid() 
  {
      return uid;
  }
  
  /** Getter for property gid.
   * @return Value of property gid.
   */
  public int getGid() 
  {
      return gid;
  }
  
  
  /** Getter for property gsiftpUrl.
   * @return Value of property gsiftpUrl.
   */
  public java.lang.String getSrcPnfsPath() 
  {
      return srcPnfsPath;
  }
  
  /** Getter for property pnfsPath.
   * @return Value of property pnfsPath.
   */
  public java.lang.String getDstPnfsPath() 
  {
      return dstPnfsPath;
  }
  
  /** Getter for property returnCode.
   * @return Value of property returnCode.
   */
  public int getReturnCode() 
  {
      return returnCode;
  }
  
  /** Setter for property returnCode.
   * @param returnCode New value of property returnCode.
   */
  public void setReturnCode(int returnCode) 
  {
      this.returnCode = returnCode;
  }
  
  /** Getter for property description.
   * @return Value of property description.
   */
  public java.lang.String getDescription() 
  {
      return description;
  }
  
  /** Setter for property description.
   * @param description New value of property description.
   */
  public void setDescription(java.lang.String description) 
  {
      this.description = description;
  }
  
  /** Getter for property numberOfRetries.
   * @return Value of property numberOfRetries.
   */
  public int getNumberOfRetries() {
      return numberOfRetries;
  }
  
  /** Setter for property numberOfRetries.
   * @param numberOfRetries New value of property numberOfRetries.
   */
  public void setNumberOfRetries(int numberOfRetries) {
      this.numberOfRetries = numberOfRetries;
  }
  
  /** Getter for property numberOfPerformedRetries.
   * @return Value of property numberOfPerformedRetries.
   */
  public int getNumberOfPerformedRetries() {
      return numberOfPerformedRetries;
  }
  
  public void increaseNumberOfPerformedRetries() {
      this.numberOfPerformedRetries++;
  }
  
   
  
}



