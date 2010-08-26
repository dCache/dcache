package diskCacheV111.vehicles.spaceManager;
import diskCacheV111.vehicles.Message;
/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public abstract class SpaceManagerMessage extends Message 
{
   
  
  protected long spaceToken; 
  
  public SpaceManagerMessage()
  {
    super();
    
  }
   
  public SpaceManagerMessage(long spaceToken) {
    super();
    this.spaceToken = spaceToken;
      
  }
    
  /** Getter for property spaceToken.
   * @return Value of property spaceToken.
   *
   */
  public long getSpaceToken() {
      return spaceToken;
  }
  
  /** Setter for property spaceToken.
   * @param spaceToken New value of property spaceToken.
   *
   */
  public void setSpaceToken(long spaceToken) {
      this.spaceToken = spaceToken;
  }
  
    public String toString(){
        String cname = this.getClass().getName();
        if(cname.lastIndexOf('.') >0) {
            cname = cname.substring(cname.lastIndexOf('.'));
        }
        return cname+
	"["+spaceToken+
        "]";        
        
    }
  
}



