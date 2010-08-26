package diskCacheV111.vehicles.transferManager;
import diskCacheV111.vehicles.Message;
/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public  class CancelTransferMessage extends Message 
{
  long callerUniqueId;
  
  public CancelTransferMessage(long id,long callerUniqueId)
  {
    super();
    setId(id);
    this.callerUniqueId = callerUniqueId;
    setReplyRequired(false);
    
  }
   
  
  public long getCallerUniqueId()
  {
      return callerUniqueId;
  }
  
  
}



