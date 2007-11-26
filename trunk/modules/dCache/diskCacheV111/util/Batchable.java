// $Id: Batchable.java,v 1.4 2004-11-08 23:01:27 timur Exp $
package diskCacheV111.util ;

public interface Batchable extends Runnable {
    //this method used to notify the Batchable that scheduler has assigned 
    // an id to it
    public void ided(int id);
    public void queued() ;
    public void unqueued() ;
//    public double getTransferRate() ;
    public String getClient(); 
    public long   getClientId() ; 

}
